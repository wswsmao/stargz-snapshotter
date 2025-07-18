/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/*
   Copyright 2019 The Go Authors. All rights reserved.
   Use of this source code is governed by a BSD-style
   license that can be found in the NOTICE.md file.
*/

//
// Example implementation of FileSystem.
//
// This implementation uses stargz by CRFS(https://github.com/google/crfs) as
// image format, which has following feature:
// - We can use docker registry as a backend store (means w/o additional layer
//   stores).
// - The stargz-formatted image is still docker-compatible (means normal
//   runtimes can still use the formatted image).
//
// Currently, we reimplemented CRFS-like filesystem for ease of integration.
// But in the near future, we intend to integrate it with CRFS.
//

package fs

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/containerd/v2/pkg/reference"
	"github.com/containerd/log"
	"github.com/containerd/stargz-snapshotter/estargz"
	"github.com/containerd/stargz-snapshotter/fs/config"
	"github.com/containerd/stargz-snapshotter/fs/layer"
	commonmetrics "github.com/containerd/stargz-snapshotter/fs/metrics/common"
	layermetrics "github.com/containerd/stargz-snapshotter/fs/metrics/layer"
	"github.com/containerd/stargz-snapshotter/fs/remote"
	"github.com/containerd/stargz-snapshotter/fs/source"
	"github.com/containerd/stargz-snapshotter/metadata"
	memorymetadata "github.com/containerd/stargz-snapshotter/metadata/memory"
	"github.com/containerd/stargz-snapshotter/snapshot"
	"github.com/containerd/stargz-snapshotter/task"
	metrics "github.com/docker/go-metrics"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
)

const (
	defaultFuseTimeout    = time.Second
	defaultMaxConcurrency = 2
)

var fusermountBin = []string{"fusermount", "fusermount3"}
var (
	nsLock = sync.Mutex{}

	ns         *metrics.Namespace
	metricsCtr *layermetrics.Controller
)

type Option func(*options)

type options struct {
	getSources              source.GetSources
	resolveHandlers         map[string]remote.Handler
	metadataStore           metadata.Store
	metricsLogLevel         *log.Level
	overlayOpaqueType       layer.OverlayOpaqueType
	additionalDecompressors func(context.Context, source.RegistryHosts, reference.Spec, ocispec.Descriptor) []metadata.Decompressor
}

func WithGetSources(s source.GetSources) Option {
	return func(opts *options) {
		opts.getSources = s
	}
}

func WithResolveHandler(name string, handler remote.Handler) Option {
	return func(opts *options) {
		if opts.resolveHandlers == nil {
			opts.resolveHandlers = make(map[string]remote.Handler)
		}
		opts.resolveHandlers[name] = handler
	}
}

func WithMetadataStore(metadataStore metadata.Store) Option {
	return func(opts *options) {
		opts.metadataStore = metadataStore
	}
}

func WithMetricsLogLevel(logLevel log.Level) Option {
	return func(opts *options) {
		opts.metricsLogLevel = &logLevel
	}
}

func WithOverlayOpaqueType(overlayOpaqueType layer.OverlayOpaqueType) Option {
	return func(opts *options) {
		opts.overlayOpaqueType = overlayOpaqueType
	}
}

func WithAdditionalDecompressors(d func(context.Context, source.RegistryHosts, reference.Spec, ocispec.Descriptor) []metadata.Decompressor) Option {
	return func(opts *options) {
		opts.additionalDecompressors = d
	}
}

func NewFilesystem(root string, cfg config.Config, opts ...Option) (_ snapshot.FileSystem, err error) {
	var fsOpts options
	for _, o := range opts {
		o(&fsOpts)
	}
	maxConcurrency := cfg.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = defaultMaxConcurrency
	}

	attrTimeout := time.Duration(cfg.AttrTimeout) * time.Second
	if attrTimeout == 0 {
		attrTimeout = defaultFuseTimeout
	}

	entryTimeout := time.Duration(cfg.EntryTimeout) * time.Second
	if entryTimeout == 0 {
		entryTimeout = defaultFuseTimeout
	}

	metadataStore := fsOpts.metadataStore
	if metadataStore == nil {
		metadataStore = memorymetadata.NewReader
	}

	getSources := fsOpts.getSources
	if getSources == nil {
		getSources = source.FromDefaultLabels(func(refspec reference.Spec) (hosts []docker.RegistryHost, _ error) {
			return docker.ConfigureDefaultRegistries(docker.WithPlainHTTP(docker.MatchLocalhost))(refspec.Hostname())
		})
	}
	tm := task.NewBackgroundTaskManager(maxConcurrency, 5*time.Second)
	r, err := layer.NewResolver(root, tm, cfg, fsOpts.resolveHandlers, metadataStore, fsOpts.overlayOpaqueType, fsOpts.additionalDecompressors)
	if err != nil {
		return nil, fmt.Errorf("failed to setup resolver: %w", err)
	}

	nsLock.Lock()
	defer nsLock.Unlock()

	if !cfg.NoPrometheus && ns == nil {
		ns = metrics.NewNamespace("stargz", "fs", nil)
		logLevel := log.DebugLevel
		if fsOpts.metricsLogLevel != nil {
			logLevel = *fsOpts.metricsLogLevel
		}
		commonmetrics.Register(logLevel) // Register common metrics. This will happen only once.
		metrics.Register(ns)             // Register layer metrics.
	}
	if metricsCtr == nil {
		metricsCtr = layermetrics.NewLayerMetrics(ns)
	}

	return &filesystem{
		resolver:              r,
		getSources:            getSources,
		prefetchSize:          cfg.PrefetchSize,
		noprefetch:            cfg.NoPrefetch,
		noBackgroundFetch:     cfg.NoBackgroundFetch,
		debug:                 cfg.Debug,
		layer:                 make(map[string]layer.Layer),
		backgroundTaskManager: tm,
		allowNoVerification:   cfg.AllowNoVerification,
		disableVerification:   cfg.DisableVerification,
		metricsController:     metricsCtr,
		attrTimeout:           attrTimeout,
		entryTimeout:          entryTimeout,
	}, nil
}

type filesystem struct {
	resolver              *layer.Resolver
	prefetchSize          int64
	noprefetch            bool
	noBackgroundFetch     bool
	debug                 bool
	layer                 map[string]layer.Layer
	layerMu               sync.Mutex
	backgroundTaskManager *task.BackgroundTaskManager
	allowNoVerification   bool
	disableVerification   bool
	getSources            source.GetSources
	metricsController     *layermetrics.Controller
	attrTimeout           time.Duration
	entryTimeout          time.Duration
}

func (fs *filesystem) Mount(ctx context.Context, mountpoint string, labels map[string]string) (retErr error) {
	// Setting the start time to measure the Mount operation duration.
	start := time.Now()

	// This is a prioritized task and all background tasks will be stopped
	// execution so this can avoid being disturbed for NW traffic by background
	// tasks.
	fs.backgroundTaskManager.DoPrioritizedTask()
	defer fs.backgroundTaskManager.DonePrioritizedTask()
	ctx = log.WithLogger(ctx, log.G(ctx).WithField("mountpoint", mountpoint))

	// Get source information of this layer.
	src, err := fs.getSources(labels)
	if err != nil {
		return err
	} else if len(src) == 0 {
		return fmt.Errorf("source must be passed")
	}

	defaultPrefetchSize := fs.prefetchSize
	if psStr, ok := labels[config.TargetPrefetchSizeLabel]; ok {
		if ps, err := strconv.ParseInt(psStr, 10, 64); err == nil {
			defaultPrefetchSize = ps
		}
	}

	// Resolve the target layer
	var (
		resultChan = make(chan layer.Layer)
		errChan    = make(chan error)
	)
	go func() {
		rErr := fmt.Errorf("failed to resolve target")
		for _, s := range src {
			l, err := fs.resolver.Resolve(ctx, s.Hosts, s.Name, s.Target)
			if err == nil {
				resultChan <- l
				fs.prefetch(ctx, l, defaultPrefetchSize, start)
				return
			}
			rErr = fmt.Errorf("failed to resolve layer %q from %q: %v: %w", s.Target.Digest, s.Name, err, rErr)
		}
		errChan <- rErr
	}()

	// Also resolve and cache other layers in parallel
	preResolve := src[0] // TODO: should we pre-resolve blobs in other sources as well?
	for _, desc := range neighboringLayers(preResolve.Manifest, preResolve.Target) {
		desc := desc
		go func() {
			// Avoids to get canceled by client.
			ctx := log.WithLogger(context.Background(), log.G(ctx).WithField("mountpoint", mountpoint))
			l, err := fs.resolver.Resolve(ctx, preResolve.Hosts, preResolve.Name, desc)
			if err != nil {
				log.G(ctx).WithError(err).Debug("failed to pre-resolve")
				return
			}
			fs.prefetch(ctx, l, defaultPrefetchSize, start)

			// Release this layer because this isn't target and we don't use it anymore here.
			// However, this will remain on the resolver cache until eviction.
			l.Done()
		}()
	}

	// Wait for resolving completion
	var l layer.Layer
	select {
	case l = <-resultChan:
	case err := <-errChan:
		log.G(ctx).WithError(err).Debug("failed to resolve layer")
		return fmt.Errorf("failed to resolve layer: %w", err)
	case <-time.After(30 * time.Second):
		log.G(ctx).Debug("failed to resolve layer (timeout)")
		return fmt.Errorf("failed to resolve layer (timeout)")
	}
	defer func() {
		if retErr != nil {
			l.Done() // don't use this layer.
		}
	}()

	// Verify layer's content
	if fs.disableVerification {
		// Skip if verification is disabled completely
		l.SkipVerify()
		log.G(ctx).Infof("Verification forcefully skipped")
	} else if tocDigest, ok := labels[estargz.TOCJSONDigestAnnotation]; ok {
		// Verify this layer using the TOC JSON digest passed through label.
		dgst, err := digest.Parse(tocDigest)
		if err != nil {
			log.G(ctx).WithError(err).Debugf("failed to parse passed TOC digest %q", dgst)
			return fmt.Errorf("invalid TOC digest: %v: %w", tocDigest, err)
		}
		if err := l.Verify(dgst); err != nil {
			log.G(ctx).WithError(err).Debugf("invalid layer")
			return fmt.Errorf("invalid stargz layer: %w", err)
		}
		log.G(ctx).Debugf("verified")
	} else if _, ok := labels[config.TargetSkipVerifyLabel]; ok && fs.allowNoVerification {
		// If unverified layer is allowed, use it with warning.
		// This mode is for legacy stargz archives which don't contain digests
		// necessary for layer verification.
		l.SkipVerify()
		log.G(ctx).Warningf("No verification is held for layer")
	} else {
		// Verification must be done. Don't mount this layer.
		return fmt.Errorf("digest of TOC JSON must be passed")
	}
	node, err := l.RootNode(0)
	if err != nil {
		log.G(ctx).WithError(err).Warnf("Failed to get root node")
		return fmt.Errorf("failed to get root node: %w", err)
	}

	// Measuring duration of Mount operation for resolved layer.
	digest := l.Info().Digest // get layer sha
	defer commonmetrics.MeasureLatencyInMilliseconds(commonmetrics.Mount, digest, start)

	// Register the mountpoint layer
	fs.layerMu.Lock()
	fs.layer[mountpoint] = l
	fs.layerMu.Unlock()
	fs.metricsController.Add(mountpoint, l)

	// mount the node to the specified mountpoint
	// TODO: bind mount the state directory as a read-only fs on snapshotter's side
	rawFS := fusefs.NewNodeFS(node, &fusefs.Options{
		AttrTimeout:     &fs.attrTimeout,
		EntryTimeout:    &fs.entryTimeout,
		NullPermissions: true,
	})
	mountOpts := &fuse.MountOptions{
		AllowOther: true,     // allow users other than root&mounter to access fs
		FsName:     "stargz", // name this filesystem as "stargz"
		Debug:      fs.debug,
	}
	if isFusermountBinExist() {
		log.G(ctx).Infof("fusermount detected")
		mountOpts.Options = []string{"suid"} // option for fusermount; allow setuid inside container
	} else {
		log.G(ctx).WithError(err).Infof("%s not installed; trying direct mount", fusermountBin)
		mountOpts.DirectMount = true
	}
	server, err := fuse.NewServer(rawFS, mountpoint, mountOpts)
	if err != nil {
		log.G(ctx).WithError(err).Debug("failed to make filesystem server")
		return err
	}

	go server.Serve()
	return server.WaitMount()
}

func (fs *filesystem) Check(ctx context.Context, mountpoint string, labels map[string]string) error {
	// This is a prioritized task and all background tasks will be stopped
	// execution so this can avoid being disturbed for NW traffic by background
	// tasks.
	fs.backgroundTaskManager.DoPrioritizedTask()
	defer fs.backgroundTaskManager.DonePrioritizedTask()

	defer commonmetrics.MeasureLatencyInMilliseconds(commonmetrics.PrefetchesCompleted, digest.FromString(""), time.Now()) // measuring the time the container launch is blocked on prefetch to complete

	ctx = log.WithLogger(ctx, log.G(ctx).WithField("mountpoint", mountpoint))

	fs.layerMu.Lock()
	l := fs.layer[mountpoint]
	fs.layerMu.Unlock()
	if l == nil {
		log.G(ctx).Debug("layer not registered")
		return fmt.Errorf("layer not registered")
	}

	if l.Info().FetchedSize < l.Info().Size {
		// Image contents hasn't fully cached yet.
		// Check the blob connectivity and try to refresh the connection on failure
		if err := fs.check(ctx, l, labels); err != nil {
			log.G(ctx).WithError(err).Warn("check failed")
			return err
		}
	}

	// Wait for prefetch compeletion
	if !fs.noprefetch {
		if err := l.WaitForPrefetchCompletion(); err != nil {
			log.G(ctx).WithError(err).Warn("failed to sync with prefetch completion")
		}
	}

	return nil
}

func (fs *filesystem) check(ctx context.Context, l layer.Layer, labels map[string]string) error {
	err := l.Check()
	if err == nil {
		return nil
	}
	log.G(ctx).WithError(err).Warn("failed to connect to blob")

	// Check failed. Try to refresh the connection with fresh source information
	src, err := fs.getSources(labels)
	if err != nil {
		return err
	}
	var (
		retrynum = 1
		rErr     = fmt.Errorf("failed to refresh connection")
	)
	for retry := 0; retry < retrynum; retry++ {
		log.G(ctx).Warnf("refreshing(%d)...", retry)
		for _, s := range src {
			err := l.Refresh(ctx, s.Hosts, s.Name, s.Target)
			if err == nil {
				log.G(ctx).Debug("Successfully refreshed connection")
				return nil
			}
			log.G(ctx).WithError(err).Warnf("failed to refresh the layer %q from %q", s.Target.Digest, s.Name)
			rErr = fmt.Errorf("failed(layer:%q, ref:%q): %v: %w", s.Target.Digest, s.Name, err, rErr)
		}
	}

	return rErr
}

func (fs *filesystem) Unmount(ctx context.Context, mountpoint string) error {
	if mountpoint == "" {
		return fmt.Errorf("mount point must be specified")
	}
	fs.layerMu.Lock()
	l, ok := fs.layer[mountpoint]
	if !ok {
		fs.layerMu.Unlock()
		return fmt.Errorf("specified path %q isn't a mountpoint", mountpoint)
	}
	delete(fs.layer, mountpoint)      // unregisters the corresponding layer
	if err := l.Close(); err != nil { // Cleanup associated resources
		log.G(ctx).WithError(err).Warn("failed to release resources of the layer")
	}
	fs.layerMu.Unlock()
	fs.metricsController.Remove(mountpoint)

	if err := unmount(mountpoint, 0); err != nil {
		if err != unix.EBUSY {
			return err
		}
		// Try force unmount
		log.G(ctx).WithError(err).Debugf("trying force unmount %q", mountpoint)
		if err := unmount(mountpoint, unix.MNT_FORCE); err != nil {
			return err
		}
	}

	return nil
}

func unmount(target string, flags int) error {
	for {
		if err := unix.Unmount(target, flags); err != unix.EINTR {
			return err
		}
	}
}

func (fs *filesystem) prefetch(ctx context.Context, l layer.Layer, defaultPrefetchSize int64, start time.Time) {
	// Prefetch a layer. The first Check() for this layer waits for the prefetch completion.
	if !fs.noprefetch {
		go l.Prefetch(defaultPrefetchSize)
	}

	// Fetch whole layer aggressively in background.
	if !fs.noBackgroundFetch {
		go func() {
			if err := l.BackgroundFetch(); err == nil {
				// write log record for the latency between mount start and last on demand fetch
				commonmetrics.LogLatencyForLastOnDemandFetch(ctx, l.Info().Digest, start, l.Info().ReadTime)
			}
		}()
	}
}

// neighboringLayers returns layer descriptors except the `target` layer in the specified manifest.
func neighboringLayers(manifest ocispec.Manifest, target ocispec.Descriptor) (descs []ocispec.Descriptor) {
	for _, desc := range manifest.Layers {
		if desc.Digest.String() != target.Digest.String() {
			descs = append(descs, desc)
		}
	}
	return
}

func isFusermountBinExist() bool {
	for _, b := range fusermountBin {
		if _, err := exec.LookPath(b); err == nil {
			return true
		}
	}
	return false
}
