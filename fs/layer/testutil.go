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

package layer

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/pkg/reference"
	"github.com/containerd/stargz-snapshotter/cache"
	"github.com/containerd/stargz-snapshotter/estargz"
	"github.com/containerd/stargz-snapshotter/fs/reader"
	"github.com/containerd/stargz-snapshotter/fs/remote"
	"github.com/containerd/stargz-snapshotter/fs/source"
	"github.com/containerd/stargz-snapshotter/metadata"
	"github.com/containerd/stargz-snapshotter/task"
	tutil "github.com/containerd/stargz-snapshotter/util/testutil"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/klauspost/compress/zstd"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
)

const (
	sampleChunkSize = 3
	sampleData1     = "0123456789"
	sampleData2     = "abcdefghij"
)

var srcCompressions = map[string]tutil.CompressionFactory{
	"zstd-fastest":               tutil.ZstdCompressionWithLevel(zstd.SpeedFastest),
	"gzip-bestspeed":             tutil.GzipCompressionWithLevel(gzip.BestSpeed),
	"externaltoc-gzip-bestspeed": tutil.ExternalTOCGzipCompressionWithLevel(gzip.BestSpeed),
}

type layerConfig struct {
	name              string
	passThroughConfig passThroughConfig
}

func TestSuiteLayer(t *testing.T, store metadata.Store) {
	for _, lc := range []layerConfig{
		{
			name: "default",
			passThroughConfig: passThroughConfig{
				enable: false,
			},
		},
		{
			name: "passthrough",
			passThroughConfig: passThroughConfig{
				enable:           true,
				mergeBufferSize:  419430400,
				mergeWorkerCount: 10,
			},
		},
		{
			name: "passthorough without concorrency",
			passThroughConfig: passThroughConfig{
				enable:           true,
				mergeBufferSize:  419430400,
				mergeWorkerCount: 1,
			},
		},
		{
			name: "passthorough with small buffer",
			passThroughConfig: passThroughConfig{
				enable:           true,
				mergeBufferSize:  1,
				mergeWorkerCount: 10,
			},
		},
	} {
		testPrefetch(t, store, lc)
		testNodeRead(t, store, lc)
		testNodes(t, store, lc)
	}
}

var testStateLayerDigest = digest.FromString("dummy")

func testPrefetch(t *testing.T, factory metadata.Store, lc layerConfig) {
	data64KB := string(tutil.RandomBytes(t, 64000))
	defaultPrefetchSize := int64(10000)
	landmarkPosition := func(t *testing.T, l *layer) int64 {
		if l.r == nil {
			t.Fatalf("layer hasn't been verified yet")
		}
		if id, _, err := l.r.Metadata().GetChild(l.r.Metadata().RootID(), estargz.PrefetchLandmark); err == nil {
			offset, err := l.r.Metadata().GetOffset(id)
			if err != nil {
				t.Fatalf("failed to get offset of prefetch landmark")
			}
			return offset
		}
		return defaultPrefetchSize
	}
	tests := []struct {
		name             string
		chunkSize        int // default is "sampleChunkSize"
		minChunkSize     int
		in               []tutil.TarEntry
		wantNum          int      // number of chunks wanted in the cache
		wants            []string // filenames to compare
		prefetchSize     func(*testing.T, *layer) int64
		prioritizedFiles []string
	}{
		{
			name: "no_prefetch",
			in: []tutil.TarEntry{
				tutil.File("foo.txt", sampleData1),
			},
			wantNum:          0,
			prioritizedFiles: nil,
		},
		{
			name: "prefetch",
			in: []tutil.TarEntry{
				tutil.File("foo.txt", sampleData1),
				tutil.File("bar.txt", sampleData2),
			},
			wantNum:          chunkNum(sampleData1),
			wants:            []string{"foo.txt"},
			prefetchSize:     landmarkPosition,
			prioritizedFiles: []string{"foo.txt"},
		},
		{
			name: "with_dir",
			in: []tutil.TarEntry{
				tutil.Dir("foo/"),
				tutil.File("foo/bar.txt", sampleData1),
				tutil.Dir("buz/"),
				tutil.File("buz/buzbuz.txt", sampleData2),
			},
			wantNum:          chunkNum(sampleData1),
			wants:            []string{"foo/bar.txt"},
			prefetchSize:     landmarkPosition,
			prioritizedFiles: []string{"foo/", "foo/bar.txt"},
		},
		{
			name:         "several_files_in_chunk",
			minChunkSize: 8000,
			chunkSize:    1000000000, // do not chunk
			in: []tutil.TarEntry{
				tutil.Dir("foo/"),
				tutil.File("foo/foo1", data64KB),
				tutil.File("foo2", "bb"),
				tutil.File("foo22", "ccc"),
				tutil.Dir("bar/"),
				tutil.File("bar/bar.txt", "aaa"),
				tutil.File("foo3", data64KB),
			},
			// NOTE: we assume that the compressed "data64KB" is still larger than 8KB
			// landmark+dir+foo1, foo2+foo22+dir+bar.txt+foo3, TOC, footer
			wantNum:          5, // foo1 + foo2 + foo22 + bar.txt + foo3
			wants:            []string{"foo/foo1", "foo2", "foo22", "bar/bar.txt", "foo3"},
			prefetchSize:     landmarkPosition,
			prioritizedFiles: []string{"foo/", "foo/foo1", "foo2", "foo22", "bar/", "bar/bar.txt", "foo3"},
		},
		{
			name:         "several_files_in_chunk_chunked",
			minChunkSize: 8000,
			chunkSize:    32000,
			in: []tutil.TarEntry{
				tutil.Dir("foo/"),
				tutil.File("foo/foo1", data64KB),
				tutil.File("foo2", "bb"),
				tutil.Dir("bar/"),
				tutil.File("foo3", data64KB),
			},
			// NOTE: we assume that the compressed chunk of "data64KB" is still larger than 8KB
			// landmark+dir+foo1(1), foo1(2), foo2, dir+foo3(1), foo3(2), TOC, footer
			wantNum:          3, // foo1(2) + foo2 (foo3(1) shouldn't be in a separated stream)
			wants:            []string{"foo/foo1", "foo2"},
			prefetchSize:     landmarkPosition,
			prioritizedFiles: []string{"foo/", "foo/foo1", "foo2"},
		},
	}

	for _, tt := range tests {
		for srcCompressionName, srcCompression := range srcCompressions {
			cl := srcCompression()
			t.Run("testPrefetch-"+tt.name+"-"+srcCompressionName+"-"+lc.name, func(t *testing.T) {
				chunkSize := sampleChunkSize
				if tt.chunkSize > 0 {
					chunkSize = tt.chunkSize
				}
				minChunkSize := 0
				if tt.minChunkSize > 0 {
					minChunkSize = tt.minChunkSize
				}
				sr, dgst, err := tutil.BuildEStargz(tt.in,
					tutil.WithEStargzOptions(
						estargz.WithChunkSize(chunkSize),
						estargz.WithMinChunkSize(minChunkSize),
						estargz.WithPrioritizedFiles(tt.prioritizedFiles),
						estargz.WithCompression(cl),
					))
				if err != nil {
					t.Fatalf("failed to build eStargz: %v", err)
				}
				blob := newBlob(t, sr)
				mcache := cache.NewMemoryCache()
				mr, err := factory(sr, metadata.WithDecompressors(cl))
				if err != nil {
					t.Fatalf("failed to create metadata reader: %v", err)
				}
				defer mr.Close()
				vr, err := reader.NewReader(mr, mcache, digest.FromString(""))
				if err != nil {
					t.Fatalf("failed to create reader: %v", err)
				}
				l := newLayer(
					&Resolver{
						prefetchTimeout:       time.Second,
						backgroundTaskManager: task.NewBackgroundTaskManager(10, 5*time.Second),
					},
					ocispec.Descriptor{Digest: testStateLayerDigest},
					&blobRef{blob, func(bool) {}},
					vr,
					lc.passThroughConfig,
				)
				if err := l.Verify(dgst); err != nil {
					t.Errorf("failed to verify reader: %v", err)
					return
				}
				prefetchSize := int64(0)
				if tt.prefetchSize != nil {
					prefetchSize = tt.prefetchSize(t, l)
				}
				if err := l.Prefetch(defaultPrefetchSize); err != nil {
					t.Errorf("failed to prefetch: %v", err)
					return
				}
				if blob.calledPrefetchOffset != 0 {
					t.Errorf("invalid prefetch offset %d; want %d",
						blob.calledPrefetchOffset, 0)
				}
				if blob.calledPrefetchSize != prefetchSize {
					t.Errorf("invalid prefetch size %d; want %d",
						blob.calledPrefetchSize, prefetchSize)
				}
				if cLen := len(mcache.(*cache.MemoryCache).Membuf); tt.wantNum != cLen {
					t.Errorf("number of chunks in the cache %d; want %d: %v", cLen, tt.wantNum, err)
					return
				}

				lr := l.r
				if lr == nil {
					t.Fatalf("failed to get reader from layer: %v", err)
				}
				for _, file := range tt.wants {
					id, err := lookup(lr.Metadata(), file)
					if err != nil {
						t.Fatalf("failed to lookup %q: %v", file, err)
					}
					e, err := lr.Metadata().GetAttr(id)
					if err != nil {
						t.Fatalf("failed to get attr of %q: %v", file, err)
					}
					wantFile, err := lr.OpenFile(id)
					if err != nil {
						t.Fatalf("failed to open file %q", file)
					}
					blob.readCalled = false
					if _, err := io.Copy(io.Discard, io.NewSectionReader(wantFile, 0, e.Size)); err != nil {
						t.Fatalf("failed to read file %q", file)
					}
					if blob.readCalled {
						t.Errorf("chunks of file %q aren't cached", file)
						return
					}
				}
			})
		}
	}
}

func lookup(r metadata.Reader, name string) (uint32, error) {
	name = strings.TrimPrefix(path.Clean("/"+name), "/")
	if name == "" {
		return r.RootID(), nil
	}
	dir, base := filepath.Split(name)
	pid, err := lookup(r, dir)
	if err != nil {
		return 0, err
	}
	id, _, err := r.GetChild(pid, base)
	return id, err
}

func chunkNum(data string) int {
	return (len(data)-1)/sampleChunkSize + 1
}

type region struct {
	begin int64
	end   int64 // inclusive
}

func isDup(a, b region) bool {
	if a.begin < b.begin {
		return a.end >= b.begin
	}
	// b.begin <= a.begin
	return b.end >= a.begin
}

func newBlob(t *testing.T, sr *io.SectionReader) *sampleBlob {
	return &sampleBlob{
		t: t,
		r: sr,
	}
}

type sampleBlob struct {
	t *testing.T

	r                    *io.SectionReader
	readCalled           bool
	calledPrefetchOffset int64
	calledPrefetchSize   int64
	calledRegions        []region // sorted
}

func (sb *sampleBlob) Authn(tr http.RoundTripper) (http.RoundTripper, error) { return nil, nil }
func (sb *sampleBlob) Check() error                                          { return nil }
func (sb *sampleBlob) Size() int64                                           { return sb.r.Size() }
func (sb *sampleBlob) FetchedSize() int64                                    { return 0 }
func (sb *sampleBlob) ReadAt(p []byte, offset int64, opts ...remote.Option) (int, error) {
	if len(p) > 0 {
		target := region{offset, offset + int64(len(p)) - 1}
		if len(sb.calledRegions) == 0 {
			sb.calledRegions = []region{target}
		} else {
			pos := 0
			found := false
			for i, r := range sb.calledRegions {
				if target.begin < r.begin {
					pos = i
					found = true
					break
				}
			}
			if !found {
				pos = len(sb.calledRegions)
			}
			if pos > 0 {
				b := sb.calledRegions[pos-1]
				if isDup(b, target) {
					sb.t.Fatalf("reading on the previous region is duplicated: %+v and %+v", b, target)
				}
			}
			if pos+1 < len(sb.calledRegions) {
				a := sb.calledRegions[pos+1]
				if isDup(a, target) {
					sb.t.Fatalf("reading on the next region is duplicated: %+v and %+v", a, target)
				}
			}
			sb.calledRegions = append(sb.calledRegions[:pos], append([]region{target}, sb.calledRegions[pos:]...)...)
		}
	}

	sb.readCalled = true
	return sb.r.ReadAt(p, offset)
}
func (sb *sampleBlob) Cache(offset int64, size int64, option ...remote.Option) error {
	sb.calledPrefetchOffset = offset
	sb.calledPrefetchSize = size
	return nil
}
func (sb *sampleBlob) Refresh(ctx context.Context, hosts source.RegistryHosts, refspec reference.Spec, desc ocispec.Descriptor) error {
	return nil
}
func (sb *sampleBlob) Close() error { return nil }

const (
	sampleMiddleOffset = sampleChunkSize / 2
	lastChunkOffset1   = sampleChunkSize * (int64(len(sampleData1)) / sampleChunkSize)
)

func testNodeRead(t *testing.T, factory metadata.Store, lc layerConfig) {
	sizeCond := map[string]int64{
		"single_chunk": sampleChunkSize - sampleMiddleOffset,
		"multi_chunks": sampleChunkSize + sampleMiddleOffset,
	}
	innerOffsetCond := map[string]int64{
		"at_top":    0,
		"at_middle": sampleMiddleOffset,
	}
	baseOffsetCond := map[string]int64{
		"of_1st_chunk":  sampleChunkSize * 0,
		"of_2nd_chunk":  sampleChunkSize * 1,
		"of_last_chunk": lastChunkOffset1,
	}
	fileSizeCond := map[string]int64{
		"in_1_chunk_file":  sampleChunkSize * 1,
		"in_2_chunks_file": sampleChunkSize * 2,
		"in_max_size_file": int64(len(sampleData1)),
	}
	for sn, size := range sizeCond {
		for in, innero := range innerOffsetCond {
			for bo, baseo := range baseOffsetCond {
				for fn, filesize := range fileSizeCond {
					for _, srcCompression := range srcCompressions {
						cl := srcCompression()
						t.Run(fmt.Sprintf("reading_%s_%s_%s_%s_%s", sn, in, bo, fn, lc.name), func(t *testing.T) {
							if filesize > int64(len(sampleData1)) {
								t.Fatal("sample file size is larger than sample data")
							}

							wantN := size
							offset := baseo + innero
							if remain := filesize - offset; remain < wantN {
								if wantN = remain; wantN < 0 {
									wantN = 0
								}
							}

							// use constant string value as a data source.
							want := strings.NewReader(sampleData1)

							// data we want to get.
							wantData := make([]byte, wantN)
							_, err := want.ReadAt(wantData, offset)
							if err != nil && err != io.EOF {
								t.Fatalf("want.ReadAt (offset=%d,size=%d): %v", offset, wantN, err)
							}

							// data we get from the file node.
							f, closeFn := makeNodeReader(t, []byte(sampleData1)[:filesize], sampleChunkSize, factory, cl, lc)
							defer closeFn()
							tmpbuf := make([]byte, size) // fuse library can request bigger than remain
							rr, errno := f.Read(context.Background(), tmpbuf, offset)
							if errno != 0 {
								t.Errorf("failed to read off=%d, size=%d, filesize=%d: %v", offset, size, filesize, err)
								return
							}
							if rsize := rr.Size(); int64(rsize) != wantN {
								t.Errorf("read size: %d; want: %d; passed %d", rsize, wantN, size)
								return
							}
							tmpbuf = make([]byte, len(tmpbuf))
							respData, fs := rr.Bytes(tmpbuf)
							if fs != fuse.OK {
								t.Errorf("failed to read result data for off=%d, size=%d, filesize=%d: %v", offset, size, filesize, err)
							}

							if !bytes.Equal(wantData, respData) {
								t.Errorf("off=%d, filesize=%d; read data{size=%d,data=%q}; want (size=%d,data=%q)",
									offset, filesize, len(respData), string(respData), wantN, string(wantData))
								return
							}
						})
					}
				}
			}
		}
	}
}

func makeNodeReader(t *testing.T, contents []byte, chunkSize int, factory metadata.Store, cl tutil.Compression, lc layerConfig) (_ *file, closeFn func() error) {
	testName := "test"
	sr, tocDgst, err := tutil.BuildEStargz(
		[]tutil.TarEntry{tutil.File(testName, string(contents))},
		tutil.WithEStargzOptions(estargz.WithChunkSize(chunkSize), estargz.WithCompression(cl)),
	)
	if err != nil {
		t.Fatalf("failed to build sample eStargz: %v", err)
	}
	r, err := factory(sr, metadata.WithDecompressors(cl))
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	rootNode := getRootNode(t, r, OverlayOpaqueAll, tocDgst, cache.NewMemoryCache(), lc)
	var eo fuse.EntryOut
	inode, errno := rootNode.Lookup(context.Background(), testName, &eo)
	if errno != 0 {
		r.Close()
		t.Fatalf("failed to lookup test node; errno: %v", errno)
	}
	f, _, errno := inode.Operations().(fusefs.NodeOpener).Open(context.Background(), 0)
	if errno != 0 {
		r.Close()
		t.Fatalf("failed to open test file; errno: %v", errno)
	}
	return f.(*file), r.Close
}

func testNodes(t *testing.T, factory metadata.Store, lc layerConfig) {
	for _, o := range []OverlayOpaqueType{OverlayOpaqueAll, OverlayOpaqueTrusted, OverlayOpaqueUser} {
		testNodesWithOpaque(t, factory, o, lc)
	}
}

func testNodesWithOpaque(t *testing.T, factory metadata.Store, opaque OverlayOpaqueType, lc layerConfig) {
	data64KB := string(tutil.RandomBytes(t, 64000))
	hasOpaque := func(entry string) check {
		return func(t *testing.T, root *node, cc cache.BlobCache, cr *calledReaderAt) {
			for _, k := range opaqueXattrs[opaque] {
				hasNodeXattrs(entry, k, opaqueXattrValue)(t, root, cc, cr)
			}
		}
	}
	tests := []struct {
		name         string
		chunkSize    int
		minChunkSize int
		in           []tutil.TarEntry
		want         []check
	}{
		{
			name: "1_whiteout_with_sibling",
			in: []tutil.TarEntry{
				tutil.Dir("foo/"),
				tutil.File("foo/bar.txt", ""),
				tutil.File("foo/.wh.foo.txt", ""),
			},
			want: []check{
				hasValidWhiteout("foo/foo.txt"),
				fileNotExist("foo/.wh.foo.txt"),
			},
		},
		{
			name: "1_whiteout_with_duplicated_name",
			in: []tutil.TarEntry{
				tutil.Dir("foo/"),
				tutil.File("foo/bar.txt", "test"),
				tutil.File("foo/.wh.bar.txt", ""),
			},
			want: []check{
				hasFileDigest("foo/bar.txt", digestFor("test")),
				fileNotExist("foo/.wh.bar.txt"),
			},
		},
		{
			name: "1_opaque",
			in: []tutil.TarEntry{
				tutil.Dir("foo/"),
				tutil.File("foo/.wh..wh..opq", ""),
			},
			want: []check{
				hasOpaque("foo/"),
				fileNotExist("foo/.wh..wh..opq"),
			},
		},
		{
			name: "1_opaque_with_sibling",
			in: []tutil.TarEntry{
				tutil.Dir("foo/"),
				tutil.File("foo/.wh..wh..opq", ""),
				tutil.File("foo/bar.txt", "test"),
			},
			want: []check{
				hasOpaque("foo/"),
				hasFileDigest("foo/bar.txt", digestFor("test")),
				fileNotExist("foo/.wh..wh..opq"),
			},
		},
		{
			name: "1_opaque_with_xattr",
			in: []tutil.TarEntry{
				tutil.Dir("foo/", tutil.WithDirXattrs(map[string]string{"foo": "bar"})),
				tutil.File("foo/.wh..wh..opq", ""),
			},
			want: []check{
				hasOpaque("foo/"),
				hasNodeXattrs("foo/", "foo", "bar"),
				fileNotExist("foo/.wh..wh..opq"),
			},
		},
		{
			name: "prefetch_landmark",
			in: []tutil.TarEntry{
				tutil.File(estargz.PrefetchLandmark, "test"),
				tutil.Dir("foo/"),
				tutil.File(fmt.Sprintf("foo/%s", estargz.PrefetchLandmark), "test"),
			},
			want: []check{
				fileNotExist(estargz.PrefetchLandmark),
				hasFileDigest(fmt.Sprintf("foo/%s", estargz.PrefetchLandmark), digestFor("test")),
			},
		},
		{
			name: "no_prefetch_landmark",
			in: []tutil.TarEntry{
				tutil.File(estargz.NoPrefetchLandmark, "test"),
				tutil.Dir("foo/"),
				tutil.File(fmt.Sprintf("foo/%s", estargz.NoPrefetchLandmark), "test"),
			},
			want: []check{
				fileNotExist(estargz.NoPrefetchLandmark),
				hasFileDigest(fmt.Sprintf("foo/%s", estargz.NoPrefetchLandmark), digestFor("test")),
			},
		},
		{
			name: "state_file",
			in: []tutil.TarEntry{
				tutil.File("test", "test"),
			},
			want: []check{
				hasFileDigest("test", digestFor("test")),
				hasStateFile(t, testStateLayerDigest.String()+".json"),
			},
		},
		{
			name: "file_suid",
			in: []tutil.TarEntry{
				tutil.File("test", "test", tutil.WithFileMode(0644|os.ModeSetuid)),
			},
			want: []check{
				hasExtraMode("test", os.ModeSetuid),
			},
		},
		{
			name: "dir_sgid",
			in: []tutil.TarEntry{
				tutil.Dir("test/", tutil.WithDirMode(0755|os.ModeSetgid)),
			},
			want: []check{
				hasExtraMode("test/", os.ModeSetgid),
			},
		},
		{
			name: "file_sticky",
			in: []tutil.TarEntry{
				tutil.File("test", "test", tutil.WithFileMode(0644|os.ModeSticky)),
			},
			want: []check{
				hasExtraMode("test", os.ModeSticky),
			},
		},
		{
			name: "symlink_size",
			in: []tutil.TarEntry{
				tutil.Symlink("test", "target"),
			},
			want: []check{
				hasSize("test", len("target")),
			},
		},
		{
			name:         "several_files_in_chunk",
			minChunkSize: 8000,
			in: []tutil.TarEntry{
				tutil.Dir("foo/"),
				tutil.File("foo/foo1", data64KB),
				tutil.File("foo2", "bb"),
				tutil.File("foo22", "ccc"),
				tutil.Dir("bar/"),
				tutil.File("bar/bar.txt", "aaa"),
				tutil.File("foo3", data64KB),
			},
			// NOTE: we assume that the compressed "data64KB" is still larger than 8KB
			// landmark+dir+foo1, foo2+foo22+dir+bar.txt+foo3, TOC, footer
			want: []check{
				hasFileContentsWithPreCached("foo22", 0, "ccc", chunkInfo{"foo2", "bb", 0, 2}, chunkInfo{"bar/bar.txt", "aaa", 0, 3}, chunkInfo{"foo3", data64KB, 0, 64000}),
				hasFileContentsOffset("foo2", 0, "bb", true),
				hasFileContentsOffset("bar/bar.txt", 0, "aaa", true),
				hasFileContentsOffset("bar/bar.txt", 1, "aa", true),
				hasFileContentsOffset("bar/bar.txt", 2, "a", true),
				hasFileContentsOffset("foo3", 0, data64KB, true),
				hasFileContentsOffset("foo22", 0, "ccc", true),
				hasFileContentsOffset("foo/foo1", 0, data64KB, false),
				hasFileContentsOffset("foo/foo1", 0, data64KB, true),
				hasFileContentsOffset("foo/foo1", 1, data64KB[1:], true),
				hasFileContentsOffset("foo/foo1", 2, data64KB[2:], true),
				hasFileContentsOffset("foo/foo1", 3, data64KB[3:], true),
			},
		},
		{
			name:         "several_files_in_chunk_chunked",
			minChunkSize: 8000,
			chunkSize:    32000,
			in: []tutil.TarEntry{
				tutil.Dir("foo/"),
				tutil.File("foo/foo1", data64KB),
				tutil.File("foo2", "bb"),
				tutil.Dir("bar/"),
				tutil.File("foo3", data64KB),
			},
			// NOTE: we assume that the compressed chunk of "data64KB" is still larger than 8KB
			// landmark+dir+foo1(1), foo1(2), foo2+dir+foo3(1), foo3(2), TOC, footer
			want: []check{
				hasFileContentsWithPreCached("foo2", 0, "bb", chunkInfo{"foo3", data64KB[:32000], 0, 32000}),
				hasFileContentsOffset("foo2", 0, "bb", true),
				hasFileContentsOffset("foo2", 1, "b", true),
				hasFileContentsOffset("foo3", 0, data64KB[:len(data64KB)/2], true),
				hasFileContentsOffset("foo3", 1, data64KB[1:len(data64KB)/2], true),
				hasFileContentsOffset("foo3", 2, data64KB[2:len(data64KB)/2], true),
				hasFileContentsOffset("foo3", int64(len(data64KB)/2), data64KB[len(data64KB)/2:], false),
				hasFileContentsOffset("foo3", int64(len(data64KB)-1), data64KB[len(data64KB)-1:], true),
				hasFileContentsOffset("foo/foo1", 0, data64KB, false),
				hasFileContentsOffset("foo/foo1", 1, data64KB[1:], true),
				hasFileContentsOffset("foo/foo1", 2, data64KB[2:], true),
				hasFileContentsOffset("foo/foo1", int64(len(data64KB)/2), data64KB[len(data64KB)/2:], true),
				hasFileContentsOffset("foo/foo1", int64(len(data64KB)-1), data64KB[len(data64KB)-1:], true),
			},
		},
	}

	for _, tt := range tests {
		for _, srcCompression := range srcCompressions {
			cl := srcCompression()
			t.Run(tt.name+"-"+lc.name, func(t *testing.T) {
				opts := []tutil.BuildEStargzOption{
					tutil.WithEStargzOptions(estargz.WithCompression(cl)),
				}
				if tt.chunkSize > 0 {
					opts = append(opts, tutil.WithEStargzOptions(estargz.WithChunkSize(tt.chunkSize)))
				}
				if tt.minChunkSize > 0 {
					opts = append(opts, tutil.WithEStargzOptions(estargz.WithMinChunkSize(tt.minChunkSize)))
				}
				sgz, tocDgst, err := tutil.BuildEStargz(tt.in, opts...)
				if err != nil {
					t.Fatalf("failed to build sample eStargz: %v", err)
				}

				testR := &calledReaderAt{sgz, nil}
				r, err := factory(io.NewSectionReader(testR, 0, sgz.Size()), metadata.WithDecompressors(cl))
				if err != nil {
					t.Fatalf("failed to create reader: %v", err)
				}
				defer r.Close()
				mcache := cache.NewMemoryCache()
				rootNode := getRootNode(t, r, opaque, tocDgst, mcache, lc)
				for _, want := range tt.want {
					want(t, rootNode, mcache, testR)
				}
			})
		}
	}
}

func getRootNode(t *testing.T, r metadata.Reader, opaque OverlayOpaqueType, tocDgst digest.Digest, cc cache.BlobCache, lc layerConfig) *node {
	vr, err := reader.NewReader(r, cc, digest.FromString(""))
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	rr, err := vr.VerifyTOC(tocDgst)
	if err != nil {
		t.Fatalf("failed to verify reader: %v", err)
	}
	rootNode, err := newNode(testStateLayerDigest, rr, &testBlobState{10, 5}, 100, opaque, lc.passThroughConfig)
	if err != nil {
		t.Fatalf("failed to get root node: %v", err)
	}
	fusefs.NewNodeFS(rootNode, &fusefs.Options{}) // initializes root node
	return rootNode.(*node)
}

type testBlobState struct {
	size        int64
	fetchedSize int64
}

func (tb *testBlobState) Check() error       { return nil }
func (tb *testBlobState) Size() int64        { return tb.size }
func (tb *testBlobState) FetchedSize() int64 { return tb.fetchedSize }
func (tb *testBlobState) ReadAt(p []byte, offset int64, opts ...remote.Option) (int, error) {
	return 0, nil
}
func (tb *testBlobState) Cache(offset int64, size int64, opts ...remote.Option) error { return nil }
func (tb *testBlobState) Refresh(ctx context.Context, host source.RegistryHosts, refspec reference.Spec, desc ocispec.Descriptor) error {
	return nil
}
func (tb *testBlobState) Close() error { return nil }

type check func(*testing.T, *node, cache.BlobCache, *calledReaderAt)

func fileNotExist(file string) check {
	return func(t *testing.T, root *node, cc cache.BlobCache, cr *calledReaderAt) {
		if _, _, err := getDirentAndNode(t, root, file); err == nil {
			t.Errorf("Node %q exists", file)
		}
	}
}

func hasFileDigest(filename string, digest string) check {
	return func(t *testing.T, root *node, cc cache.BlobCache, cr *calledReaderAt) {
		_, n, err := getDirentAndNode(t, root, filename)
		if err != nil {
			t.Fatalf("failed to get node %q: %v", filename, err)
		}
		ni := n.Operations().(*node)
		attr, err := ni.fs.r.Metadata().GetAttr(ni.id)
		if err != nil {
			t.Fatalf("failed to get attr %q(%d): %v", filename, ni.id, err)
		}
		fh, _, errno := ni.Open(context.Background(), 0)
		if errno != 0 {
			t.Fatalf("failed to open node %q: %v", filename, errno)
		}
		rr, errno := fh.(*file).Read(context.Background(), make([]byte, attr.Size), 0)
		if errno != 0 {
			t.Fatalf("failed to read node %q: %v", filename, errno)
		}
		res, status := rr.Bytes(make([]byte, attr.Size))
		if status != fuse.OK {
			t.Fatalf("failed to get read result of node %q: %v", filename, status)
		}
		if ndgst := digestFor(string(res)); ndgst != digest {
			t.Fatalf("Digest(%q) = %q, want %q", filename, ndgst, digest)
		}
	}
}

func hasSize(name string, size int) check {
	return func(t *testing.T, root *node, cc cache.BlobCache, cr *calledReaderAt) {
		_, n, err := getDirentAndNode(t, root, name)
		if err != nil {
			t.Fatalf("failed to get node %q: %v", name, err)
		}
		var ao fuse.AttrOut
		if errno := n.Operations().(fusefs.NodeGetattrer).Getattr(context.Background(), nil, &ao); errno != 0 {
			t.Fatalf("failed to get attributes of node %q: %v", name, errno)
		}
		if ao.Size != uint64(size) {
			t.Fatalf("got size = %d, want %d", ao.Size, size)
		}
	}
}

func hasExtraMode(name string, mode os.FileMode) check {
	return func(t *testing.T, root *node, cc cache.BlobCache, cr *calledReaderAt) {
		_, n, err := getDirentAndNode(t, root, name)
		if err != nil {
			t.Fatalf("failed to get node %q: %v", name, err)
		}
		var ao fuse.AttrOut
		if errno := n.Operations().(fusefs.NodeGetattrer).Getattr(context.Background(), nil, &ao); errno != 0 {
			t.Fatalf("failed to get attributes of node %q: %v", name, errno)
		}
		a := ao.Attr
		gotMode := a.Mode & (syscall.S_ISUID | syscall.S_ISGID | syscall.S_ISVTX)
		wantMode := extraModeToTarMode(mode)
		if gotMode != uint32(wantMode) {
			t.Fatalf("got mode = %b, want %b", gotMode, wantMode)
		}
	}
}

func hasValidWhiteout(name string) check {
	return func(t *testing.T, root *node, cc cache.BlobCache, cr *calledReaderAt) {
		ent, n, err := getDirentAndNode(t, root, name)
		if err != nil {
			t.Fatalf("failed to get node %q: %v", name, err)
		}
		var ao fuse.AttrOut
		if errno := n.Operations().(fusefs.NodeGetattrer).Getattr(context.Background(), nil, &ao); errno != 0 {
			t.Fatalf("failed to get attributes of file %q: %v", name, errno)
		}
		a := ao.Attr
		if a.Ino != ent.Ino {
			t.Errorf("inconsistent inodes %d(Node) != %d(Dirent)", a.Ino, ent.Ino)
			return
		}

		// validate the direntry
		if ent.Mode != syscall.S_IFCHR {
			t.Errorf("whiteout entry %q isn't a char device", name)
			return
		}

		// validate the node
		if a.Mode != syscall.S_IFCHR {
			t.Errorf("whiteout %q has an invalid mode %o; want %o",
				name, a.Mode, syscall.S_IFCHR)
			return
		}
		if a.Rdev != uint32(unix.Mkdev(0, 0)) {
			t.Errorf("whiteout %q has invalid device numbers (%d, %d); want (0, 0)",
				name, unix.Major(uint64(a.Rdev)), unix.Minor(uint64(a.Rdev)))
			return
		}
	}
}

func hasNodeXattrs(entry, name, value string) check {
	return func(t *testing.T, root *node, cc cache.BlobCache, cr *calledReaderAt) {
		_, n, err := getDirentAndNode(t, root, entry)
		if err != nil {
			t.Fatalf("failed to get node %q: %v", entry, err)
		}

		// check xattr exists in the xattrs list.
		buf := make([]byte, 1000)
		nb, errno := n.Operations().(fusefs.NodeListxattrer).Listxattr(context.Background(), buf)
		if errno != 0 {
			t.Fatalf("failed to get xattrs list of node %q: %v", entry, err)
		}
		attrs := strings.Split(string(buf[:nb]), "\x00")
		var found bool
		for _, x := range attrs {
			if x == name {
				found = true
			}
		}
		if !found {
			t.Errorf("node %q doesn't have an opaque xattr %q", entry, value)
			return
		}

		// check the xattr has valid value.
		v := make([]byte, len(value))
		nv, errno := n.Operations().(fusefs.NodeGetxattrer).Getxattr(context.Background(), name, v)
		if errno != 0 {
			t.Fatalf("failed to get xattr %q of node %q: %v", name, entry, err)
		}
		if int(nv) != len(value) {
			t.Fatalf("invalid xattr size for file %q, value %q got %d; want %d",
				name, value, nv, len(value))
		}
		if string(v) != value {
			t.Errorf("node %q has an invalid xattr %q; want %q", entry, v, value)
			return
		}
	}
}

func hasEntry(t *testing.T, name string, ents fusefs.DirStream) (fuse.DirEntry, bool) {
	for ents.HasNext() {
		de, errno := ents.Next()
		if errno != 0 {
			t.Fatalf("faield to read entries for %q", name)
		}
		if de.Name == name {
			return de, true
		}
	}
	return fuse.DirEntry{}, false
}

func hasStateFile(t *testing.T, id string) check {
	return func(t *testing.T, root *node, cc cache.BlobCache, cr *calledReaderAt) {

		// Check the state dir is hidden on OpenDir for "/"
		ents, errno := root.Readdir(context.Background())
		if errno != 0 {
			t.Errorf("failed to open root directory: %v", errno)
			return
		}
		if _, ok := hasEntry(t, stateDirName, ents); ok {
			t.Errorf("state direntry %q should not be listed", stateDirName)
			return
		}

		// Check existence of state dir
		var eo fuse.EntryOut
		sti, errno := root.Lookup(context.Background(), stateDirName, &eo)
		if errno != 0 {
			t.Errorf("failed to lookup directory %q: %v", stateDirName, errno)
			return
		}
		st, ok := sti.Operations().(*state)
		if !ok {
			t.Errorf("directory %q isn't a state node", stateDirName)
			return
		}

		// Check existence of state file
		ents, errno = st.Readdir(context.Background())
		if errno != 0 {
			t.Errorf("failed to open directory %q: %v", stateDirName, errno)
			return
		}
		if _, ok := hasEntry(t, id, ents); !ok {
			t.Errorf("direntry %q not found in %q", id, stateDirName)
			return
		}
		inode, errno := st.Lookup(context.Background(), id, &eo)
		if errno != 0 {
			t.Errorf("failed to lookup node %q in %q: %v", id, stateDirName, errno)
			return
		}
		n, ok := inode.Operations().(*statFile)
		if !ok {
			t.Errorf("entry %q isn't a normal node", id)
			return
		}

		// wanted data
		b, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
		if err != nil {
			panic(err)
		}
		wantErr := fmt.Errorf("test-%d", b.Int64())

		// report the data
		root.fs.s.report(wantErr)

		// obtain file size (check later)
		var ao fuse.AttrOut
		errno = n.Operations().(fusefs.NodeGetattrer).Getattr(context.Background(), nil, &ao)
		if errno != 0 {
			t.Errorf("failed to get attr of state file: %v", errno)
			return
		}
		attr := ao.Attr

		// get data via state file
		tmp := make([]byte, 4096)
		res, errno := n.Read(context.Background(), nil, tmp, 0)
		if errno != 0 {
			t.Errorf("failed to read state file: %v", errno)
			return
		}
		gotState, status := res.Bytes(nil)
		if status != fuse.OK {
			t.Errorf("failed to get result bytes of state file: %v", errno)
			return
		}
		if attr.Size != uint64(len(string(gotState))) {
			t.Errorf("size %d; want %d", attr.Size, len(string(gotState)))
			return
		}

		var j statJSON
		if err := json.Unmarshal(gotState, &j); err != nil {
			t.Errorf("failed to unmarshal %q: %v", string(gotState), err)
			return
		}
		if wantErr.Error() != j.Error {
			t.Errorf("expected error %q, got %q", wantErr.Error(), j.Error)
			return
		}
	}
}

// getDirentAndNode gets dirent and node at the specified path at once and makes
// sure that the both of them exist.
func getDirentAndNode(t *testing.T, root *node, path string) (ent fuse.DirEntry, n *fusefs.Inode, err error) {
	dir, base := filepath.Split(filepath.Clean(path))

	// get the target's parent directory.
	var eo fuse.EntryOut
	d := root
	for _, name := range strings.Split(dir, "/") {
		if len(name) == 0 {
			continue
		}
		di, errno := d.Lookup(context.Background(), name, &eo)
		if errno != 0 {
			err = fmt.Errorf("failed to lookup directory %q: %v", name, errno)
			return
		}
		var ok bool
		if d, ok = di.Operations().(*node); !ok {
			err = fmt.Errorf("directory %q isn't a normal node", name)
			return
		}

	}

	// get the target's direntry.
	ents, errno := d.Readdir(context.Background())
	if errno != 0 {
		err = fmt.Errorf("failed to open directory %q: %v", path, errno)
	}
	ent, ok := hasEntry(t, base, ents)
	if !ok {
		err = fmt.Errorf("direntry %q not found in the parent directory of %q", base, path)
	}

	// get the target's node.
	n, errno = d.Lookup(context.Background(), base, &eo)
	if errno != 0 {
		err = fmt.Errorf("failed to lookup node %q: %v", path, errno)
	}

	return
}

func digestFor(content string) string {
	sum := sha256.Sum256([]byte(content))
	return fmt.Sprintf("sha256:%x", sum)
}

// suid, guid, sticky bits for archive/tar
// https://github.com/golang/go/blob/release-branch.go1.13/src/archive/tar/common.go#L607-L609
const (
	cISUID = 04000 // Set uid
	cISGID = 02000 // Set gid
	cISVTX = 01000 // Save text (sticky bit)
)

func extraModeToTarMode(fm os.FileMode) (tm int64) {
	if fm&os.ModeSetuid != 0 {
		tm |= cISUID
	}
	if fm&os.ModeSetgid != 0 {
		tm |= cISGID
	}
	if fm&os.ModeSticky != 0 {
		tm |= cISVTX
	}
	return
}

type chunkInfo struct {
	name        string
	data        string
	chunkOffset int64
	chunkSize   int64
}

func hasFileContentsWithPreCached(name string, off int64, contents string, extra ...chunkInfo) check {
	return func(t *testing.T, root *node, cc cache.BlobCache, cr *calledReaderAt) {
		buf := readFile(t, root, name, int64(len(contents)), off)
		if len(buf) != len(contents) {
			t.Fatalf("failed to read contents %q (off:%d, want:%q) got %q", name, off, longBytesView([]byte(contents)), longBytesView(buf))
		}
		if string(buf) != contents {
			t.Fatalf("unexpected content of %q: %q want %q", name, longBytesView(buf), longBytesView([]byte(contents)))
		}
		for _, e := range extra {
			cr.called = nil // reset test
			data := readFile(t, root, e.name, e.chunkSize, e.chunkOffset)
			if string(data) != e.data {
				t.Fatalf("unexpected contents of %q (%+v): %q; wanted %q", e.name, e, longBytesView(data), longBytesView([]byte(e.data)))
			}
			if len(cr.called) != 0 {
				t.Fatalf("unexpected read on %q: offsets: %v", e.name, cr.called)
			}
		}
	}
}

func hasFileContentsOffset(name string, off int64, contents string, fromCache bool) check {
	return func(t *testing.T, root *node, cc cache.BlobCache, cr *calledReaderAt) {
		cr.called = nil // reset test
		buf := readFile(t, root, name, int64(len(contents)), off)
		if len(buf) != len(contents) {
			t.Fatalf("failed to read contents %q (off:%d, want:%q) got %q", name, off, longBytesView([]byte(contents)), longBytesView(buf))
		}
		if string(buf) != contents {
			t.Fatalf("unexpected content of %q: %q want %q", name, longBytesView(buf), longBytesView([]byte(contents)))
		}
		t.Logf("reader calls for %q: offsets: %+v", name, cr.called)
		if fromCache {
			if len(cr.called) != 0 {
				t.Fatalf("unexpected read on %q: offsets: %v", name, cr.called)
			}
		} else {
			if len(cr.called) == 0 {
				t.Fatalf("no call happened to reader for %q", name)
			}
		}
	}
}

func readFile(t *testing.T, root *node, filename string, size, off int64) []byte {
	_, n, err := getDirentAndNode(t, root, filename)
	if err != nil {
		t.Fatalf("failed to get node %q: %v", filename, err)
	}
	ni := n.Operations().(*node)
	fh, _, errno := ni.Open(context.Background(), 0)
	if errno != 0 {
		t.Fatalf("failed to open node %q: %v", filename, errno)
	}
	rr, errno := fh.(*file).Read(context.Background(), make([]byte, size), off)
	if errno != 0 {
		t.Fatalf("failed to read node %q: %v", filename, errno)
	}
	buf, status := rr.Bytes(make([]byte, size))
	if status != fuse.OK {
		t.Fatalf("failed to get read result of node %q: %v", filename, status)
	}
	return buf
}

type calledReaderAt struct {
	io.ReaderAt
	called []int64
}

func (r *calledReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.called = append(r.called, off)
	return r.ReaderAt.ReadAt(p, off)
}

// longBytesView is an alias of []byte suitable for printing a long data as an omitted string to avoid long data being printed.
type longBytesView []byte

func (b longBytesView) String() string {
	if len(b) < 100 {
		return string(b)
	}
	return string(b[:50]) + "...(omit)..." + string(b[len(b)-50:])
}
