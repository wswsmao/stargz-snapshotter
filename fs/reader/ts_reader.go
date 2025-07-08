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

package reader

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type FileReadaheader interface {
	ReadaheadFile(workerCount int) error
}

func (sf *file) ReadaheadFile(workerCount int) error {
	if sf.gr.isClosed() {
		return fmt.Errorf("reader is already closed")
	}

	if workerCount <= 0 {
		workerCount = 1
	}

	var offset int64
	var chunks []chunkData

	for {
		chunkOffset, chunkSize, digestStr, ok := sf.fr.ChunkEntryForOffset(offset)
		if !ok {
			break
		}
		chunks = append(chunks, chunkData{
			offset:    chunkOffset,
			size:      chunkSize,
			digestStr: digestStr,
		})
		offset = chunkOffset + chunkSize
	}

	// Unlike passthrough, this can be skipped here,
	// and a pull can be attempted again during subsequent reads.
	if len(chunks) == 0 {
		return nil
	}

	eg, ctx := errgroup.WithContext(context.Background())
	sem := semaphore.NewWeighted(int64(workerCount))

	for i := range chunks {
		chunk := chunks[i]
		if err := sem.Acquire(ctx, 1); err != nil {
			return err
		}

		eg.Go(func() error {
			defer sem.Release(1)
			return sf.readaheadChunk(chunk)
		})
	}

	return eg.Wait()
}

func (sf *file) readaheadChunk(chunk chunkData) error {
	id := genID(sf.id, chunk.offset, chunk.size)

	if r, err := sf.gr.cache.Get(id); err == nil {
		r.Close()
		return nil
	}

	b := sf.gr.bufPool.Get().(*bytes.Buffer)
	defer sf.gr.putBuffer(b)

	b.Reset()
	b.Grow(int(chunk.size))
	ip := b.Bytes()[:chunk.size]

	if _, err := sf.fr.ReadAt(ip, chunk.offset); err != nil && err != io.EOF {
		return fmt.Errorf("failed to read chunk at offset %d: %w", chunk.offset, err)
	}

	if err := sf.gr.verifyAndCache(sf.id, ip, chunk.digestStr, id); err != nil {
		return fmt.Errorf("failed to verify and cache chunk at offset %d: %w", chunk.offset, err)
	}

	return nil
}
