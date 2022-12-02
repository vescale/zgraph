// Copyright 2022 zGraph Authors. All rights reserved.
//
// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"

	"github.com/vescale/zgraph/storage/kv"
)

// BatchBufferGetter is the interface for BatchGet.
type BatchBufferGetter interface {
	kv.Getter
	Len() int
}

// BufferBatchGetter is the type for BatchGet with MemBuffer.
type BufferBatchGetter struct {
	buffer   BatchBufferGetter
	snapshot kv.BatchGetter
}

// NewBufferBatchGetter creates a new BufferBatchGetter.
func NewBufferBatchGetter(buffer BatchBufferGetter, snapshot kv.BatchGetter) *BufferBatchGetter {
	return &BufferBatchGetter{buffer: buffer, snapshot: snapshot}
}

// BatchGet gets a batch of values.
func (b *BufferBatchGetter) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	if b.buffer.Len() == 0 {
		return b.snapshot.BatchGet(ctx, keys)
	}
	bufferValues := make([][]byte, len(keys))
	shrinkKeys := make([]kv.Key, 0, len(keys))
	for i, key := range keys {
		val, err := b.buffer.Get(ctx, key)
		if err == nil {
			bufferValues[i] = val
			continue
		}
		if !IsErrNotFound(err) {
			return nil, err
		}
		shrinkKeys = append(shrinkKeys, key)
	}
	storageValues, err := b.snapshot.BatchGet(ctx, shrinkKeys)
	if err != nil {
		return nil, err
	}
	for i, key := range keys {
		if len(bufferValues[i]) == 0 {
			continue
		}
		storageValues[string(key)] = bufferValues[i]
	}
	return storageValues, nil
}
