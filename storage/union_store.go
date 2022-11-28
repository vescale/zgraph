// Copyright 2022 zGraph Authors. All rights reserved.
//
// Copyright 2021 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/storage/kv"
)

// UnionStore is an in-memory Store which contains a buffer for write and a
// snapshot for read.
type UnionStore struct {
	memBuffer *MemDB
	snapshot  Snapshot
}

// NewUnionStore builds a new unionStore.
func NewUnionStore(snapshot Snapshot) *UnionStore {
	return &UnionStore{
		snapshot:  snapshot,
		memBuffer: newMemDB(),
	}
}

// MemBuffer return the MemBuffer binding to this unionStore.
func (us *UnionStore) MemBuffer() *MemDB {
	return us.memBuffer
}

// Get implements the Retriever interface.
func (us *UnionStore) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	v, err := us.memBuffer.Get(ctx, k)
	if errors.Cause(err) == ErrNotExist {
		v, err = us.snapshot.Get(ctx, k)
	}
	if err != nil {
		return v, err
	}
	if len(v) == 0 {
		return nil, ErrNotExist
	}
	return v, nil
}

// Iter implements the Retriever interface.
func (us *UnionStore) Iter(lowerBound, upperBound kv.Key) (Iterator, error) {
	bufferIt, err := us.memBuffer.Iter(lowerBound, upperBound)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := us.snapshot.Iter(lowerBound, upperBound)
	if err != nil {
		return nil, err
	}
	return NewUnionIter(bufferIt, retrieverIt, false)
}

// IterReverse implements the Retriever interface.
func (us *UnionStore) IterReverse(lowerBound, upperBound kv.Key) (Iterator, error) {
	bufferIt, err := us.memBuffer.IterReverse(lowerBound, upperBound)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := us.snapshot.IterReverse(lowerBound, upperBound)
	if err != nil {
		return nil, err
	}
	return NewUnionIter(bufferIt, retrieverIt, true)
}

// HasPresumeKeyNotExists gets the key exist error info for the lazy check.
func (us *UnionStore) HasPresumeKeyNotExists(k []byte) bool {
	flags, err := us.memBuffer.GetFlags(k)
	if err != nil {
		return false
	}
	return flags.HasPresumeKeyNotExists()
}

// UnmarkPresumeKeyNotExists deletes the key exist error info for the lazy check.
func (us *UnionStore) UnmarkPresumeKeyNotExists(k []byte) {
	us.memBuffer.UpdateFlags(k, kv.DelPresumeKeyNotExists)
}

// SetEntrySizeLimit sets the size limit for each entry and total buffer.
func (us *UnionStore) SetEntrySizeLimit(entryLimit, bufferLimit uint64) {
	us.memBuffer.entrySizeLimit = entryLimit
	us.memBuffer.bufferSizeLimit = bufferLimit
}
