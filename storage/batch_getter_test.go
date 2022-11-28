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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph/storage/kv"
)

func TestBufferBatchGetter(t *testing.T) {
	snap := newMockStore()
	ka := []byte("a")
	kb := []byte("b")
	kc := []byte("c")
	kd := []byte("d")
	snap.Set(ka, ka)
	snap.Set(kb, kb)
	snap.Set(kc, kc)
	snap.Set(kd, kd)

	buffer := newMockStore()
	buffer.Set(ka, []byte("a1"))
	buffer.Delete(kb)

	batchGetter := NewBufferBatchGetter(buffer, snap)
	result, err := batchGetter.BatchGet(context.Background(), []kv.Key{ka, kb, kc, kd})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result))
	assert.Equal(t, "a1", string(result[string(ka)]))
	assert.Equal(t, "c", string(result[string(kc)]))
	assert.Equal(t, "d", string(result[string(kd)]))
}

type mockBatchGetterStore struct {
	index [][]byte
	value [][]byte
}

func newMockStore() *mockBatchGetterStore {
	return &mockBatchGetterStore{
		index: make([][]byte, 0),
		value: make([][]byte, 0),
	}
}

func (s *mockBatchGetterStore) Len() int {
	return len(s.index)
}

func (s *mockBatchGetterStore) Get(_ context.Context, k kv.Key) ([]byte, error) {
	for i, key := range s.index {
		if kv.CmpKey(key, k) == 0 {
			return s.value[i], nil
		}
	}
	return nil, ErrNotExist
}

func (s *mockBatchGetterStore) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	m := make(map[string][]byte)
	for _, k := range keys {
		v, err := s.Get(ctx, k)
		if err == nil {
			m[string(k)] = v
			continue
		}
		if IsErrNotFound(err) {
			continue
		}
		return m, err
	}
	return m, nil
}

func (s *mockBatchGetterStore) Set(k []byte, v []byte) error {
	for i, key := range s.index {
		if kv.CmpKey(key, k) == 0 {
			s.value[i] = v
			return nil
		}
	}
	s.index = append(s.index, k)
	s.value = append(s.value, v)
	return nil
}

func (s *mockBatchGetterStore) Delete(k []byte) error {
	s.Set(k, []byte{})
	return nil
}
