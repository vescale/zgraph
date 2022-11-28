// Copyright 2022 zGraph Authors. All rights reserved.
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
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph/storage/mvcc"
)

func TestIterator_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	s := New()
	err := s.Open(tmpDir)
	assert.Nil(t, err)

	db := s.(*mvccStorage).db
	writes := db.NewBatch()

	// Mock data
	data := []struct {
		key []byte
		val []byte
		ts  uint64
	}{
		{key: []byte("test"), val: []byte("test1"), ts: 100},
		{key: []byte("test"), val: []byte("test3"), ts: 300},
		{key: []byte("test1"), val: []byte("test5"), ts: 500},
		{key: []byte("test1"), val: []byte("test7"), ts: 700},
		{key: []byte("test2"), val: []byte("test9"), ts: 900},
	}
	wo := &pebble.WriteOptions{}
	for _, d := range data {
		v := mvcc.Value{
			Type:     mvcc.ValueTypePut,
			StartTS:  d.ts,
			CommitTS: d.ts + 10,
			Value:    d.val,
		}
		val, err := v.MarshalBinary()
		assert.Nil(t, err)
		assert.NotNil(t, val)
		err = writes.Set(mvcc.Encode(d.key, d.ts+1), val, wo)
		assert.Nil(t, err)
	}

	err = db.Apply(writes, wo)
	assert.Nil(t, err)

	type kv struct {
		k string
		v string
	}
	expected := []struct {
		ts      uint64
		results []kv
	}{
		{
			ts: 200,
			results: []kv{
				{"test", "test1"},
			},
		},
		{
			ts: 320,
			results: []kv{
				{"test", "test3"},
			},
		},
		{
			ts: 1000,
			results: []kv{
				{"test", "test3"},
				{"test1", "test7"},
				{"test2", "test9"},
			},
		},
	}
	for _, e := range expected {
		snapshot, err := s.Snapshot(mvcc.Version{Ver: e.ts})
		assert.Nil(t, err)
		iter, err := snapshot.Iter([]byte("t"), []byte("u"))
		assert.Nil(t, err)
		var results []kv

		for iter.Valid() {
			key := iter.Key()
			val := iter.Value()
			results = append(results, kv{string(key), string(val)})
			err = iter.Next()
			assert.Nil(t, err)
		}
		assert.True(t, reflect.DeepEqual(results, e.results), "expected: %v, got: %v (ts:%d)",
			e.results, results, e.ts)

		iter.Close()
	}
}

func TestReverseIterator(t *testing.T) {
	tmpDir := t.TempDir()
	s := New()
	err := s.Open(tmpDir)
	assert.Nil(t, err)

	db := s.(*mvccStorage).db
	writes := db.NewBatch()

	// Mock data
	data := []struct {
		key []byte
		val []byte
		ts  uint64
	}{
		{key: []byte("test"), val: []byte("test1"), ts: 100},
		{key: []byte("test"), val: []byte("test3"), ts: 300},
		{key: []byte("test1"), val: []byte("test5"), ts: 500},
		{key: []byte("test1"), val: []byte("test7"), ts: 700},
		{key: []byte("test2"), val: []byte("test9"), ts: 900},
	}
	wo := &pebble.WriteOptions{}
	for _, d := range data {
		v := mvcc.Value{
			Type:     mvcc.ValueTypePut,
			StartTS:  d.ts,
			CommitTS: d.ts + 10,
			Value:    d.val,
		}
		val, err := v.MarshalBinary()
		assert.Nil(t, err)
		assert.NotNil(t, val)
		err = writes.Set(mvcc.Encode(d.key, d.ts+1), val, wo)
		assert.Nil(t, err)
	}

	err = db.Apply(writes, wo)
	assert.Nil(t, err)

	type kv struct {
		k string
		v string
	}
	expected := []struct {
		ts      uint64
		results []kv
	}{
		{
			ts: 200,
			results: []kv{
				{"test", "test1"},
			},
		},
		{
			ts: 320,
			results: []kv{
				{"test", "test3"},
			},
		},
		{
			ts: 1000,
			results: []kv{
				{"test2", "test9"},
				{"test1", "test7"},
				{"test", "test3"},
			},
		},
	}
	for _, e := range expected {
		snapshot, err := s.Snapshot(mvcc.Version{Ver: e.ts})
		assert.Nil(t, err)
		iter, err := snapshot.IterReverse([]byte("t"), []byte("u"))
		assert.Nil(t, err)
		var results []kv

		for iter.Valid() {
			key := iter.Key()
			val := iter.Value()
			results = append(results, kv{string(key), string(val)})
			err = iter.Next()
			assert.Nil(t, err)
		}
		assert.True(t, reflect.DeepEqual(results, e.results), "expected: %v, got: %v (ts:%d)",
			e.results, results, e.ts)

		iter.Close()
	}
}
