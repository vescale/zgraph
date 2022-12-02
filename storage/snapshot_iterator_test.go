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
	"github.com/vescale/zgraph/storage/kv"
	"github.com/vescale/zgraph/storage/mvcc"
)

func TestIterator_Basic(t *testing.T) {
	s, err := Open(t.TempDir())
	assert.Nil(t, err)
	assert.NotNil(t, s)

	db := s.(*mvccStorage).db
	writes := db.NewBatch()

	// Mock data
	data := []struct {
		key []byte
		val []byte
		ver kv.Version
	}{
		{key: []byte("test"), val: []byte("test1"), ver: 100},
		{key: []byte("test"), val: []byte("test3"), ver: 300},
		{key: []byte("test1"), val: []byte("test5"), ver: 500},
		{key: []byte("test1"), val: []byte("test7"), ver: 700},
		{key: []byte("test2"), val: []byte("test9"), ver: 900},
	}
	wo := &pebble.WriteOptions{}
	for _, d := range data {
		v := mvcc.Value{
			Type:      mvcc.ValueTypePut,
			StartVer:  d.ver,
			CommitVer: d.ver + 10,
			Value:     d.val,
		}
		val, err := v.MarshalBinary()
		assert.Nil(t, err)
		assert.NotNil(t, val)
		err = writes.Set(mvcc.Encode(d.key, d.ver+1), val, wo)
		assert.Nil(t, err)
	}

	err = db.Apply(writes, wo)
	assert.Nil(t, err)

	type pair struct {
		k string
		v string
	}
	expected := []struct {
		ver     uint64
		results []pair
	}{
		{
			ver: 200,
			results: []pair{
				{"test", "test1"},
			},
		},
		{
			ver: 320,
			results: []pair{
				{"test", "test3"},
			},
		},
		{
			ver: 1000,
			results: []pair{
				{"test", "test3"},
				{"test1", "test7"},
				{"test2", "test9"},
			},
		},
	}
	for _, e := range expected {
		snapshot, err := s.Snapshot(kv.Version(e.ver))
		assert.Nil(t, err)
		iter, err := snapshot.Iter([]byte("t"), []byte("u"))
		assert.Nil(t, err)
		var results []pair

		for iter.Valid() {
			key := iter.Key()
			val := iter.Value()
			results = append(results, pair{string(key), string(val)})
			err = iter.Next()
			assert.Nil(t, err)
		}
		assert.True(t, reflect.DeepEqual(results, e.results), "expected: %v, got: %v (ver:%d)",
			e.results, results, e.ver)

		iter.Close()
	}
}

func TestReverseIterator(t *testing.T) {
	s, err := Open(t.TempDir())
	assert.Nil(t, err)
	assert.NotNil(t, s)

	db := s.(*mvccStorage).db
	writes := db.NewBatch()

	// Mock data
	data := []struct {
		key []byte
		val []byte
		ver kv.Version
	}{
		{key: []byte("test"), val: []byte("test1"), ver: 100},
		{key: []byte("test"), val: []byte("test3"), ver: 300},
		{key: []byte("test1"), val: []byte("test5"), ver: 500},
		{key: []byte("test1"), val: []byte("test7"), ver: 700},
		{key: []byte("test2"), val: []byte("test9"), ver: 900},
	}
	wo := &pebble.WriteOptions{}
	for _, d := range data {
		v := mvcc.Value{
			Type:      mvcc.ValueTypePut,
			StartVer:  d.ver,
			CommitVer: d.ver + 10,
			Value:     d.val,
		}
		val, err := v.MarshalBinary()
		assert.Nil(t, err)
		assert.NotNil(t, val)
		err = writes.Set(mvcc.Encode(d.key, d.ver+1), val, wo)
		assert.Nil(t, err)
	}

	err = db.Apply(writes, wo)
	assert.Nil(t, err)

	type pair struct {
		k string
		v string
	}
	expected := []struct {
		ver     uint64
		results []pair
	}{
		{
			ver: 200,
			results: []pair{
				{"test", "test1"},
			},
		},
		{
			ver: 320,
			results: []pair{
				{"test", "test3"},
			},
		},
		{
			ver: 1000,
			results: []pair{
				{"test2", "test9"},
				{"test1", "test7"},
				{"test", "test3"},
			},
		},
	}
	for _, e := range expected {
		snapshot, err := s.Snapshot(kv.Version(e.ver))
		assert.Nil(t, err)
		iter, err := snapshot.IterReverse([]byte("t"), []byte("u"))
		assert.Nil(t, err)
		var results []pair

		for iter.Valid() {
			key := iter.Key()
			val := iter.Value()
			results = append(results, pair{string(key), string(val)})
			err = iter.Next()
			assert.Nil(t, err)
		}
		assert.True(t, reflect.DeepEqual(results, e.results), "expected: %v, got: %v (ver:%d)",
			e.results, results, e.ver)

		iter.Close()
	}
}
