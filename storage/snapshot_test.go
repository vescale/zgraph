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
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph/storage/kv"
	"github.com/vescale/zgraph/storage/mvcc"
)

func TestSnapshot_Get(t *testing.T) {
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
		ver mvcc.Version
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

	expected := []struct {
		key string
		val string
		ver mvcc.Version
	}{
		{"test", "test1", 200},
		{"test", "test1", 300},
		{"test", "test3", 310},
		{"test", "test3", 400},
		{"test1", "test5", 600},
		{"test1", "test7", 750},
	}
	for _, e := range expected {
		snapshot, err := s.Snapshot(e.ver)
		assert.Nil(t, err)
		val, err := snapshot.Get(context.Background(), []byte(e.key))
		assert.Nil(t, err)
		assert.True(t, bytes.Equal(val, []byte(e.val)), "expected: %v, got: %v (key: %s, ver:%d)",
			e.val, string(val), e.key, e.ver)
	}
}

func TestSnapshot_BatchGet(t *testing.T) {
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
		ver mvcc.Version
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

	expected := []struct {
		keys   []string
		values map[string][]byte
		ver    mvcc.Version
	}{
		{
			keys: []string{"test", "test1", "test2"},
			values: map[string][]byte{
				"test":  []byte("test1"),
				"test1": nil,
				"test2": nil,
			},
			ver: 200,
		},
		{
			keys: []string{"test", "test1", "test2"},
			values: map[string][]byte{
				"test":  []byte("test3"),
				"test1": []byte("test5"),
				"test2": nil,
			},
			ver: 510,
		},
		{
			keys: []string{"test", "test1", "test2"},
			values: map[string][]byte{
				"test":  []byte("test3"),
				"test1": []byte("test7"),
				"test2": []byte("test9"),
			},
			ver: 1000,
		},
	}
	for _, e := range expected {
		snapshot, err := s.Snapshot(mvcc.Version(e.ver))
		assert.Nil(t, err)
		var keys []kv.Key
		for _, k := range e.keys {
			keys = append(keys, kv.Key(k))
		}
		values, err := snapshot.BatchGet(context.Background(), keys)
		assert.Nil(t, err)
		assert.True(t, reflect.DeepEqual(values, e.values), "expected: %v, got: %v (key: %s, ver:%d)",
			e.values, values, e.keys, e.ver)
	}
}
