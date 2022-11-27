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

	expected := []struct {
		key string
		val string
		ts  uint64
	}{
		{"test", "test1", 200},
		{"test", "test1", 300},
		{"test", "test3", 310},
		{"test", "test3", 400},
		{"test1", "test5", 600},
		{"test1", "test7", 750},
	}
	for _, e := range expected {
		snapshot, err := s.Snapshot(mvcc.Version{Ver: e.ts})
		assert.Nil(t, err)
		val, err := snapshot.Get(context.Background(), []byte(e.key))
		assert.Nil(t, err)
		assert.True(t, bytes.Equal(val, []byte(e.val)), "expected: %v, got: %v (key: %s, ts:%d)",
			e.val, string(val), e.key, e.ts)
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

	expected := []struct {
		keys   []string
		values map[string][]byte
		ts     uint64
	}{
		{
			keys: []string{"test", "test1", "test2"},
			values: map[string][]byte{
				"test":  []byte("test1"),
				"test1": nil,
				"test2": nil,
			},
			ts: 200,
		},
		{
			keys: []string{"test", "test1", "test2"},
			values: map[string][]byte{
				"test":  []byte("test3"),
				"test1": []byte("test5"),
				"test2": nil,
			},
			ts: 510,
		},
		{
			keys: []string{"test", "test1", "test2"},
			values: map[string][]byte{
				"test":  []byte("test3"),
				"test1": []byte("test7"),
				"test2": []byte("test9"),
			},
			ts: 1000,
		},
	}
	for _, e := range expected {
		snapshot, err := s.Snapshot(mvcc.Version{Ver: e.ts})
		assert.Nil(t, err)
		var keys []Key
		for _, k := range e.keys {
			keys = append(keys, Key(k))
		}
		values, err := snapshot.BatchGet(context.Background(), keys)
		assert.Nil(t, err)
		assert.True(t, reflect.DeepEqual(values, e.values), "expected: %v, got: %v (key: %s, ts:%d)",
			e.values, values, e.keys, e.ts)
	}
}
