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

package mvcc

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
)

func TestLockDecoder_Decode(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(tmpDir, &pebble.Options{})
	assert.Nil(t, err)

	writes := db.NewBatch()

	// Mock data
	data := []struct {
		key []byte
		val []byte
		ts  uint64
	}{
		{key: []byte("test"), val: []byte("test1"), ts: 1},
		{key: []byte("test"), val: []byte("test3"), ts: 3},
		{key: []byte("test1"), val: []byte("test5"), ts: 5},
		{key: []byte("test1"), val: []byte("test7"), ts: 7},
		{key: []byte("test2"), val: []byte("test9"), ts: 9},
	}
	wo := &pebble.WriteOptions{}
	for _, d := range data {
		v := Value{
			Type:     ValueTypePut,
			StartTS:  d.ts,
			CommitTS: d.ts + 1,
			Value:    d.val,
		}
		val, err := v.MarshalBinary()
		assert.Nil(t, err)
		assert.NotNil(t, val)
		err = writes.Set(Encode(d.key, d.ts+1), val, wo)
		assert.Nil(t, err)
	}

	// Mock lock
	locks := []struct {
		key []byte
		val []byte
		ts  uint64
	}{
		{key: []byte("test"), val: []byte("test1"), ts: 1},
		{key: []byte("test1"), val: []byte("test5"), ts: 5},
		{key: []byte("test2"), val: []byte("test9"), ts: 9},
	}
	for _, d := range locks {
		l := Lock{
			Primary: []byte("test"),
			StartTS: d.ts,
			Op:      Op_Put,
			Value:   d.val,
			TTL:     5,
		}
		val, err := l.MarshalBinary()
		assert.Nil(t, err)
		assert.NotNil(t, val)
		err = writes.Set(Encode(d.key, LockVer), val, wo)
		assert.Nil(t, err)
	}

	err = db.Apply(writes, wo)
	assert.Nil(t, err)

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: Encode([]byte("test"), LockVer),
		UpperBound: Encode([]byte("test1"), LockVer),
	})
	ok := iter.First()
	assert.True(t, ok)
	assert.True(t, iter.Valid())
	assert.Nil(t, iter.Error())

	decoder := LockDecoder{ExpectKey: []byte("test")}
	ok, err = decoder.Decode(iter)
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.True(t, bytes.Equal(decoder.Lock.Value, []byte("test1")))
}

func TestValueDecoder_Decode(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(tmpDir, &pebble.Options{})
	assert.Nil(t, err)

	writes := db.NewBatch()

	// Mock data
	data := []struct {
		key []byte
		val []byte
		ts  uint64
	}{
		{key: []byte("test"), val: []byte("test1"), ts: 1},
		{key: []byte("test"), val: []byte("test3"), ts: 3},
		{key: []byte("test1"), val: []byte("test5"), ts: 5},
		{key: []byte("test1"), val: []byte("test7"), ts: 7},
		{key: []byte("test2"), val: []byte("test9"), ts: 9},
	}
	wo := &pebble.WriteOptions{}
	for _, d := range data {
		v := Value{
			Type:     ValueTypePut,
			StartTS:  d.ts,
			CommitTS: d.ts + 1,
			Value:    d.val,
		}
		val, err := v.MarshalBinary()
		assert.Nil(t, err)
		assert.NotNil(t, val)
		err = writes.Set(Encode(d.key, d.ts+1), val, wo)
		assert.Nil(t, err)
	}

	err = db.Apply(writes, wo)
	assert.Nil(t, err)

	expected := []struct {
		key []byte
		val []byte
	}{
		{[]byte("test"), []byte("test3")},
		{[]byte("test1"), []byte("test7")},
		{[]byte("test2"), []byte("test9")},
	}

	for _, e := range expected {
		iter := db.NewIter(&pebble.IterOptions{
			LowerBound: Encode(e.key, LockVer),
		})
		ok := iter.First()
		assert.True(t, ok)
		assert.True(t, iter.Valid())
		assert.Nil(t, iter.Error())

		decoder := ValueDecoder{ExpectKey: e.key}
		ok, err = decoder.Decode(iter)
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.True(t, bytes.Equal(decoder.Value.Value, e.val))
	}
}

func TestSkipDecoder_Decode(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := pebble.Open(tmpDir, &pebble.Options{})
	assert.Nil(t, err)

	writes := db.NewBatch()

	// Mock data
	data := []struct {
		key []byte
		val []byte
		ts  uint64
	}{
		{key: []byte("test"), val: []byte("test1"), ts: 1},
		{key: []byte("test"), val: []byte("test3"), ts: 3},
		{key: []byte("test1"), val: []byte("test5"), ts: 5},
		{key: []byte("test1"), val: []byte("test7"), ts: 7},
		{key: []byte("test2"), val: []byte("test9"), ts: 9},
	}
	wo := &pebble.WriteOptions{}
	for _, d := range data {
		v := Value{
			Type:     ValueTypePut,
			StartTS:  d.ts,
			CommitTS: d.ts + 1,
			Value:    d.val,
		}
		val, err := v.MarshalBinary()
		assert.Nil(t, err)
		assert.NotNil(t, val)
		err = writes.Set(Encode(d.key, d.ts+1), val, wo)
		assert.Nil(t, err)
	}

	err = db.Apply(writes, wo)
	assert.Nil(t, err)

	expected := []struct {
		key  []byte
		next []byte
	}{
		{[]byte("test"), []byte("test1")},
		{[]byte("test1"), []byte("test2")},
		{[]byte("test2"), nil},
	}

	for _, e := range expected {
		iter := db.NewIter(&pebble.IterOptions{
			LowerBound: Encode(e.key, LockVer),
		})
		ok := iter.First()
		assert.True(t, ok)
		assert.True(t, iter.Valid())
		assert.Nil(t, iter.Error())

		decoder := SkipDecoder{CurrKey: e.key}
		ok, err = decoder.Decode(iter)
		assert.Nil(t, err)
		if e.next != nil {
			assert.True(t, ok)
			assert.True(t, bytes.Equal(decoder.CurrKey, e.next))
		}
	}
}
