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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph/storage/kv"
)

func TestTxn_Commit(t *testing.T) {
	assert := assert.New(t)

	storage, err := Open(t.TempDir())
	assert.Nil(err)
	assert.NotNil(storage)
	defer storage.Close()

	cases := []struct {
		keys []string
		vals []string
	}{
		{
			keys: []string{"a"},
			vals: []string{"b"},
		},
		// Overwrite same key.
		{
			keys: []string{"a"},
			vals: []string{"c"},
		},
		// Multiple key transaction
		{
			keys: []string{"c", "d", "e"},
			vals: []string{"x", "y", "z"},
		},
	}
	for _, c := range cases {
		txn, err := storage.Begin()
		assert.Nil(err)
		assert.NotNil(txn)
		for i, k := range c.keys {
			err := txn.Set(kv.Key(k), []byte(c.vals[i]))
			assert.Nil(err)
		}
		err = txn.Commit(context.Background())
		assert.Nil(err)

		// Validate the data
		snapshot, err := storage.Snapshot(storage.CurrentVersion())
		assert.Nil(err)
		for i, k := range c.keys {
			val, err := snapshot.Get(context.Background(), kv.Key(k))
			assert.Nil(err)
			assert.Equal(c.vals[i], string(val))
		}

		// Batch get interface.
		var keys []kv.Key
		var vals = map[string][]byte{}
		for i, k := range c.keys {
			keys = append(keys, kv.Key(k))
			vals[k] = []byte(c.vals[i])
		}
		res, err := snapshot.BatchGet(context.Background(), keys)
		assert.Nil(err)
		assert.Equal(vals, res)
	}
}

func TestTxn_Iter(t *testing.T) {
	assert := assert.New(t)

	storage, err := Open(t.TempDir())
	assert.Nil(err)
	assert.NotNil(storage)
	defer storage.Close()

	cases := []struct {
		keys   []string
		vals   []string
		order  []string
		result map[string]string
	}{
		{
			keys:  []string{"a"},
			vals:  []string{"b"},
			order: []string{"a"},
			result: map[string]string{
				"a": "b",
			},
		},
		// Overwrite same key.
		{
			keys:  []string{"a"},
			vals:  []string{"c"},
			order: []string{"a"},
			result: map[string]string{
				"a": "c",
			},
		},
		// Multiple key transaction
		{
			keys:  []string{"c", "d", "e"},
			vals:  []string{"x", "y", "z"},
			order: []string{"a", "c", "d", "e"},
			result: map[string]string{
				"a": "c",
				"c": "x",
				"d": "y",
				"e": "z",
			},
		},
	}
	for _, c := range cases {
		txn, err := storage.Begin()
		assert.Nil(err)
		assert.NotNil(txn)
		for i, k := range c.keys {
			err := txn.Set(kv.Key(k), []byte(c.vals[i]))
			assert.Nil(err)
		}
		err = txn.Commit(context.Background())
		assert.Nil(err)

		// Validate the data
		snapshot, err := storage.Snapshot(storage.CurrentVersion())
		assert.Nil(err)
		iter, err := snapshot.Iter(nil, nil)
		assert.Nil(err)

		var order []string
		var result = map[string]string{}
		for iter.Valid() {
			key := iter.Key()
			val := iter.Value()
			order = append(order, string(key))
			result[string(key)] = string(val)
			err = iter.Next()
			assert.Nil(err)
		}
		assert.Equal(c.order, order)
		assert.Equal(c.result, result)
	}
}
