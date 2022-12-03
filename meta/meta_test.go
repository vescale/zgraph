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

package meta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph/storage"
	"github.com/vescale/zgraph/storage/kv"
)

func TestMeta_GlobalID(t *testing.T) {
	assert := assert.New(t)
	store, err := storage.Open(t.TempDir())
	assert.Nil(err)

	err = kv.RunNewTxnContext(context.Background(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		id, err := meta.GlobalID()
		assert.Nil(err)
		assert.Zero(id)
		return nil
	})
	assert.Nil(err)

	err = kv.RunNewTxnContext(context.Background(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		id, err := meta.NextGlobalID()
		assert.Nil(err)
		assert.Equal(int64(1), id)
		return nil
	})
	assert.Nil(err)

	err = kv.RunNewTxnContext(context.Background(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		id, err := meta.AdvanceGlobalID(10)
		assert.Nil(err)
		assert.Equal(int64(1), id)

		id, err = meta.GlobalID()
		assert.Nil(err)
		assert.Equal(int64(11), id)
		return nil
	})
	assert.Nil(err)

	err = kv.RunNewTxnContext(context.Background(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		ids, err := meta.GenGlobalIDs(3)
		assert.Nil(err)
		assert.Equal([]int64{12, 13, 14}, ids)
		return nil
	})
	assert.Nil(err)
}
