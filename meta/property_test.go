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
	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/storage"
	"github.com/vescale/zgraph/storage/kv"
)

func TestProperty(t *testing.T) {
	assert := assert.New(t)
	store, err := storage.Open(t.TempDir())
	assert.Nil(err)

	var graphID = int64(100)

	err = kv.RunNewTxnContext(context.TODO(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		info := &model.GraphInfo{
			ID:   graphID,
			Name: model.NewCIStr("test-graph"),
		}
		return meta.CreateGraph(info)
	})
	assert.Nil(err)

	propertyNames := []string{
		"property1",
		"property2",
		"property3",
		"property4",
		"property5",
	}

	// Create propertys
	for i, name := range propertyNames {
		err := kv.RunNewTxnContext(context.TODO(), store, func(_ context.Context, txn kv.Transaction) error {
			meta := New(txn)
			info := &model.PropertyInfo{
				ID:   int64(i) + 1,
				Name: model.NewCIStr(name),
			}
			return meta.CreateProperty(graphID, info)
		})
		assert.Nil(err)
	}

	// List properties
	err = kv.RunNewTxnContext(context.TODO(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		propertys, err := meta.ListProperties(graphID)
		assert.Nil(err)
		names := make([]string, 0, len(propertys))
		ids := make([]int64, 0, len(propertys))
		for _, g := range propertys {
			names = append(names, g.Name.L)
			ids = append(ids, g.ID)
		}

		assert.Equal(propertyNames, names)
		assert.Equal([]int64{1, 2, 3, 4, 5}, ids)

		return nil
	})
	assert.Nil(err)

	// Update property
	err = kv.RunNewTxnContext(context.TODO(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		err := meta.UpdateProperty(graphID, &model.PropertyInfo{
			ID:   4,
			Name: model.NewCIStr("PROPERTY4-modified"),
		})
		assert.Nil(err)
		return nil
	})
	assert.Nil(err)

	// Get property
	err = kv.RunNewTxnContext(context.TODO(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		property, err := meta.GetProperty(graphID, 4)
		assert.Nil(err)
		assert.Equal(int64(4), property.ID)
		assert.Equal("property4-modified", property.Name.L)
		return nil
	})
	assert.Nil(err)

	// Drop property
	err = kv.RunNewTxnContext(context.TODO(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		err := meta.DropProperty(graphID, 3)
		assert.Nil(err)
		return nil
	})
	assert.Nil(err)

	// List propertys again
	err = kv.RunNewTxnContext(context.TODO(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		propertys, err := meta.ListProperties(graphID)
		assert.Nil(err)
		names := make([]string, 0, len(propertys))
		ids := make([]int64, 0, len(propertys))
		for _, g := range propertys {
			names = append(names, g.Name.L)
			ids = append(ids, g.ID)
		}

		propertyNames := []string{
			"property1",
			"property2",
			"property4-modified",
			"property5",
		}
		assert.Equal(propertyNames, names)
		assert.Equal([]int64{1, 2, 4, 5}, ids)

		return nil
	})
	assert.Nil(err)
}
