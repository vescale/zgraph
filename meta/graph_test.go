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

func TestGraph(t *testing.T) {
	assert := assert.New(t)
	store, err := storage.Open(t.TempDir())
	assert.Nil(err)

	graphNames := []string{
		"graph1",
		"graph2",
		"graph3",
		"graph4",
		"graph5",
	}

	// Create graphs
	for i, name := range graphNames {
		err := kv.RunNewTxnContext(context.TODO(), store, func(_ context.Context, txn kv.Transaction) error {
			meta := New(txn)
			info := &model.GraphInfo{
				ID:   int64(i) + 1,
				Name: model.NewCIStr(name),
			}
			return meta.CreateGraph(info)
		})
		assert.Nil(err)
	}

	// List graphs
	err = kv.RunNewTxnContext(context.TODO(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		graphs, err := meta.ListGraphs()
		assert.Nil(err)
		names := make([]string, 0, len(graphs))
		ids := make([]int64, 0, len(graphs))
		for _, g := range graphs {
			names = append(names, g.Name.L)
			ids = append(ids, g.ID)
		}

		assert.Equal(graphNames, names)
		assert.Equal([]int64{1, 2, 3, 4, 5}, ids)

		return nil
	})
	assert.Nil(err)

	// Update graph
	err = kv.RunNewTxnContext(context.TODO(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		err := meta.UpdateGraph(&model.GraphInfo{
			ID:   4,
			Name: model.NewCIStr("GRAPH4-modified"),
		})
		assert.Nil(err)
		return nil
	})
	assert.Nil(err)

	// Get graph
	err = kv.RunNewTxnContext(context.TODO(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		graph, err := meta.GetGraph(4)
		assert.Nil(err)
		assert.Equal(int64(4), graph.ID)
		assert.Equal("graph4-modified", graph.Name.L)
		return nil
	})
	assert.Nil(err)

	// Drop graph
	err = kv.RunNewTxnContext(context.TODO(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		err := meta.DropGraph(3)
		assert.Nil(err)
		return nil
	})
	assert.Nil(err)

	// List graphs again
	err = kv.RunNewTxnContext(context.TODO(), store, func(_ context.Context, txn kv.Transaction) error {
		meta := New(txn)
		graphs, err := meta.ListGraphs()
		assert.Nil(err)
		names := make([]string, 0, len(graphs))
		ids := make([]int64, 0, len(graphs))
		for _, g := range graphs {
			names = append(names, g.Name.L)
			ids = append(ids, g.ID)
		}

		graphNames := []string{
			"graph1",
			"graph2",
			"graph4-modified",
			"graph5",
		}
		assert.Equal(graphNames, names)
		assert.Equal([]int64{1, 2, 4, 5}, ids)

		return nil
	})
	assert.Nil(err)
}
