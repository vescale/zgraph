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

package catalog

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph/meta"
	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/storage"
	"github.com/vescale/zgraph/storage/kv"
)

func Test_Load(t *testing.T) {
	assert := assert.New(t)
	store, err := storage.Open(t.TempDir())
	assert.Nil(err)
	defer store.Close()

	ID := atomic.Int64{}
	cases := []*model.GraphInfo{
		{
			ID:   ID.Add(1),
			Name: model.NewCIStr("graph1"),
			Labels: []*model.LabelInfo{
				{
					ID:   ID.Add(1),
					Name: model.NewCIStr("label1"),
				},
				{
					ID:   ID.Add(1),
					Name: model.NewCIStr("label2"),
				},
			},
		},
		{
			ID:   ID.Add(1),
			Name: model.NewCIStr("graph2"),
			Labels: []*model.LabelInfo{
				{
					ID:   ID.Add(1),
					Name: model.NewCIStr("label1"),
				},
				{
					ID:   ID.Add(1),
					Name: model.NewCIStr("label2"),
					Indexes: []*model.IndexInfo{
						{
							ID:   ID.Add(1),
							Name: model.NewCIStr("label2_index"),
						},
						{
							ID:   ID.Add(1),
							Name: model.NewCIStr("label2_index2"),
						},
					},
				},
			},
		},
	}

	// Create mock data.
	err = kv.RunNewTxn(store, func(txn kv.Transaction) error {
		meta := meta.New(txn)
		for _, g := range cases {
			err := meta.CreateGraph(g)
			assert.Nil(err)
			for _, l := range g.Labels {
				err := meta.CreateLabel(g.ID, l)
				assert.Nil(err)
			}
		}
		return nil
	})
	assert.Nil(err)

	snapshot, err := store.Snapshot(store.CurrentVersion())
	assert.Nil(err)

	catalog, err := Load(snapshot)
	assert.Nil(err)
	assert.Equal(len(cases), len(catalog.byName))
	assert.Equal(len(catalog.byID), len(catalog.byName))

	for _, g := range cases {
		graph := catalog.Graph(g.Name.L)
		assert.Equal(g.ID, graph.Meta().ID)
		assert.Equal(graph, catalog.GraphByID(g.ID))

		// Labels
		for _, l := range g.Labels {
			label := graph.Label(l.Name.L)
			assert.Equal(l, label.Meta())
			assert.Equal(label, graph.LabelByID(l.ID))
		}
	}
}
