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

package compiler_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph"
	"github.com/vescale/zgraph/compiler"
	"github.com/vescale/zgraph/meta"
	"github.com/vescale/zgraph/parser"
	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/storage"
	"github.com/vescale/zgraph/storage/kv"
)

func initCatalog(assert *assert.Assertions, dirname string) {
	store, err := storage.Open(dirname)
	assert.Nil(err)
	defer store.Close()

	// Create
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
				},
			},
			Indexes: []*model.IndexInfo{
				{
					ID:   ID.Add(1),
					Name: model.NewCIStr("index"),
				},
				{
					ID:   ID.Add(1),
					Name: model.NewCIStr("index2"),
				},
			},
			Properties: []*model.PropertyInfo{
				{
					ID:   uint16(ID.Add(1)),
					Name: model.NewCIStr("property"),
				},
				{
					ID:   uint16(ID.Add(1)),
					Name: model.NewCIStr("property2"),
				},
			},
		},
	}

	// Create mock data.
	err = kv.Txn(store, func(txn kv.Transaction) error {
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
}

func TestPreprocess(t *testing.T) {
	assert := assert.New(t)
	tempDir := t.TempDir()
	initCatalog(assert, tempDir)

	cases := []struct {
		graph string
		query string
		err   string
	}{
		{
			query: "create graph graph1",
			err:   "graph exists",
		},
		{
			query: "drop graph graph1",
		},
		{
			query: "drop graph graph4",
			err:   "graph not exists",
		},
		{
			query: "drop graph if exists graph4",
		},
		{
			query: "create graph if not exists graph1",
		},
		{
			graph: "graph0",
			query: "create label label1",
			err:   "graph not exists",
		},
		{
			graph: "graph1",
			query: "create label label1",
			err:   "label exists",
		},
		{
			graph: "graph1",
			query: "drop label label1",
		},
		{
			graph: "graph1",
			query: "drop label label4",
			err:   "label not exists",
		},
		{
			graph: "graph1",
			query: "drop label if exists label4",
		},
		{
			graph: "graph1",
			query: "create label if not exists label2",
		},
		{
			graph: "graph2",
			query: "create index if not exists idx_name (a, b)",
			err:   "property a: property not exists",
		},
		{
			graph: "graph2",
			query: "create index index2 (a, b)",
			err:   "index exists",
		},
		{
			graph: "graph2",
			query: "drop index index2",
		},
		{
			graph: "graph2",
			query: "drop index label4_index2",
			err:   "index not exists",
		},
		{
			graph: "graph2",
			query: "drop index if exists label4_index2",
		},
		{
			query: "use graph100",
			err:   "graph not exists",
		},
		{
			query: "use graph1",
		},
		{
			query: "INSERT VERTEX x",
			err:   "graph not exists",
		},
		{
			query: "INSERT INTO graph1 VERTEX x",
		},
		{
			query: "INSERT INTO graph1 VERTEX x LABELS (label1, label2) PROPERTIES ( x.name = 'test')",
		},
		{
			query: "INSERT INTO graph1 VERTEX x LABELS (label0, label2) PROPERTIES ( x.name = 'test')",
			err:   "label not exists",
		},
		{
			query: "INSERT INTO graph1 VERTEX x LABELS (label1, label2) PROPERTIES ( y.name = 'test')",
			err:   "reference not exists variable",
		},
		{
			graph: "graph2",
			query: "INSERT VERTEX x LABELS (label1, label2) PROPERTIES ( y.name = 'test')",
			err:   "reference not exists variable",
		},
	}

	db, err := zgraph.Open(tempDir, nil)
	assert.Nil(err)

	for _, c := range cases {
		parser := parser.New()
		stmt, err := parser.ParseOneStmt(c.query)
		assert.Nil(err)
		sc := stmtctx.New(db.Store(), db.Catalog())
		sc.SetCurrentGraphName(c.graph)

		prep := compiler.NewPreprocess(sc)
		stmt.Accept(prep)
		if c.err == "" {
			assert.Nil(prep.Error(), c.query)
		} else {
			assert.ErrorContains(prep.Error(), c.err, c.query)
		}
	}
}
