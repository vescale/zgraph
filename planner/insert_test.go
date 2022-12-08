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

package planner_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph"
	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/parser"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/planner"
	"github.com/vescale/zgraph/stmtctx"
)

func TestBuilder_BuildInsert(t *testing.T) {
	assert := assert.New(t)

	db, err := zgraph.Open(t.TempDir(), nil)
	assert.Nil(err)

	// Prepare mock catalog.
	mockGraphs := []string{"graph101", "graph102"}
	mockLabels := []string{"A", "B", "C"}
	mockProps := []string{"name", "age"}
	id := atomic.Int64{}
	for _, g := range mockGraphs {
		graphID := id.Add(1)
		db.Catalog().Apply(&catalog.Patch{
			Type: catalog.PatchTypeCreateGraph,
			Data: &model.GraphInfo{
				ID:   graphID,
				Name: model.NewCIStr(g),
			},
		})
		for _, l := range mockLabels {
			db.Catalog().Apply(&catalog.Patch{
				Type: catalog.PatchTypeCreateLabel,
				Data: &catalog.PatchLabel{
					GraphID: graphID,
					LabelInfo: &model.LabelInfo{
						ID:   id.Add(1),
						Name: model.NewCIStr(l),
					},
				},
			})

		}
		var properties []*model.PropertyInfo
		for i, p := range mockProps {
			properties = append(properties, &model.PropertyInfo{
				ID:   uint16(i + 1),
				Name: model.NewCIStr(p),
			})
		}
		db.Catalog().Apply(&catalog.Patch{
			Type: catalog.PatchTypeCreateProperties,
			Data: &catalog.PatchProperties{
				MaxPropID:  uint16(len(properties)),
				GraphID:    graphID,
				Properties: properties,
			},
		})

	}

	cases := []struct {
		query string
		check func(insert *planner.Insert)
	}{
		// Catalog information refer: initCatalog
		{
			query: "insert vertex x labels(A, B, C)",
			check: func(insert *planner.Insert) {
				assert.Equal(1, len(insert.Insertions))
				assert.Equal(3, len(insert.Insertions[0].Labels))
			},
		},
		{
			query: "insert into graph102 vertex x labels(A, B, C)",
			check: func(insert *planner.Insert) {
				assert.Equal("graph102", insert.Graph.Meta().Name.L)
			},
		},
		{
			query: "insert vertex x properties (x.name = 'test')",
			check: func(insert *planner.Insert) {
				assert.Equal(1, len(insert.Insertions[0].Assignments))
			},
		},
		{
			query: `insert vertex x labels(A, B, C) properties (x.name = 'test'),
					       vertex y labels(A, B) properties (y.name = 'test'),
					       vertex z labels(B, C) properties (z.name = 'test')`,
			check: func(insert *planner.Insert) {
				assert.Equal(3, len(insert.Insertions))
			},
		},
		{
			query: `insert vertex x labels(A, B, C) properties (x.name = 'test'),
					       vertex y labels(A, B) properties (y.name = 'test'),
					       edge z between x and y labels(B, C) from match (x), match (y)`,
			check: func(insert *planner.Insert) {
				assert.Equal(3, len(insert.Insertions))
				assert.Equal(ast.InsertionTypeEdge, insert.Insertions[2].Type)
			},
		},
	}

	for _, c := range cases {
		parser := parser.New()
		stmt, err := parser.ParseOneStmt(c.query)
		assert.Nil(err, c.query)

		sc := stmtctx.New(db.Store(), db.Catalog())
		sc.SetCurrentGraph("graph101")

		builder := planner.NewBuilder(sc)
		plan, err := builder.Build(stmt)
		assert.Nil(err)
		insert, ok := plan.(*planner.Insert)
		assert.True(ok)
		c.check(insert)
	}
}
