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

//go:build ignore

package planner_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph"
	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/parser"
	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/planner"
	"github.com/vescale/zgraph/stmtctx"
)

func TestBuilder_BuildMatch(t *testing.T) {
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
		check func(proj *planner.LogicalProjection)
	}{
		// Catalog information refer: initCatalog
		{
			query: "SELECT * FROM MATCH (n:A)->(m:B)",
			check: func(proj *planner.LogicalProjection) {
				match := proj.Children()[0].(*planner.LogicalMatch)
				assert.Equal(1, len(match.Subgraphs))
				assert.Equal(1, len(match.Subgraphs[0].Paths))
				assert.Equal(2, len(match.Subgraphs[0].Paths[0].Vertices))
			},
		},
		{
			query: "SELECT * FROM MATCH ( (n:A)->(m:B), (c:C)->(m:B) )",
			check: func(proj *planner.LogicalProjection) {
				match := proj.Children()[0].(*planner.LogicalMatch)
				assert.Equal(1, len(match.Subgraphs))
				assert.Equal(2, len(match.Subgraphs[0].Paths))
				assert.Equal(2, len(match.Subgraphs[0].Paths[0].Vertices))
			},
		},
		{
			query: "SELECT * FROM MATCH (n:A)->(m:B), MATCH (c:C)->(m:B)",
			check: func(proj *planner.LogicalProjection) {
				match := proj.Children()[0].(*planner.LogicalMatch)
				assert.Equal(2, len(match.Subgraphs))
				assert.Equal(1, len(match.Subgraphs[0].Paths))
				assert.Equal(2, len(match.Subgraphs[0].Paths[0].Vertices))
			},
		},
	}

	for _, c := range cases {
		parser := parser.New()
		stmt, err := parser.ParseOneStmt(c.query)
		assert.Nil(err, c.query)

		sc := stmtctx.New(db.Store(), db.Catalog())
		sc.SetCurrentGraphName("graph101")

		builder := planner.NewBuilder(sc)
		plan, err := builder.Build(stmt)
		assert.Nil(err)
		projection, ok := plan.(*planner.LogicalProjection)
		assert.True(ok)
		c.check(projection)
	}
}
