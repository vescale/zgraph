// Copyright 2023 zGraph Authors. All rights reserved.
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

package executor

import (
	"context"

	"github.com/vescale/zgraph/datum"
	"github.com/vescale/zgraph/meta"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/planner"
	"github.com/vescale/zgraph/types"
)

var showStmtColumns = [...][]planner.ResultColumn{
	ast.ShowTargetGraphs: {
		{Name: model.NewCIStr("graph"), Type: types.String},
	},
	ast.ShowTargetLabels: {
		{Name: model.NewCIStr("label"), Type: types.String},
	},
}

// ShowExec is used to execute the show statements.
type ShowExec struct {
	baseExecutor

	statement *ast.ShowStmt
	results   []datum.Row
	index     int
}

func (e *ShowExec) Open(ctx context.Context) error {
	switch e.statement.Tp {
	case ast.ShowTargetGraphs:
		graphs := e.sc.Catalog().Graphs()
		for _, g := range graphs {
			e.results = append(e.results, datum.Row{datum.NewString(g.Meta().Name.O)})
		}

	case ast.ShowTargetLabels:
		graphName := e.sc.CurrentGraphName()
		if !e.statement.GraphName.IsEmpty() {
			graphName = e.statement.GraphName.L
		}
		graph := e.sc.Catalog().Graph(graphName)
		if graph == nil {
			return meta.ErrGraphNotExists
		}
		labels := graph.Labels()
		for _, l := range labels {
			e.results = append(e.results, datum.Row{datum.NewString(l.Meta().Name.O)})
		}
	}
	return nil
}

func (e *ShowExec) Next(_ context.Context) (datum.Row, error) {
	if e.index >= len(e.results) {
		return nil, nil
	}

	row := e.results[e.index]
	e.index++

	return row, nil
}
