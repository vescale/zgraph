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

package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/internal/chunk"
	"github.com/vescale/zgraph/meta"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/storage/kv"
)

// DDLExec is used to execute a DDL operation.
type DDLExec struct {
	baseExecutor

	sc        *stmtctx.Context
	done      bool
	statement ast.DDLNode
}

// Next implements the Executor interface.
func (e *DDLExec) Next(_ context.Context, _ *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true

	// TODO: prevent executing DDL in transaction context.
	// Prevent executing DDL concurrently.
	e.sc.Catalog().MDLock()
	defer e.sc.Catalog().MDUnlock()

	var patch *catalog.Patch
	err := kv.RunNewTxn(e.sc.Store(), func(txn kv.Transaction) error {
		m := meta.New(txn)
		var err error
		switch stmt := e.statement.(type) {
		case *ast.CreateGraphStmt:
			patch, err = e.createGraph(m, stmt)
		case *ast.DropGraphStmt:
			patch, err = e.dropGraph(m, stmt)
		case *ast.CreateLabelStmt:
			patch, err = e.createLabel(m, stmt)
		case *ast.DropLabelStmt:
			patch, err = e.dropLabel(m, stmt)

		default:
			return errors.Errorf("unknown DDL(%T)", e.statement)
		}
		return err
	})
	if err != nil {
		return err
	}

	// Apply the patch to catalog after the DDL changes have persistent in storage.
	if patch != nil {
		e.sc.Catalog().Apply(patch)
	}
	return nil
}

func (e *DDLExec) createGraph(m *meta.Meta, stmt *ast.CreateGraphStmt) (*catalog.Patch, error) {
	graph := e.sc.Catalog().Graph(stmt.Graph.L)
	if graph != nil {
		if stmt.IfNotExists {
			return nil, nil
		}
		return nil, meta.ErrGraphExists
	}

	// Persistent to storage.
	id, err := m.NextGlobalID()
	if err != nil {
		return nil, err
	}
	graphInfo := &model.GraphInfo{
		ID:    id,
		Name:  stmt.Graph,
		Query: stmt.Text(),
	}
	err = m.CreateGraph(graphInfo)
	if err != nil {
		return nil, err
	}

	patch := &catalog.Patch{
		Type: catalog.PatchTypeCreateGraph,
		Data: graphInfo,
	}
	return patch, nil
}

func (e *DDLExec) dropGraph(m *meta.Meta, stmt *ast.DropGraphStmt) (*catalog.Patch, error) {
	graph := e.sc.Catalog().Graph(stmt.Graph.L)
	if graph == nil {
		if stmt.IfExists {
			return nil, nil
		}
		return nil, meta.ErrGraphNotExists
	}

	// Persistent to storage.
	graphID := graph.Meta().ID
	labels := graph.Labels()
	for _, label := range labels {
		err := m.DropLabel(graphID, label.Meta().ID)
		if err != nil {
			return nil, err
		}
	}
	err := m.DropGraph(graphID)
	if err != nil {
		return nil, err
	}

	patch := &catalog.Patch{
		Type: catalog.PatchTypeDropGraph,
		Data: graph.Meta(),
	}
	return patch, nil
}

func (e *DDLExec) createLabel(m *meta.Meta, stmt *ast.CreateLabelStmt) (*catalog.Patch, error) {
	graphName := e.sc.CurrentGraph()
	graph := e.sc.Catalog().Graph(graphName)
	if graph == nil {
		return nil, meta.ErrGraphNotExists
	}
	label := graph.Label(stmt.Label.L)
	if label != nil {
		if stmt.IfNotExists {
			return nil, nil
		}
		return nil, meta.ErrLabelExists
	}

	var properties []*model.PropertyInfo
	for _, p := range stmt.Properties {
		id, err := m.NextGlobalID()
		if err != nil {
			return nil, err
		}
		prop := &model.PropertyInfo{
			ID:   id,
			Name: p.Name,
			Type: p.Type,
		}
		for _, opt := range p.Options {
			switch opt.Type {
			case ast.LabelPropertyOptionTypeNotNull:
				prop.Flag |= model.PropertyFlagNotNull
			case ast.LabelPropertyOptionTypeNull:
				prop.Flag |= model.PropertyFlagNull
			case ast.LabelPropertyOptionTypeDefault:
				prop.Flag |= model.PropertyFlagDefault
				// TODO: set the default value.
			case ast.LabelPropertyOptionTypeComment:
				prop.Flag |= model.PropertyFlagComment
				prop.Comment = opt.Data.(string)
			}
		}
		properties = append(properties, prop)
	}

	// Persistent to storage.
	id, err := m.NextGlobalID()
	if err != nil {
		return nil, err
	}
	labelInfo := &model.LabelInfo{
		ID:         id,
		Name:       stmt.Label,
		Query:      stmt.Text(),
		Properties: properties,
	}
	err = m.CreateLabel(graph.Meta().ID, labelInfo)
	if err != nil {
		return nil, err
	}

	patch := &catalog.Patch{
		Type: catalog.PatchTypeCreateLabel,
		Data: &catalog.PatchLabel{
			GraphID:   graph.Meta().ID,
			LabelInfo: labelInfo,
		},
	}
	return patch, nil
}

func (e *DDLExec) dropLabel(m *meta.Meta, stmt *ast.DropLabelStmt) (*catalog.Patch, error) {
	graphName := e.sc.CurrentGraph()
	graph := e.sc.Catalog().Graph(graphName)
	if graph == nil {
		return nil, meta.ErrGraphNotExists
	}
	label := graph.Label(stmt.Label.L)
	if label == nil {
		if stmt.IfExists {
			return nil, nil
		}
		return nil, meta.ErrLabelNotExists
	}

	// Persistent to storage.
	err := m.DropLabel(graph.Meta().ID, label.Meta().ID)
	if err != nil {
		return nil, err
	}

	patch := &catalog.Patch{
		Type: catalog.PatchTypeDropLabel,
		Data: &catalog.PatchLabel{
			GraphID:   graph.Meta().ID,
			LabelInfo: label.Meta(),
		},
	}
	return patch, nil
}
