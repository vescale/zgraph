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
	catalog   *catalog.Catalog
}

// Next implements the Executor interface.
func (e *DDLExec) Next(_ context.Context, _ *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true

	// TODO: prevent executing DDL in transaction context.
	// Prevent executing DDL concurrently.
	e.catalog.MDLock()
	defer e.catalog.MDUnlock()

	var patch *catalog.Patch
	err := kv.RunNewTxn(e.sc.Store(), func(txn kv.Transaction) error {
		m := meta.New(txn)
		var err error
		switch stmt := e.statement.(type) {
		case *ast.CreateGraphStmt:
			patch, err = e.createGraph(m, stmt)
		case *ast.DropGraphStmt:
			patch, err = e.dropGraph(m, stmt)
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
		e.catalog.Apply(patch)
	}
	return nil
}

func (e *DDLExec) createGraph(m *meta.Meta, stmt *ast.CreateGraphStmt) (*catalog.Patch, error) {
	graph := e.catalog.Graph(stmt.Graph.L)
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
		ID:   id,
		Name: stmt.Graph,
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
	graph := e.catalog.Graph(stmt.Graph.L)
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