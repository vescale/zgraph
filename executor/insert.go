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
	"github.com/vescale/zgraph/codec"
	"github.com/vescale/zgraph/expression"
	"github.com/vescale/zgraph/internal/logutil"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/planner"
	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/storage/kv"
)

// InsertExec represents the executor of INSERT statement.
type InsertExec struct {
	baseExecutor

	done       bool
	graph      *catalog.Graph
	idRange    *stmtctx.IDRange
	insertions []*planner.ElementInsertion
	kvs        []kv.Pair
	buffer     []byte
	encoder    *codec.PropertyEncoder
	decoder    *codec.PropertyDecoder
}

// Open implements the Executor interface.
func (e *InsertExec) Open(_ context.Context) error {
	var kvs int
	var ids int
	// Precalculate key/value count and preallocate ID count.
	for _, insertion := range e.insertions {
		// Every label has a unique key.
		kvs += len(insertion.Labels)
		switch insertion.Type {
		case ast.InsertionTypeVertex:
			// Every vertex need to allocate an identifier.
			ids++
			kvs++
		case ast.InsertionTypeEdge:
			// Every edge will write two key/value: src <-> dst
			kvs += 2
		}
	}
	idRange, err := e.sc.AllocID(e.graph, ids)
	if err != nil {
		return err
	}

	e.idRange = idRange
	e.kvs = make([]kv.Pair, 0, kvs)

	return nil
}

// Next implements the Executor interface.
func (e *InsertExec) Next(_ context.Context) (Row, error) {
	if e.done {
		return nil, nil
	}
	e.done = true

	if len(e.insertions) == 0 {
		return nil, nil
	}

	err := e.encodeInsertions()
	if err != nil {
		return nil, err
	}
	if len(e.kvs) == 0 {
		return nil, nil
	}

	// FIXME: use transaction in stmtctx.Context
	err = kv.Txn(e.sc.Store(), func(txn kv.Transaction) error {
		for _, pair := range e.kvs {
			err := txn.Set(pair.Key, pair.Val)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		logutil.Errorf("Insert vertices/edges failed: %+v", e.insertions)
	}

	return nil, err
}

func (e *InsertExec) encodeInsertions() error {
	graphID := e.graph.Meta().ID
	for _, insertion := range e.insertions {
		switch insertion.Type {
		case ast.InsertionTypeVertex:
			vertexID, err := e.idRange.Next()
			if err != nil {
				return err
			}
			if len(insertion.Labels) > 0 {
				e.encodeLabels(graphID, vertexID, 0, insertion.Labels)
			}
			err = e.encodeVertex(graphID, vertexID, insertion)
			if err != nil {
				return err
			}

		case ast.InsertionTypeEdge:
			// TODO: implement the edge codec.
		}
	}

	return nil
}

func (e *InsertExec) encodeLabels(graphID, vertexID, dstVertexID int64, labels []*catalog.Label) {
	for _, label := range labels {
		key := codec.LabelKey(graphID, label.Meta().ID, vertexID, dstVertexID)
		e.kvs = append(e.kvs, kv.Pair{Key: key})
		if dstVertexID != 0 {
			key := codec.LabelKey(graphID, label.Meta().ID, dstVertexID, vertexID)
			e.kvs = append(e.kvs, kv.Pair{Key: key})
		}
	}
}

func (e *InsertExec) encodeVertex(graphID, vertexID int64, insertion *planner.ElementInsertion) error {
	key := codec.VertexKey(graphID, vertexID)
	var propertyIDs []uint16
	var row Row
	for _, assignment := range insertion.Assignments {
		constant, ok := assignment.Expr.(*expression.Constant)
		if !ok {
			return errors.Errorf("unsupported expression: %T", assignment.Expr)
		}
		propertyIDs = append(propertyIDs, assignment.PropertyRef.Property.ID)
		row = append(row, constant.Value)
	}
	ret, err := e.encoder.Encode(e.buffer, propertyIDs, row)
	if err != nil {
		return err
	}
	val := make([]byte, len(ret))
	copy(val, ret)
	e.kvs = append(e.kvs, kv.Pair{Key: key, Val: val})
	return nil
}
