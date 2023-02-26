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

	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/codec"
	"github.com/vescale/zgraph/datum"
	"github.com/vescale/zgraph/internal/logutil"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/planner"
	"github.com/vescale/zgraph/storage/kv"
)

// InsertExec represents the executor of INSERT statement.
type InsertExec struct {
	baseExecutor

	done       bool
	graph      *catalog.Graph
	insertions []*planner.ElementInsertion
	kvs        []kv.Pair
	buffer     []byte
	encoder    *codec.PropertyEncoder
	decoder    *codec.PropertyDecoder
	matchExec  Executor
}

// Open implements the Executor interface.
func (e *InsertExec) Open(ctx context.Context) error {
	if e.matchExec != nil {
		if err := e.matchExec.Open(ctx); err != nil {
			return err
		}
	} else {
		var kvs int
		// Precalculate key/value count and preallocate ID count.
		for _, insertion := range e.insertions {
			// Every label has a unique key.
			kvs += len(insertion.Labels)
			switch insertion.Type {
			case ast.InsertionTypeVertex:
				kvs++
			case ast.InsertionTypeEdge:
				// Every edge will write two key/value: src <-> dst
				kvs += 2
			}
		}
		e.kvs = make([]kv.Pair, 0, kvs)
	}
	return nil
}

// Next implements the Executor interface.
func (e *InsertExec) Next(ctx context.Context) (datum.Row, error) {
	if e.done {
		return nil, nil
	}
	e.done = true

	if len(e.insertions) == 0 {
		return nil, nil
	}

	var err error
	if e.matchExec == nil {
		err = e.encodeInsertions(nil)
	} else {
		err = e.encodeInsertionsFromMatch(ctx)
	}
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

func (e *InsertExec) encodeInsertions(matchRow datum.Row) error {
	graphID := e.graph.Meta().ID
	idRange, err := e.sc.AllocID(e.graph, len(e.insertions))
	if err != nil {
		return err
	}

	for _, insertion := range e.insertions {
		switch insertion.Type {
		case ast.InsertionTypeVertex:
			vertexID, err := idRange.Next()
			if err != nil {
				return err
			}
			if err := e.encodeVertex(graphID, vertexID, insertion, matchRow); err != nil {
				return err
			}
		case ast.InsertionTypeEdge:
			if err := e.encodeEdge(graphID, insertion, matchRow); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *InsertExec) encodeInsertionsFromMatch(ctx context.Context) error {
	for {
		row, err := e.matchExec.Next(ctx)
		if err != nil {
			return err
		}
		if row == nil {
			return nil
		}
		if err := e.encodeInsertions(row); err != nil {
			return err
		}
	}
}

func (e *InsertExec) encodeVertex(graphID, vertexID int64, insertion *planner.ElementInsertion, matchRow datum.Row) error {
	key := codec.VertexKey(graphID, vertexID)
	var (
		propertyIDs []uint16
		values      datum.Row
	)
	for _, assignment := range insertion.Assignments {
		value, err := assignment.Expr.Eval(e.sc, matchRow)
		if err != nil {
			return err
		}
		propertyIDs = append(propertyIDs, assignment.PropertyRef.Property.ID)
		values = append(values, value)
	}
	var labelIDs []uint16
	for _, label := range insertion.Labels {
		labelIDs = append(labelIDs, uint16(label.Meta().ID))
	}
	ret, err := e.encoder.Encode(e.buffer, labelIDs, propertyIDs, values)
	if err != nil {
		return err
	}
	val := make([]byte, len(ret))
	copy(val, ret)
	e.kvs = append(e.kvs, kv.Pair{Key: key, Val: val})
	return nil
}

func (e *InsertExec) encodeEdge(graphID int64, insertion *planner.ElementInsertion, matchRow datum.Row) error {
	// TODO: Edge also need an unique ID. How to encode and index it? See https://pgql-lang.org/spec/1.5/#id.
	var (
		propertyIDs []uint16
		values      []datum.Datum
	)
	for _, assignment := range insertion.Assignments {
		value, err := assignment.Expr.Eval(e.sc, matchRow)
		if err != nil {
			return err
		}
		propertyIDs = append(propertyIDs, assignment.PropertyRef.Property.ID)
		values = append(values, value)
	}

	srcIDVal, err := insertion.FromIDExpr.Eval(e.sc, matchRow)
	if err != nil {
		return err
	}
	dstIDVal, err := insertion.ToIDExpr.Eval(e.sc, matchRow)
	if err != nil {
		return err
	}

	srcID := datum.AsInt(srcIDVal)
	dstID := datum.AsInt(dstIDVal)

	var labelIDs []uint16
	for _, label := range insertion.Labels {
		labelIDs = append(labelIDs, uint16(label.Meta().ID))
	}
	ret, err := e.encoder.Encode(e.buffer, labelIDs, propertyIDs, values)
	if err != nil {
		return err
	}
	val := make([]byte, len(ret))
	copy(val, ret)
	e.kvs = append(e.kvs, kv.Pair{Key: codec.IncomingEdgeKey(graphID, srcID, dstID), Val: val})
	e.kvs = append(e.kvs, kv.Pair{Key: codec.OutgoingEdgeKey(graphID, srcID, dstID), Val: val})
	return nil
}

func (e *InsertExec) Close() error {
	if e.matchExec != nil {
		return e.matchExec.Close()
	}
	return nil
}
