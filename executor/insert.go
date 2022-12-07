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
	insertions []*planner.GraphInsertion
	kvs        []kv.Pair
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

	return nil, nil
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
			e.encodeVertex(graphID, vertexID, insertion)

		case ast.InsertionTypeEdge:
			// TODO: implement the edge codec.
		}
	}

	return nil
}

func (e *InsertExec) encodeLabels(graphID, vertexID, dstVertexID int64, labels []*catalog.Label) {
	for _, label := range labels {
		key := codec.LabelIndexKey(graphID, label.Meta().ID, vertexID, dstVertexID)
		e.kvs = append(e.kvs, kv.Pair{Key: key})
		if dstVertexID != 0 {
			key := codec.LabelIndexKey(graphID, label.Meta().ID, dstVertexID, vertexID)
			e.kvs = append(e.kvs, kv.Pair{Key: key})
		}
	}
}

func (e *InsertExec) encodeVertex(graphID, vertexID int64, insertion *planner.GraphInsertion) {
	// TODO: implement the vertex value codec.
	//key := codec.VertexKey(graphID, vertexID)
	//for _, assignment := range insertion.Assignments {
	//
	//}
}
