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
	"math"

	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/codec"
	"github.com/vescale/zgraph/expression"
	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/planner"
	"github.com/vescale/zgraph/storage/kv"
	"github.com/vescale/zgraph/types"
	"golang.org/x/exp/slices"
)

type MatchExec struct {
	baseExecutor

	subgraph *planner.Subgraph

	prepared bool
	visited  struct {
		vertices map[string]*planner.Vertex
		edges    map[string]*planner.Edge
	}
	matched map[string]*types.GraphVar
	results []expression.Row
	txn     kv.Transaction
}

func (m *MatchExec) Next(_ context.Context) (expression.Row, error) {
	if !m.prepared {
		if err := m.prepare(); err != nil {
			return nil, err
		}
		m.prepared = true
	}
	if len(m.results) == 0 {
		return nil, nil
	}
	row := m.results[0]
	m.results = m.results[1:]
	return row, nil
}

func (m *MatchExec) prepare() error {
	m.visited.vertices = make(map[string]*planner.Vertex)
	m.visited.edges = make(map[string]*planner.Edge)
	m.matched = make(map[string]*types.GraphVar)

	txn, err := m.sc.Store().Begin()
	if err != nil {
		return err
	}
	m.txn = txn

	var startVertex *planner.Vertex
	for _, vertex := range m.subgraph.Vertices {
		startVertex = vertex
		break
	}
	return m.iterVertex(startVertex, func(vertexVar *types.GraphVar) error {
		return m.search(startVertex, vertexVar)
	})
}

// search performs a depth-first search on the graph. curVertex is the current vertex to be visited.
// It guarantees that all visited vertices does not violate the subgraph constraints and all connections
// between visited vertices are visited.
func (m *MatchExec) search(curVertex *planner.Vertex, curVertexVar *types.GraphVar) error {
	m.visited.vertices[curVertex.Name.L] = curVertex
	m.matched[curVertex.Name.L] = curVertexVar
	defer func() {
		delete(m.visited.vertices, curVertex.Name.L)
		delete(m.matched, curVertex.Name.L)
	}()

	if m.isMatched() {
		m.appendResult()
		return nil
	}

	// TODO: match connections
	for _, conn := range m.subgraph.Connections {
		edge, ok := conn.(*planner.Edge)
		if !ok {
			return errors.Errorf("variable-length path is not supported yet")
		}
		_ = edge
	}

	// The subgraph is disconnected, so we need to find a new start vertex.
	for _, vertex := range m.subgraph.Vertices {
		_, visited := m.visited.vertices[vertex.Name.L]
		if visited {
			continue
		}
		if err := m.iterVertex(vertex, func(vertexVar *types.GraphVar) error {
			return m.search(vertex, vertexVar)
		}); err != nil {
			return err
		}
	}

	return nil
}

func (m *MatchExec) isMatched() bool {
	return len(m.visited.vertices) == len(m.subgraph.Vertices) && len(m.visited.edges) == len(m.subgraph.Connections)
}

func (m *MatchExec) appendResult() {
	result := make(expression.Row, 0, len(m.subgraph.SingletonVars))
	for _, singletonVar := range m.subgraph.SingletonVars {
		var d types.Datum
		d.SetGraphVar(m.matched[singletonVar.Name.L])
		result = append(result, d)
	}
	m.results = append(m.results, result)
}

func (m *MatchExec) iterVertex(vertex *planner.Vertex, f func(vertexVar *types.GraphVar) error) error {
	graph := m.sc.CurrentGraph()
	lower := codec.VertexKey(graph.Meta().ID, 0)
	upper := codec.VertexKey(graph.Meta().ID, math.MaxInt64)
	iter, err := m.txn.Iter(lower, upper)
	if err != nil {
		return err
	}
	defer iter.Close()

	var labelInfos []*model.LabelInfo
	for _, label := range graph.Labels() {
		labelInfos = append(labelInfos, label.Meta())
	}
	dec := codec.NewPropertyDecoder(labelInfos, graph.Properties())

	for ; err == nil && iter.Valid(); err = iter.Next() {
		// TODO: better way to skip edge keys
		if len(iter.Key()) != codec.VertexKeyLen {
			continue
		}
		_, vertexID, err := codec.ParseVertexKey(iter.Key())
		if err != nil {
			return err
		}

		labelIDs, propertyValues, err := dec.Decode(iter.Value())
		if err != nil {
			return err
		}

		// Check if the vertex matches the label requirements.
		if len(vertex.Labels) > 0 {
			var labelIDSlice []int64
			for labelID := range labelIDs {
				labelIDSlice = append(labelIDSlice, int64(labelID))
			}
			if !slices.ContainsFunc(labelIDSlice, func(labelID int64) bool {
				return slices.ContainsFunc(vertex.Labels, func(label *catalog.Label) bool {
					return label.Meta().ID == labelID
				})
			}) {
				continue
			}
		}

		graphVar := &types.GraphVar{
			ID:         vertexID,
			Properties: make(map[string]types.Datum),
		}
		for labelID := range labelIDs {
			graphVar.Labels = append(graphVar.Labels, graph.LabelByID(int64(labelID)).Meta().Name.L)
		}
		for propID, propVal := range propertyValues {
			propName := graph.PropertyByID(propID).Name.L
			graphVar.Properties[propName] = propVal
		}

		if err := f(graphVar); err != nil {
			return err
		}
	}
	return nil
}

func (m *MatchExec) Close() error {
	if m.txn != nil {
		return m.txn.Rollback()
	}
	return nil
}
