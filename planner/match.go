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

package planner

import (
	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/parser/model"
)

type VertexRef struct {
	Name   model.CIStr
	Labels []*catalog.Label
}

type PathPattern struct {
	Tp       ast.PathPatternType
	TopK     int64
	Vertices []*VertexRef
	// TODO: connections
}

type Subgraph struct {
	Graph *catalog.Graph
	Paths []*PathPattern
}

type LogicalMatch struct {
	logicalSchemaProducer

	Subgraphs []*Subgraph
}

type PhysicalMatch struct {
	physicalSchemaProducer
	Subgraphs []*Subgraph
}
