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
	"github.com/vescale/zgraph/expression"
	"github.com/vescale/zgraph/parser/ast"
)

// Insert represents the plan of INSERT statement.
type Insert struct {
	baseSchemaProducer

	Graph      *catalog.Graph
	Insertions []*ElementInsertion
}

// ElementInsertion represents a graph insertion element.
type ElementInsertion struct {
	Type             ast.InsertionType
	ElementReference *expression.VariableRef
	// INSERT EDGE e BETWEEN x AND y FROM MATCH (x) , MATCH (y) WHERE id(x) = 1 AND id(y) = 2
	// FromReference represents the source vertex of an edge.
	FromReference *expression.VariableRef
	// ToReference represents the destination vertex of an edge.
	ToReference *expression.VariableRef
	Labels      []*catalog.Label
	Assignments []*expression.Assignment
}
