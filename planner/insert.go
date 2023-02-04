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
	basePlan

	Graph      *catalog.Graph
	Insertions []*ElementInsertion
	MatchPlan  Plan
}

// ElementInsertion represents a graph insertion element.
type ElementInsertion struct {
	Type ast.InsertionType
	// INSERT EDGE e BETWEEN x AND y FROM MATCH (x) , MATCH (y) WHERE id(x) = 1 AND id(y) = 2
	// FromIDExpr and ToIDExpr are the expressions to get the ID of the source and destination vertex.
	FromIDExpr  expression.Expression
	ToIDExpr    expression.Expression
	Labels      []*catalog.Label
	Assignments []*expression.Assignment
}
