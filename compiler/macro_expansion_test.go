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

package compiler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph/parser"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/parser/opcode"
)

func TestMacroExpansion(t *testing.T) {
	assert := assert.New(t)

	cases := []struct {
		query string
		check func(node ast.Node)
	}{
		{
			query: `PATH has_parent AS () -[:has_father|has_mother]-> (:Person)
					SELECT ancestor.name
					  FROM MATCH (p1:Person) -/:has_parent+/-> (ancestor)
						 , MATCH (p2:Person) -/:has_parent+/-> (ancestor)
					 WHERE p1.name = 'Mario'
					   AND p2.name = 'Luigi'`,
			check: func(node ast.Node) {
				stmt := node.(*ast.SelectStmt)
				assert.Equal(1, len(stmt.PathPatternMacros))
				expr := stmt.From.Matches[0].Paths[0].Connections[0].(*ast.ReachabilityPathExpr)
				assert.Equal(expr.Macros["has_parent"], stmt.PathPatternMacros[0].Path)
			},
		},
		{
			query: `PATH connects_to AS (:Generator) -[:has_connector]-> (c:Connector) <-[:has_connector]- (:Generator)
					  WHERE c.status = 'OPERATIONAL'
					SELECT generatorA.location, generatorB.location
					  FROM MATCH (generatorA) -/:connects_to+/-> (generatorB)`,
			check: func(node ast.Node) {
				stmt := node.(*ast.SelectStmt)
				assert.Equal(1, len(stmt.PathPatternMacros))
				expr := stmt.From.Matches[0].Paths[0].Connections[0].(*ast.ReachabilityPathExpr)
				assert.Equal(expr.Macros["connects_to"], stmt.PathPatternMacros[0].Path)
				assert.NotNil(stmt.Where)
			},
		},
		{
			query: `PATH connects_to AS (:Generator) -[:has_connector]-> (c:Connector) <-[:has_connector]- (:Generator)
					  WHERE c.status = 'OPERATIONAL'
					PATH has_parent AS () -[:has_father|has_mother]-> (:Person)
					SELECT generatorA.location, generatorB.location
					  FROM MATCH (generatorA) -/:connects_to|has_parent+/-> (generatorB)`,
			check: func(node ast.Node) {
				stmt := node.(*ast.SelectStmt)
				assert.Equal(2, len(stmt.PathPatternMacros))
				expr := stmt.From.Matches[0].Paths[0].Connections[0].(*ast.ReachabilityPathExpr)
				assert.Equal(expr.Macros["connects_to"], stmt.PathPatternMacros[0].Path)
				assert.Equal(expr.Macros["has_parent"], stmt.PathPatternMacros[1].Path)
				assert.NotNil(stmt.Where)
			},
		},
		{
			query: `PATH connects_to AS (:Generator) -[:has_connector]-> (c:Connector) <-[:has_connector]- (:Generator)
					  WHERE c.status = 'OPERATIONAL'
					PATH has_parent AS () -[f:has_father|has_mother]-> (:Person)
					  WHERE f.age > 30
					SELECT generatorA.location, generatorB.location
					  FROM MATCH (generatorA) -/:connects_to|has_parent+/-> (generatorB)`,
			check: func(node ast.Node) {
				stmt := node.(*ast.SelectStmt)
				assert.Equal(2, len(stmt.PathPatternMacros))
				expr := stmt.From.Matches[0].Paths[0].Connections[0].(*ast.ReachabilityPathExpr)
				assert.Equal(expr.Macros["connects_to"], stmt.PathPatternMacros[0].Path)
				assert.Equal(expr.Macros["has_parent"], stmt.PathPatternMacros[1].Path)
				assert.NotNil(stmt.Where)
				logicalAnd := stmt.Where.(*ast.BinaryOperationExpr)
				assert.Equal(opcode.LogicAnd, logicalAnd.Op)
			},
		},
		{
			query: `PATH connects_to AS (:Generator) -[:has_connector]-> (c:Connector) <-[:has_connector]- (:Generator)
					  WHERE c.status = 'OPERATIONAL'
					PATH has_parent AS () -[f:has_father|has_mother]-> (:Person)
					  WHERE f.age > 30
					SELECT generatorA.location, generatorB.location
					  FROM MATCH (generatorA) -/:connects_to|has_parent+/-> (generatorB)
					 WHERE a > 10`,
			check: func(node ast.Node) {
				stmt := node.(*ast.SelectStmt)
				assert.Equal(2, len(stmt.PathPatternMacros))
				expr := stmt.From.Matches[0].Paths[0].Connections[0].(*ast.ReachabilityPathExpr)
				assert.Equal(expr.Macros["connects_to"], stmt.PathPatternMacros[0].Path)
				assert.Equal(expr.Macros["has_parent"], stmt.PathPatternMacros[1].Path)
				assert.NotNil(stmt.Where)
				logicalAnd := stmt.Where.(*ast.BinaryOperationExpr)
				assert.Equal(opcode.LogicAnd, logicalAnd.Op)
				logicalEq := logicalAnd.L.(*ast.BinaryOperationExpr)
				assert.Equal(opcode.GT, logicalEq.Op) //  a > 10
				logicalAnd = logicalAnd.R.(*ast.BinaryOperationExpr)
				assert.Equal(opcode.LogicAnd, logicalAnd.Op)
			},
		},
	}

	for _, c := range cases {
		parser := parser.New()
		stmt, err := parser.ParseOneStmt(c.query)
		assert.Nil(err)

		exp := NewMacroExpansion()
		n, ok := stmt.Accept(exp)
		assert.True(ok)
		c.check(n)
	}
}
