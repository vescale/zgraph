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
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/parser/opcode"
)

// MacroExpansion is used to expand the PathPatternMacros.
//
//	 PATH has_parent AS () -[:has_father|has_mother]-> (:Person)
//	 SELECT ancestor.name
//		  FROM MATCH (p1:Person) -/:has_parent+/-> (ancestor)
//			 , MATCH (p2:Person) -/:has_parent+/-> (ancestor)
//		 WHERE p1.name = 'Mario'
//		   AND p2.name = 'Luigi'
//
// The MacroExpansion will replace the `has_parent` macro.
type MacroExpansion struct {
	macros  []*ast.PathPatternMacro
	mapping map[string]*ast.PathPatternMacro
	wheres  map[ast.ExprNode]struct{}
}

func NewMacroExpansion() *MacroExpansion {
	return &MacroExpansion{
		mapping: map[string]*ast.PathPatternMacro{},
		wheres:  map[ast.ExprNode]struct{}{},
	}
}

func (m *MacroExpansion) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch stmt := n.(type) {
	case *ast.InsertStmt:
		m.macros = stmt.PathPatternMacros
	case *ast.UpdateStmt:
		m.macros = stmt.PathPatternMacros
	case *ast.DeleteStmt:
		m.macros = stmt.PathPatternMacros
	case *ast.SelectStmt:
		m.macros = stmt.PathPatternMacros
	case *ast.MatchClauseList:
		return m.macroExpansion(n.(*ast.MatchClauseList))
	default:
		return n, true
	}

	// We skip its children if the statement doesn't have macro definitions.
	return n, len(m.macros) == 0
}

func (m *MacroExpansion) macroExpansion(matchList *ast.MatchClauseList) (node ast.Node, skipChildren bool) {
	if len(m.macros) != len(m.mapping) {
		for _, macro := range m.macros {
			m.mapping[macro.Name.L] = macro
		}
	}

	detected := map[int] /*matchIndex*/ map[int] /*pathIndex*/ []int{}
	for matchIndex, matchClause := range matchList.Matches {
		for pathIndex, path := range matchClause.Paths {
			for connIndex, conn := range path.Connections {
				// Ref: https://pgql-lang.org/spec/1.5/#path-pattern-macros
				// One or more “path pattern macros” may be declared at the beginning of the query.
				// These macros allow for expressing complex regular expressions. PGQL 1.5 allows
				// macros only for reachability, not for (top-k) shortest path.
				reachabilityPathExpr, ok := conn.(*ast.ReachabilityPathExpr)
				if !ok {
					continue
				}

				var found bool
				for _, label := range reachabilityPathExpr.Labels {
					_, ok = m.mapping[label.L]
					found = ok || found
				}
				if found {
					pathGroup, ok := detected[matchIndex]
					if !ok {
						pathGroup = map[int][]int{}
						detected[matchIndex] = pathGroup
					}
					pathGroup[pathIndex] = append(pathGroup[pathIndex], connIndex)
				}
			}
		}
	}

	if len(detected) == 0 {
		return matchList, true
	}

	// Shallow copy the match clause list.
	newMatchList := &ast.MatchClauseList{}
	*newMatchList = *matchList
	newMatchList.Matches = make([]*ast.MatchClause, 0, len(matchList.Matches))
	newMatchList.Matches = append(newMatchList.Matches, matchList.Matches...)

	for matchIndex, pathGroup := range detected {
		oldMatch := matchList.Matches[matchIndex]
		newMatch := &ast.MatchClause{}
		*newMatch = *oldMatch
		newMatch.Paths = make([]*ast.PathPattern, 0, len(oldMatch.Paths))
		newMatch.Paths = append(newMatch.Paths, oldMatch.Paths...)
		newMatchList.Matches[matchIndex] = newMatch
		for pathIndex, connGroup := range pathGroup {
			oldPath := oldMatch.Paths[pathIndex]
			newPath := &ast.PathPattern{}
			*newPath = *oldPath
			newPath.Connections = make([]ast.VertexPairConnection, 0, len(oldPath.Connections))
			newPath.Connections = append(newPath.Connections, oldPath.Connections...)
			newMatch.Paths[pathIndex] = newPath
			for _, connIndex := range connGroup {
				oldConn := oldPath.Connections[connIndex].(*ast.ReachabilityPathExpr)
				newConn := &ast.ReachabilityPathExpr{}
				*newConn = *oldConn
				newConn.Macros = map[string]*ast.PathPattern{}
				for _, label := range newConn.Labels {
					macro, found := m.mapping[label.L]
					if !found {
						continue
					}
					newConn.Macros[label.L] = macro.Path
					if macro.Where != nil {
						m.wheres[macro.Where] = struct{}{}
					}
				}
				newPath.Connections[connIndex] = newConn
			}
		}
	}

	return newMatchList, true
}

func (m *MacroExpansion) Leave(n ast.Node) (node ast.Node, ok bool) {
	if len(m.wheres) == 0 {
		return n, true
	}

	var cnf ast.ExprNode
	for expr := range m.wheres {
		if cnf == nil {
			cnf = expr
			continue
		}
		cnf = &ast.BinaryOperationExpr{
			Op: opcode.LogicAnd,
			L:  cnf,
			R:  expr,
		}
	}

	// Attach where expressions.
	switch stmt := n.(type) {
	case *ast.InsertStmt:
		newInsert := &ast.InsertStmt{}
		*newInsert = *stmt
		if newInsert.Where != nil {
			newInsert.Where = &ast.BinaryOperationExpr{
				Op: opcode.LogicAnd,
				L:  newInsert.Where,
				R:  cnf,
			}
		} else {
			newInsert.Where = cnf
		}
		n = newInsert
	case *ast.UpdateStmt:
		newUpdate := &ast.UpdateStmt{}
		*newUpdate = *stmt
		if newUpdate.Where != nil {
			newUpdate.Where = &ast.BinaryOperationExpr{
				Op: opcode.LogicAnd,
				L:  newUpdate.Where,
				R:  cnf,
			}
		} else {
			newUpdate.Where = cnf
		}
		n = newUpdate
	case *ast.DeleteStmt:
		newDelete := &ast.DeleteStmt{}
		*newDelete = *stmt
		if newDelete.Where != nil {
			newDelete.Where = &ast.BinaryOperationExpr{
				Op: opcode.LogicAnd,
				L:  newDelete.Where,
				R:  cnf,
			}
		} else {
			newDelete.Where = cnf
		}
		n = newDelete
	case *ast.SelectStmt:
		newSelect := &ast.SelectStmt{}
		*newSelect = *stmt
		if newSelect.Where != nil {
			newSelect.Where = &ast.BinaryOperationExpr{
				Op: opcode.LogicAnd,
				L:  newSelect.Where,
				R:  cnf,
			}
		} else {
			newSelect.Where = cnf
		}
		n = newSelect
	default:
		return n, true
	}

	return n, true
}
