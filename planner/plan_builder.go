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
	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/expression"
	"github.com/vescale/zgraph/meta"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/stmtctx"
)

// builderContext represents the context of building plan.
type builderContext struct {
	plan Plan
}

// Builder is used to build the AST into a plan.
type Builder struct {
	sc     *stmtctx.Context
	stacks []*builderContext
}

// NewBuilder returns a plan builder.
func NewBuilder(sc *stmtctx.Context) *Builder {
	return &Builder{
		sc: sc,
	}
}

// Build builds a statement AST node into a Plan.
func (b *Builder) Build(node ast.StmtNode) (Plan, error) {
	b.pushContext()
	defer b.popContext()

	var err error
	switch stmt := node.(type) {
	case ast.DDLNode:
		err = b.buildDDL(stmt)
	case *ast.UseStmt:
		err = b.buildSimple(stmt)
	case *ast.InsertStmt:
		err = b.buildInsert(stmt)
	case *ast.SelectStmt:
		err = b.buildSelect(stmt)
	}
	if err != nil {
		return nil, err
	}

	return b.plan(), nil
}

func (b *Builder) pushContext() {
	b.stacks = append(b.stacks, &builderContext{})
}

func (b *Builder) popContext() {
	b.stacks = b.stacks[:len(b.stacks)-1]
}

func (b *Builder) plan() Plan {
	return b.stacks[len(b.stacks)-1].plan
}

func (b *Builder) setPlan(plan Plan) {
	b.stacks[len(b.stacks)-1].plan = plan
}

func (b *Builder) buildDDL(ddl ast.DDLNode) error {
	b.setPlan(&DDL{
		Statement: ddl,
	})
	return nil
}

func (b *Builder) buildSimple(stmt ast.StmtNode) error {
	b.setPlan(&Simple{
		Statement: stmt,
	})
	return nil
}

func (b *Builder) buildInsert(stmt *ast.InsertStmt) error {
	var intoGraph string
	if !stmt.IntoGraphName.IsEmpty() {
		intoGraph = stmt.IntoGraphName.L
	}
	if intoGraph == "" {
		intoGraph = b.sc.CurrentGraph()
	}
	graph := b.sc.Catalog().Graph(intoGraph)
	if graph == nil {
		return errors.Annotatef(meta.ErrGraphNotExists, "graph %s", intoGraph)
	}

	var insertions []*ElementInsertion
	for _, insertion := range stmt.Insertions {
		var labels []*catalog.Label
		for _, lbl := range insertion.LabelsAndProperties.Labels {
			label := graph.Label(lbl.L)
			if label == nil {
				return errors.Annotatef(meta.ErrLabelNotExists, "label %s", lbl.L)
			}
			labels = append(labels, label)
		}
		var assignments []*expression.Assignment
		for _, prop := range insertion.LabelsAndProperties.Assignments {
			// Note: The property suppose to be exists because we have invoked property
			// creation module before building plan.
			propInfo := graph.Property(prop.PropertyAccess.PropertyName.L)
			// Please fix bug in PropertyPreparation if the propInfo empty.
			if propInfo == nil {
				return errors.Errorf("property %s not exists", prop.PropertyAccess.PropertyName.L)
			}
			expr, err := RewriteExpr(prop.ValueExpression)
			if err != nil {
				return err
			}
			assignment := &expression.Assignment{
				VarReference: &expression.VariableRef{Name: prop.PropertyAccess.VariableName},
				PropertyRef:  &expression.PropertyRef{Property: propInfo},
				Expr:         expr,
			}
			assignments = append(assignments, assignment)
		}
		var variable *expression.VariableRef
		if !insertion.VariableName.IsEmpty() {
			variable = &expression.VariableRef{
				Name: insertion.VariableName,
			}
		}
		var fromRef, toRef *expression.VariableRef
		if insertion.InsertionType == ast.InsertionTypeEdge {
			fromRef = &expression.VariableRef{
				Name: insertion.From,
			}
			toRef = &expression.VariableRef{
				Name: insertion.To,
			}
		}
		gi := &ElementInsertion{
			Type:             insertion.InsertionType,
			Labels:           labels,
			Assignments:      assignments,
			ElementReference: variable,
			FromReference:    fromRef,
			ToReference:      toRef,
		}
		insertions = append(insertions, gi)
	}

	b.setPlan(&Insert{
		Graph:      graph,
		Insertions: insertions,
	})
	return nil
}

func (b *Builder) buildSelect(stmt *ast.SelectStmt) error {
	// Build source
	plan, err := b.buildMatch(stmt.From.Matches)
	if err != nil {
		return err
	}

	// Build selection
	if stmt.Where != nil {
		expr, err := RewriteExpr(stmt.Where)
		if err != nil {
			return err
		}
		where := &LogicalSelection{
			Condition: expr,
		}
		where.SetChildren(plan)
		plan = where
	}

	// TODO: support GROUP BY clause.
	// Explicit GROUP BY: SELECT * FROM MATCH (n) GROUP BY n.name;
	// Implicit GROUP BY: SELECT COUNT(*) FROM MATCH (n);
	if stmt.GroupBy != nil {

	}

	if stmt.Having != nil {
		expr, err := RewriteExpr(stmt.Having.Expr)
		if err != nil {
			return err
		}
		having := &LogicalSelection{
			Condition: expr,
		}
		having.SetChildren(plan)
		plan = having
	}

	if stmt.OrderBy != nil {
		byItems := make([]*ByItem, 0, len(stmt.OrderBy.Items))
		for _, item := range stmt.OrderBy.Items {
			expr, err := RewriteExpr(item.Expr.Expr)
			if err != nil {
				return err
			}
			byItems = append(byItems, &ByItem{
				Expr:      expr,
				AsName:    item.Expr.AsName,
				Desc:      item.Desc,
				NullOrder: item.NullOrder,
			})
		}
		orderby := &LogicalSort{
			ByItems: byItems,
		}
		orderby.SetChildren(plan)
		plan = orderby
	}

	if stmt.Limit != nil {
		offset, err := RewriteExpr(stmt.Limit.Offset)
		if err != nil {
			return err
		}
		count, err := RewriteExpr(stmt.Limit.Count)
		if err != nil {
			return err
		}
		limit := &LogicalLimit{
			Offset: offset,
			Count:  count,
		}
		limit.SetChildren(plan)
		plan = limit
	}

	b.setPlan(plan)
	return nil
}

func (b *Builder) buildMatch(matches []*ast.MatchClause) (LogicalPlan, error) {
	if len(matches) == 0 {
		return &LogicalDual{}, nil
	}

	// NOTE: Only support `SELECT x.name FROM MATCH (x)` for now.
	var subgraphs []*Subgraph
	for _, m := range matches {
		graphName := m.Graph.L
		if graphName == "" {
			graphName = b.sc.CurrentGraph()
		}
		graph := b.sc.Catalog().Graph(graphName)
		if graph == nil {
			return nil, errors.Annotatef(meta.ErrGraphNotExists, "graph %s", graphName)
		}
		var paths []*PathPattern
		for _, p := range m.Paths {
			var vertices []*VertexRef
			for _, v := range p.Vertices {
				var labels []*catalog.Label
				for _, l := range v.Variable.Labels {
					label := graph.Label(l.L)
					if label == nil {
						return nil, errors.Annotatef(meta.ErrLabelNotExists, "label %s", l.L)
					}
					labels = append(labels, label)
				}
				vertexRef := &VertexRef{
					Name:   v.Variable.Name,
					Labels: labels,
				}
				vertices = append(vertices, vertexRef)
			}
			path := &PathPattern{
				Tp:       p.Tp,
				TopK:     p.TopK,
				Vertices: vertices,
				// TODO: connections
			}
			paths = append(paths, path)
		}
		subgraph := &Subgraph{
			Graph: graph,
			Paths: paths,
		}
		subgraphs = append(subgraphs, subgraph)
	}

	plan := &LogicalMatch{
		Subgraphs: subgraphs,
	}

	return plan, nil
}
