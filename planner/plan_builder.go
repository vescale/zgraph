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
	var graph *catalog.Graph
	if intoGraph == "" {
		graph = b.sc.CurrentGraph()
	} else {
		graph = b.sc.Catalog().Graph(intoGraph)
	}
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
				VariableRef: &expression.VariableRef{Name: prop.PropertyAccess.VariableName},
				PropertyRef: &expression.PropertyRef{Property: propInfo},
				Expr:        expr,
			}
			assignments = append(assignments, assignment)
		}
		var fromIDExpr, toIDExpr expression.Expression
		if insertion.InsertionType == ast.InsertionTypeEdge {
			// TODO: Add FromIDExpr and ToIDExpr.
		}
		gi := &ElementInsertion{
			Type:        insertion.InsertionType,
			Labels:      labels,
			Assignments: assignments,
			FromIDExpr:  fromIDExpr,
			ToIDExpr:    toIDExpr,
		}
		insertions = append(insertions, gi)
	}

	plan := &Insert{
		Graph:      graph,
		Insertions: insertions,
	}

	if stmt.From != nil {
		matchPlan, err := b.buildMatch(stmt.From.Matches)
		if err != nil {
			return err
		}
		plan.MatchPlan = Optimize(matchPlan)
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

	// TODO: support DISTINCT and wildcard.
	proj := &LogicalProjection{}
	for _, elem := range stmt.Select.Elements {
		// TODO: resolve reference.
		expr, err := RewriteExpr(elem.ExpAsVar.Expr)
		if err != nil {
			return err
		}
		proj.Exprs = append(proj.Exprs, expr)
	}
	proj.SetSchema(expression.NewSchema())
	proj.SetChildren(plan)

	b.setPlan(proj)
	return nil
}

func (b *Builder) buildMatch(matches []*ast.MatchClause) (LogicalPlan, error) {
	if len(matches) == 0 {
		return &LogicalDual{}, nil
	}

	sgb := NewSubgraphBuilder(b.sc.CurrentGraph())
	for _, match := range matches {
		sgb.AddPathPatterns(match.Paths...)
	}
	sg, err := sgb.Build()
	if err != nil {
		return nil, err
	}

	plan := &LogicalMatch{
		Subgraph: sg,
	}
	return plan, nil
}
