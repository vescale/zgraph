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

	var insertions []*GraphInsertion
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
			assignment := &expression.Assignment{
				VarReference: &expression.Variable{Name: prop.PropertyAccess.VariableName},
				Property:     &expression.Property{Property: propInfo},
				Expr:         prop.ValueExpression,
			}
			assignments = append(assignments, assignment)
		}
		var variable *expression.Variable
		if !insertion.VariableName.IsEmpty() {
			variable = &expression.Variable{
				Name: insertion.VariableName,
			}
		}
		var fromRef, toRef *expression.Variable
		if insertion.InsertionType == ast.InsertionTypeEdge {
			fromRef = &expression.Variable{
				Name: insertion.From,
			}
			toRef = &expression.Variable{
				Name: insertion.To,
			}
		}
		gi := &GraphInsertion{
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
