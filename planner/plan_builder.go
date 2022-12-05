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
