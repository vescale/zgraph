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
	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/executor"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/planner"
	"github.com/vescale/zgraph/stmtctx"
)

// Compile compiles the statement AST node into an executable statement. The compiler relay
// on the statement context to retrieve some environment information and set some intermediate
// variables while compiling. The catalog is used to resolve names in the query.
func Compile(sc *stmtctx.Context, catalog *catalog.Catalog, node ast.StmtNode) (executor.Statement, error) {
	// Check the AST to ensure it is valid.
	prep := NewPreprocess(sc, catalog)
	node.Accept(prep)

	// Build plan tree from a valid AST.
	builder := NewBuilder(sc, catalog)
	plan, err := builder.Build(node)
	if err != nil {
		return nil, err
	}
	logicalPlan, isLogicalPlan := plan.(planner.LogicalPlan)
	if !isLogicalPlan {
		return executor.NewStatement(sc, catalog, plan), nil
	}

	// Optimize the logical plan and generate physical plan.
	optimized := planner.Optimize(logicalPlan)

	return executor.NewStatement(sc, catalog, optimized), nil
}
