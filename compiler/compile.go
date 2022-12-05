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
	"github.com/vescale/zgraph/executor"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/planner"
	"github.com/vescale/zgraph/stmtctx"
)

// Compile compiles the statement AST node into an executable statement. The compiler relay
// on the statement context to retrieve some environment information and set some intermediate
// variables while compiling. The catalog is used to resolve names in the query.
func Compile(sc *stmtctx.Context, node ast.StmtNode) (executor.Executor, error) {
	// Check the AST to ensure it is valid.
	prep := NewPreprocess(sc)
	node.Accept(prep)

	// Build plan tree from a valid AST.
	planBuilder := planner.NewBuilder(sc)
	plan, err := planBuilder.Build(node)
	if err != nil {
		return nil, err
	}
	logicalPlan, isLogicalPlan := plan.(planner.LogicalPlan)
	if isLogicalPlan {
		// Optimize the logical plan and generate physical plan.
		plan = planner.Optimize(logicalPlan)
	}

	execBuilder := executor.NewBuilder(sc)
	exec := execBuilder.Build(plan)
	err = execBuilder.Error()
	if err != nil {
		return nil, err
	}

	return exec, nil
}
