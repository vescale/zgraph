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

package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/expression"
	"github.com/vescale/zgraph/meta"
	"github.com/vescale/zgraph/parser/ast"
)

// SimpleExec is used to execute some simple tasks.
type SimpleExec struct {
	baseExecutor

	done      bool
	statement ast.StmtNode
}

func (e *SimpleExec) Next(_ context.Context) (expression.Row, error) {
	if e.done {
		return nil, nil
	}
	e.done = true

	switch stmt := e.statement.(type) {
	case *ast.UseStmt:
		return nil, e.execUse(stmt)
	default:
		return nil, errors.Errorf("unknown statement: %T", e.statement)
	}
}

func (e *SimpleExec) execUse(stmt *ast.UseStmt) error {
	graph := e.sc.Catalog().Graph(stmt.GraphName.L)
	if graph == nil {
		return meta.ErrGraphNotExists
	}

	e.sc.SetCurrentGraph(stmt.GraphName.L)

	return nil
}
