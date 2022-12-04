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

	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/planner"
	"github.com/vescale/zgraph/stmtctx"
)

// Statement represents an executable statement.
type Statement interface {
	Execute(ctx context.Context) (RecordSet, error)
}

type executableStmt struct {
	sc      *stmtctx.Context
	catalog *catalog.Catalog
	plan    planner.Plan
}

// NewStatement returns an executable statement.
func NewStatement(sc *stmtctx.Context, catalog *catalog.Catalog, plan planner.Plan) Statement {
	return &executableStmt{
		sc:      sc,
		catalog: catalog,
		plan:    plan,
	}
}

// Execute executes the statement and returns a record set.
func (e *executableStmt) Execute(ctx context.Context) (RecordSet, error) {
	builder := NewBuilder(e.sc, e.catalog)
	exec := builder.Build(e.plan)
	err := builder.Error()
	if err != nil {
		return nil, err
	}
	
	err = exec.Open(ctx)
	if err != nil {
		return nil, err
	}

	return &execRecordSet{exec: exec}, nil
}
