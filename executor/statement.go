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

	"github.com/vescale/zgraph/planner"
)

// Statement represents an executable statement.
type Statement interface {
	Execute(ctx context.Context) (RecordSet, error)
}

type executableStmt struct {
	plan planner.Plan
}

func NewStatement(plan planner.Plan) Statement {
	return &executableStmt{
		plan: plan,
	}
}

func (e *executableStmt) Execute(ctx context.Context) (RecordSet, error) {
	//TODO implement me
	panic("implement me")
}
