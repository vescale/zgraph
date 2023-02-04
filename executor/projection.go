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

	"github.com/vescale/zgraph/datum"
	"github.com/vescale/zgraph/expression"
)

// ProjectionExec represents a projection executor.
type ProjectionExec struct {
	baseExecutor

	exprs []expression.Expression
}

func (p *ProjectionExec) Next(ctx context.Context) (datum.Datums, error) {
	childRow, err := p.children[0].Next(ctx)
	if err != nil || childRow == nil {
		return nil, err
	}

	evalCtx := expression.NewEvalContext(p.sc)

	result := make(datum.Datums, len(p.exprs))
	for i, expr := range p.exprs {
		d, err := evalCtx.EvalExprWithCurRow(expr, childRow)
		if err != nil {
			return nil, err
		}
		result[i] = d
	}
	return result, nil
}
