// Copyright 2023 zGraph Authors. All rights reserved.
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

package expression

import (
	"fmt"

	"github.com/vescale/zgraph/datum"
	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/types"
)

var _ Expression = &Column{}

type Column struct {
	Index int
	Name  model.CIStr
	Type  types.T
}

func (c *Column) String() string {
	return c.Name.O
}

func (c *Column) ReturnType() types.T {
	return c.Type
}

func (c *Column) Eval(evalCtx *EvalContext) (datum.Datum, error) {
	if c.Index >= len(evalCtx.CurRow) {
		return nil, fmt.Errorf("column index %d out of evalCtx.CurRow length %d", c.Index, len(evalCtx.CurRow))
	}
	return evalCtx.CurRow[c.Index], nil
}
