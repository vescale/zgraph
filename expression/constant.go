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
	"github.com/vescale/zgraph/datum"
	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/types"
)

var _ Expression = &Constant{}

// Constant represents a literal constant.
type Constant struct {
	Value datum.Datum
}

// String implements the fmt.Stringer interface.
func (c *Constant) String() string {
	return c.Value.String()
}

func (c *Constant) ReturnType() types.T {
	return c.Value.Type()
}

func (c *Constant) Eval(stmtCtx *stmtctx.Context, input datum.Row) (datum.Datum, error) {
	return c.Value, nil
}
