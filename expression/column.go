// Copyright 2022 zGraph Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"

	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/types"
)

type Column struct {
	// ID is the unique id of this column.
	ID int64
	// Index is used for execution, to tell the column's position in the given row.
	Index int
}

func (c *Column) Clone() Expression {
	return &Column{
		ID:    c.ID,
		Index: c.Index,
	}
}

func (c *Column) String() string {
	return fmt.Sprintf("Column#%d", c.ID)
}

func (c *Column) Eval(_ *stmtctx.Context, row Row) (types.Datum, error) {
	return row[c.Index], nil
}
