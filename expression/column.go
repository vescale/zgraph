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

	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/types"
)

type Column struct {
	// ID is the unique id of this column.
	ID int64
	// Name is the name of this column.
	Name model.CIStr
	// Index is used for execution, to tell the column's position in the given row.
	Index int
	// If set, this column is used internally, and should not be exposed to the user.
	Hidden bool
}

func (c *Column) Clone() Expression {
	return &Column{
		ID:     c.ID,
		Name:   c.Name,
		Index:  c.Index,
		Hidden: c.Hidden,
	}
}

func (c *Column) String() string {
	return fmt.Sprintf("Column#%d", c.ID)
}

func (c *Column) Eval(_ *stmtctx.Context, row Row) (types.Datum, error) {
	return row[c.Index], nil
}
