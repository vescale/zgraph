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

package planner

import (
	"fmt"

	"github.com/vescale/zgraph/expression"
	"github.com/vescale/zgraph/parser/model"
)

// ByItem wraps a "by" item.
type ByItem struct {
	Expr      expression.Expression
	AsName    model.CIStr
	Desc      bool
	NullOrder bool
}

// String implements fmt.Stringer interface.
func (by *ByItem) String() string {
	if by.Desc {
		return fmt.Sprintf("%s true", by.Expr)
	}
	return by.Expr.String()
}

// Clone makes a copy of ByItem.
func (by *ByItem) Clone() *ByItem {
	return &ByItem{Expr: by.Expr.Clone(), Desc: by.Desc}
}

type LogicalSort struct {
	logicalSchemaProducer

	ByItems []*ByItem
}

type PhysicalSort struct {
	physicalSchemaProducer

	ByItems []*ByItem
}
