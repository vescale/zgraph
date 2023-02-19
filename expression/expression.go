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
	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/types"
)

type Expression interface {
	fmt.Stringer
	ReturnType() types.T
	Eval(stmtCtx *stmtctx.Context, input datum.Row) (datum.Datum, error)
}

// Assignment represents an assignment in INSERT/UPDATE statements.
//
// e.g:
// INSERT VERTEX x LABELS ( Male ) PROPERTIES ( x.age = 22 )
// UPDATE x SET ( x.age = 42 ) FROM MATCH (x:Person) WHERE x.name = 'John'
type Assignment struct {
	VariableRef *VariableRef
	PropertyRef *PropertyRef
	Expr        Expression
}

// VariableRef represents a variable referenced by other scope.
//
// e.g:
// INSERT VERTEX x LABELS ( Male ) PROPERTIES ( x.age = 22 )
// --------------^------------------------------^----------
type VariableRef struct {
	Name model.CIStr
}

func (v *VariableRef) String() string {
	return v.Name.O
}

// PropertyRef represents the accessor of vertex/edge's property.
type PropertyRef struct {
	Property *model.PropertyInfo
}

func (f *PropertyRef) Clone() *PropertyRef {
	fc := *f
	return &fc
}

// String implements the fmt.Stringer interface
func (f *PropertyRef) String() string {
	return f.Property.Name.O
}
