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

package expression

import (
	"fmt"

	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/types"
)

type Row []types.Datum

type Expression interface {
	fmt.Stringer

	// GetType gets the type that the expression returns.
	GetType() types.FieldType

	// Clone deeply clones an expression.
	Clone() Expression

	Eval(row Row) (types.Datum, error)
}

// Assignment represents an assignment in INSERT/UPDATE statements.
//
// e.g:
// INSERT VERTEX x LABELS ( Male ) PROPERTIES ( x.age = 22 )
// UPDATE x SET ( x.age = 42 ) FROM MATCH (x:Person) WHERE x.name = 'John'
type Assignment struct {
	VarReference *VariableRef
	PropertyRef  *PropertyRef
	Expr         Expression
}

// VariableRef represents a variable referenced by other scope.
//
// e.g:
// INSERT VERTEX x LABELS ( Male ) PROPERTIES ( x.age = 22 )
// --------------^------------------------------^----------
type VariableRef struct {
	Name model.CIStr
}
