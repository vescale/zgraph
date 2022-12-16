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

// Row represents a row of data.
type Row []types.Datum

// Nullable represents a nullable value.
type Nullable[T any] struct {
	value *T
}

// IsNull returns true if the value is null.
func (n *Nullable[T]) IsNull() bool {
	return n.value == nil
}

// Get returns the value. It panics if the value is null.
func (n *Nullable[T]) Get() T {
	return *n.value
}

// Set sets the value.
func (n *Nullable[T]) Set(value T) {
	n.value = &value
}

type Expression interface {
	fmt.Stringer

	// Clone deeply clones an expression.
	Clone() Expression
	// Eval evaluates an expression through a row.
	Eval(row Row) (types.Datum, error)
	// EvalInt returns the int64 representation of the expression.
	EvalInt(row Row) (Nullable[int64], error)
	// EvalReal returns the float64 representation of the expression.
	EvalReal(row Row) (Nullable[float64], error)
	// EvalString returns the string representation of the expression.
	EvalString(row Row) (Nullable[string], error)
	// EvalDecimal returns the decimal representation of the expression.
	EvalDecimal(row Row) (Nullable[*types.Decimal], error)
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
