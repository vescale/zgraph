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
	"bytes"
	"fmt"

	"github.com/vescale/zgraph/types"
)

// ScalarFunction represents a scalar function.
type ScalarFunction struct {
	funcName string
	function builtinFunc
}

func (sf *ScalarFunction) String() string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "%s(", sf.funcName)
	for i, arg := range sf.function.getArgs() {
		buffer.WriteString(arg.String())
		if i+1 != len(sf.function.getArgs()) {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// Clone implements Expression interface.
func (sf *ScalarFunction) Clone() Expression {
	panic("implement me")
}

func (sf *ScalarFunction) Eval(row Row) (types.Datum, error) {
	var d types.Datum

	// TODO: infer the type of the function and call the corresponding eval function.
	val, err := sf.EvalInt(row)
	if err != nil || val.IsNull() {
		d.SetNull()
		return d, err
	}
	d.SetInt64(val.Get())

	return d, nil
}

func (sf *ScalarFunction) EvalInt(row Row) (Nullable[int64], error) {
	return sf.function.evalInt(row)
}

func (sf *ScalarFunction) EvalReal(row Row) (Nullable[float64], error) {
	return sf.function.evalReal(row)
}

func (sf *ScalarFunction) EvalString(row Row) (Nullable[string], error) {
	return sf.function.evalString(row)
}

func (sf *ScalarFunction) EvalDecimal(row Row) (Nullable[*types.Decimal], error) {
	return sf.function.evalDecimal(row)
}

func NewFunction(funcName string, args ...Expression) (Expression, error) {
	fc, ok := funcs[funcName]
	if !ok {
		return nil, ErrFunctionNotExists
	}
	f, err := fc.getFunction(args)
	if err != nil {
		return nil, err
	}
	return &ScalarFunction{funcName: funcName, function: f}, nil
}
