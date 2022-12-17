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

	"github.com/vescale/zgraph/stmtctx"

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

func (sf *ScalarFunction) Eval(ctx *stmtctx.Context, row Row) (types.Datum, error) {
	return sf.function.eval(ctx, row)
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
