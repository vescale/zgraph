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
	"strings"

	"github.com/vescale/zgraph/datum"
	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/types"
)

var _ Expression = &FuncExpr{}

type FuncExpr struct {
	Name string
	Args []Expression
	Fn   Function
}

func (expr *FuncExpr) String() string {
	sb := &strings.Builder{}
	sb.WriteString(expr.Name)
	sb.WriteByte('(')
	sb.WriteString(expr.Args[0].String())
	for i := 1; i < len(expr.Args); i++ {
		sb.WriteString(", ")
		sb.WriteString(expr.Args[i].String())
	}
	sb.WriteByte(')')
	return sb.String()
}

func (expr *FuncExpr) ReturnType() types.T {
	argTypes := make([]types.T, 0, len(expr.Args))
	for _, arg := range expr.Args {
		argTypes = append(argTypes, arg.ReturnType())
	}
	return expr.Fn.InferReturnType(argTypes)
}

func (expr *FuncExpr) Eval(stmtCtx *stmtctx.Context, input datum.Row) (datum.Datum, error) {
	funcArgs, nullResult, err := expr.evalFuncArgs(stmtCtx, input)
	if err != nil {
		return nil, err
	}
	if nullResult {
		return datum.Null, nil
	}
	return expr.Fn.Eval(stmtCtx, funcArgs)
}

func (expr *FuncExpr) evalFuncArgs(stmtCtx *stmtctx.Context, input datum.Row) (_ []datum.Datum, propagateNulls bool, _ error) {
	var funcArgs []datum.Datum
	for _, arg := range expr.Args {
		d, err := arg.Eval(stmtCtx, input)
		if err != nil {
			return nil, false, err
		}
		if d == datum.Null && !expr.Fn.CallOnNullInput() {
			return nil, true, nil
		}
		funcArgs = append(funcArgs, d)
	}
	return funcArgs, false, nil
}

func NewFuncExpr(name string, args ...Expression) (*FuncExpr, error) {
	fn, ok := builtinFuncs[name]
	if !ok {
		return nil, fmt.Errorf("function %s not found", name)
	}
	if fn.NumArgs() != len(args) {
		return nil, fmt.Errorf("invalid arguments count to call function %s", name)
	}
	return &FuncExpr{
		Name: name,
		Args: args,
		Fn:   fn,
	}, nil
}

type Function interface {
	NumArgs() int
	InferReturnType(argTypes []types.T) types.T
	CallOnNullInput() bool
	Eval(stmtCtx *stmtctx.Context, args []datum.Datum) (datum.Datum, error)
}

var builtinFuncs = map[string]Function{
	"id": newBuiltIDFunc(),
}

type baseBuiltinFunc struct {
	numArgs         int
	callOnNullInput bool
}

func (b baseBuiltinFunc) NumArgs() int {
	return b.numArgs
}

func (b baseBuiltinFunc) CallOnNullInput() bool {
	return b.callOnNullInput
}

func newBaseBuiltinFunc(numArgs int, callOnNullInput bool) baseBuiltinFunc {
	return baseBuiltinFunc{numArgs: numArgs, callOnNullInput: callOnNullInput}
}

type builtinIDFunc struct {
	baseBuiltinFunc
}

func (b builtinIDFunc) InferReturnType(_ []types.T) types.T {
	return types.Int
}

func (b builtinIDFunc) Eval(_ *stmtctx.Context, args []datum.Datum) (datum.Datum, error) {
	switch x := args[0].(type) {
	case *datum.Vertex:
		return datum.NewInt(x.ID), nil
	case *datum.Edge:
		// TODO: Edge should have a unique id.
		return datum.NewInt(0), nil
	default:
		return nil, fmt.Errorf("cannot get id from data type %s", args[0].Type())
	}
}

func newBuiltIDFunc() Function {
	return builtinIDFunc{newBaseBuiltinFunc(1, false)}
}
