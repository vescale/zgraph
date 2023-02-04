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
	"github.com/vescale/zgraph/types"
)

var _ Expression = &FuncExpr{}

type FuncExpr struct {
	FnName   string
	Exprs    []Expression
	Overload *FnOverload
}

func (f *FuncExpr) String() string {
	argStrs := make([]string, 0, len(f.Exprs))
	for _, expr := range f.Exprs {
		argStrs = append(argStrs, expr.String())
	}
	return fmt.Sprintf("%s(%s)", strings.ToUpper(f.FnName), strings.Join(argStrs, ", "))
}

func (f *FuncExpr) ReturnType() types.T {
	return f.Overload.ReturnType
}

func (f *FuncExpr) Eval(evalCtx *EvalContext) (datum.Datum, error) {
	nullResult, args, err := f.evalFuncArgs(evalCtx)
	if err != nil {
		return nil, err
	}
	if nullResult {
		return datum.Null, nil
	}
	return f.Overload.Fn(evalCtx, args)
}

func (f *FuncExpr) evalFuncArgs(evalCtx *EvalContext) (propagateNulls bool, args datum.Datums, _ error) {
	args = make(datum.Datums, 0, len(f.Exprs))
	for _, expr := range f.Exprs {
		arg, err := expr.Eval(evalCtx)
		if err != nil {
			return false, nil, err
		}
		// TODO: If a function is expected to call with a NULL argument,
		//  we shouldn't propagate NULL to the result.
		if arg == datum.Null {
			return true, nil, nil
		}
		args = append(args, arg)
	}
	return false, args, nil
}

func NewFuncExpr(fnName string, exprs ...Expression) (*FuncExpr, error) {
	overload, ok := BuiltinFnOverloads[fnName]
	if !ok {
		return nil, fmt.Errorf("function %s not found", fnName)
	}
	if len(overload.ArgTypes) != len(exprs) {
		return nil, fmt.Errorf("function %s expects %d arguments, but got %d", fnName, len(overload.ArgTypes), len(exprs))
	}

	var typedExprs []Expression
	for i, expr := range exprs {
		if expr.ReturnType() != overload.ArgTypes[i] {
			typedExprs = append(typedExprs, NewCastExpr(expr, overload.ArgTypes[i]))
		} else {
			typedExprs = append(typedExprs, expr)
		}
	}

	return &FuncExpr{
		FnName:   fnName,
		Exprs:    typedExprs,
		Overload: overload,
	}, nil
}

type FnOverload struct {
	ArgTypes   []types.T
	ReturnType types.T
	Fn         func(*EvalContext, datum.Datums) (datum.Datum, error)
}

var BuiltinFnOverloads = map[string]*FnOverload{
	"id": {
		ArgTypes:   []types.T{types.Vertex},
		ReturnType: types.Int,
		Fn: func(evalCtx *EvalContext, args datum.Datums) (datum.Datum, error) {
			v := datum.MustBeVertex(args[0])
			return datum.NewInt(v.ID), nil
		},
	},
}
