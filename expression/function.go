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
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/types"
	"golang.org/x/exp/slices"
)

var _ Expression = &FuncExpr{}

type FuncExpr struct {
	FnName      string
	Exprs       []Expression
	overloadSet *funcOverloadSet
}

func (expr *FuncExpr) String() string {
	argStrs := make([]string, 0, len(expr.Exprs))
	for _, expr := range expr.Exprs {
		argStrs = append(argStrs, expr.String())
	}
	return fmt.Sprintf("%s(%s)", strings.ToUpper(expr.FnName), strings.Join(argStrs, ", "))
}

func (expr *FuncExpr) ReturnType() types.T {
	var argTypes []types.T
	for _, argExpr := range expr.Exprs {
		argTypes = append(argTypes, argExpr.ReturnType())
	}
	return expr.overloadSet.inferReturnType(argTypes)
}

func (expr *FuncExpr) Eval(evalCtx *EvalContext) (datum.Datum, error) {
	nullResult, args, err := expr.evalFuncArgs(evalCtx)
	if err != nil {
		return nil, err
	}
	if nullResult {
		return datum.Null, nil
	}
	var argTypes []types.T
	for _, arg := range args {
		argTypes = append(argTypes, arg.Type())
	}
	overload, ok := expr.overloadSet.lookupImpl(argTypes)
	if !ok {
		return datum.Null, nil
	}
	return overload.eval(evalCtx, args)
}

func (expr *FuncExpr) evalFuncArgs(evalCtx *EvalContext) (propagateNulls bool, args datum.Datums, _ error) {
	args = make(datum.Datums, 0, len(expr.Exprs))
	for _, expr := range expr.Exprs {
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
	overloadSet, ok := builtinFuncs[fnName]
	if !ok {
		return nil, fmt.Errorf("function %s not found", fnName)
	}
	if overloadSet.argLen != len(exprs) {
		return nil, fmt.Errorf("function %s expects %d arguments, but got %d", fnName, overloadSet.argLen, len(exprs))
	}

	return &FuncExpr{
		FnName:      fnName,
		Exprs:       exprs,
		overloadSet: overloadSet,
	}, nil
}

type funcOverload struct {
	argTypes   []types.T
	returnType types.T
	eval       func(*EvalContext, datum.Datums) (datum.Datum, error)
}

type funcOverloadSet struct {
	argLen     int
	overloads  []*funcOverload
	returnType types.T
}

func (s *funcOverloadSet) lookupImpl(argTypes []types.T) (*funcOverload, bool) {
	for _, overload := range s.overloads {
		if slices.Equal(overload.argTypes, argTypes) {
			return overload, true
		}
	}
	return nil, false
}

func (s *funcOverloadSet) inferReturnType(argTypes []types.T) types.T {
	if s.returnType != types.Unknown {
		return s.returnType
	}
	for _, overload := range s.overloads {
		if slices.Equal(overload.argTypes, argTypes) {
			return overload.returnType
		}
	}
	return types.Unknown
}

var builtinFuncs = map[string]*funcOverloadSet{
	ast.ID: {
		argLen:     1,
		returnType: types.Int,
		overloads: []*funcOverload{
			{
				argTypes:   []types.T{types.Vertex},
				returnType: types.Int,
				eval: func(evalCtx *EvalContext, args datum.Datums) (datum.Datum, error) {
					v := datum.MustBeVertex(args[0])
					return datum.NewInt(v.ID), nil
				},
			},
			{
				argTypes:   []types.T{types.Edge},
				returnType: types.Int,
				eval: func(evalCtx *EvalContext, args datum.Datums) (datum.Datum, error) {
					return datum.NewInt(0), nil
				},
			},
		},
	},
}
