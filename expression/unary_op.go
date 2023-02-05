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
	"github.com/vescale/zgraph/parser/opcode"
	"github.com/vescale/zgraph/types"
)

var _ Expression = &UnaryExpr{}

type UnaryExpr struct {
	Op          opcode.Op
	Expr        Expression
	overloadSet *unaryOpOverloadSet
}

func NewUnaryExpr(op opcode.Op, expr Expression) (*UnaryExpr, error) {
	overloadSet, ok := unaryOps[op]
	if !ok {
		return nil, fmt.Errorf("unsupported unary operator: %s", op)
	}
	return &UnaryExpr{
		Op:          op,
		Expr:        expr,
		overloadSet: overloadSet,
	}, nil
}

func (u *UnaryExpr) String() string {
	return fmt.Sprintf("%s %s", u.Op, u.Expr)
}

func (u *UnaryExpr) ReturnType() types.T {
	op, ok := u.overloadSet.lookupImpl(u.Expr.ReturnType())
	if ok {
		return op.returnType
	}
	return types.Unknown
}

func (u *UnaryExpr) Eval(evalCtx *EvalContext) (datum.Datum, error) {
	d, err := u.Expr.Eval(evalCtx)
	if err != nil || d == datum.Null {
		return d, err
	}
	op, ok := u.overloadSet.lookupImpl(u.Expr.ReturnType())
	if !ok {
		return datum.Null, nil
	}
	return op.fn(evalCtx, d)
}

type unaryOp struct {
	typ        types.T
	returnType types.T
	fn         func(*EvalContext, datum.Datum) (datum.Datum, error)
}

type unaryOpOverloadSet struct {
	overloads map[types.T]*unaryOp
}

func (o *unaryOpOverloadSet) lookupImpl(typ types.T) (*unaryOp, bool) {
	op, ok := o.overloads[typ]
	return op, ok
}

var unaryOps = map[opcode.Op]*unaryOpOverloadSet{
	opcode.Minus: {
		overloads: map[types.T]*unaryOp{
			types.Int: {
				typ:        types.Int,
				returnType: types.Int,
				fn: func(_ *EvalContext, d datum.Datum) (datum.Datum, error) {
					i := datum.MustBeInt(d)
					return datum.NewInt(-int64(i)), nil
				},
			},
			types.Float: {
				typ:        types.Float,
				returnType: types.Float,
				fn: func(_ *EvalContext, d datum.Datum) (datum.Datum, error) {
					f := datum.MustBeFloat(d)
					return datum.NewFloat(-float64(f)), nil
				},
			},
			types.Decimal: {
				typ:        types.Decimal,
				returnType: types.Decimal,
				fn: func(_ *EvalContext, d datum.Datum) (datum.Datum, error) {
					dec := datum.MustBeDecimal(d)
					res := &datum.Decimal{}
					res.Neg(&dec.Decimal)
					return res, nil
				},
			},
		},
	},
}
