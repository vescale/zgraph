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
	Op        opcode.Op
	Expr      Expression
	Overloads *UnaryOpOverloads
}

func NewUnaryExpr(op opcode.Op, expr Expression) *UnaryExpr {
	overloads, ok := UnaryOps[op]
	if !ok {
		panic(fmt.Sprintf("no overloads for unary operator %s", op))
	}
	return &UnaryExpr{
		Op:        op,
		Expr:      expr,
		Overloads: overloads,
	}
}

func (u *UnaryExpr) String() string {
	return fmt.Sprintf("%s %s", u.Op, u.Expr)
}

func (u *UnaryExpr) ReturnType() types.T {
	unaryOp, ok := u.Overloads.LookupImpl(u.Expr.ReturnType())
	if ok {
		return unaryOp.ReturnType
	}
	return types.Unknown
}

func (u *UnaryExpr) Eval(evalCtx *EvalContext) (datum.Datum, error) {
	d, err := u.Expr.Eval(evalCtx)
	if err != nil || d == datum.Null {
		return d, err
	}
	unaryOp, ok := u.Overloads.LookupImpl(u.Expr.ReturnType())
	if !ok {
		return datum.Null, nil
	}
	return unaryOp.Fn(evalCtx, d)
}

type UnaryOp struct {
	Type       types.T
	ReturnType types.T
	Fn         func(*EvalContext, datum.Datum) (datum.Datum, error)
}

type UnaryOpOverloads struct {
	overloads map[types.T]*UnaryOp
}

func (o *UnaryOpOverloads) LookupImpl(typ types.T) (*UnaryOp, bool) {
	unaryOp, ok := o.overloads[typ]
	return unaryOp, ok
}

var UnaryOps = map[opcode.Op]*UnaryOpOverloads{
	opcode.Minus: {
		overloads: map[types.T]*UnaryOp{
			types.Int: {
				Type:       types.Int,
				ReturnType: types.Int,
				Fn: func(_ *EvalContext, d datum.Datum) (datum.Datum, error) {
					i := datum.MustBeInt(d)
					return datum.NewInt(-int64(i)), nil
				},
			},
			types.Float: {
				Type:       types.Float,
				ReturnType: types.Float,
				Fn: func(_ *EvalContext, d datum.Datum) (datum.Datum, error) {
					f := datum.MustBeFloat(d)
					return datum.NewFloat(-float64(f)), nil
				},
			},
			types.Decimal: {
				Type:       types.Decimal,
				ReturnType: types.Decimal,
				Fn: func(_ *EvalContext, d datum.Datum) (datum.Datum, error) {
					dec := datum.MustBeDecimal(d)
					res := &datum.Decimal{}
					res.Neg(&dec.Decimal)
					return res, nil
				},
			},
		},
	},
}
