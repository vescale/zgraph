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

	"github.com/cockroachdb/apd/v3"
	"github.com/vescale/zgraph/datum"
	"github.com/vescale/zgraph/parser/opcode"
	"github.com/vescale/zgraph/types"
)

var _ Expression = &BinaryExpr{}

type BinaryExpr struct {
	Op        opcode.Op
	Left      Expression
	Right     Expression
	Overloads *BinOpOverloads
}

func NewBinaryExpr(op opcode.Op, left, right Expression) (*BinaryExpr, error) {
	overloads, ok := BinOps[op]
	if !ok {
		return nil, fmt.Errorf("no overloads for binary operator %s", op)
	}
	return &BinaryExpr{
		Op:        op,
		Left:      left,
		Right:     right,
		Overloads: overloads,
	}, nil
}

func (b *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", b.Left, b.Op, b.Right)
}

func (b *BinaryExpr) ReturnType() types.T {
	leftType := b.Left.ReturnType()
	rightType := b.Right.ReturnType()
	binOp, ok := b.Overloads.LookupImpl(leftType, rightType)
	if ok {
		return binOp.ReturnType
	}
	return types.Unknown
}

func (b *BinaryExpr) Eval(evalCtx *EvalContext) (datum.Datum, error) {
	left, err := b.Left.Eval(evalCtx)
	if err != nil || left == datum.Null {
		return left, err
	}
	right, err := b.Right.Eval(evalCtx)
	if err != nil || right == datum.Null {
		return right, err
	}
	binOp, ok := b.Overloads.LookupImpl(left.Type(), right.Type())
	if !ok {
		return datum.Null, nil
	}
	return binOp.Fn(evalCtx, left, right)
}

type BinOp struct {
	BinOpArgTypes
	ReturnType types.T
	Fn         func(*EvalContext, datum.Datum, datum.Datum) (datum.Datum, error)
}

type BinOpArgTypes struct {
	LeftType  types.T
	RightType types.T
}

type BinOpOverloads struct {
	overloads map[BinOpArgTypes]*BinOp
}

func (o *BinOpOverloads) LookupImpl(leftType, rightType types.T) (*BinOp, bool) {
	binOp, ok := o.overloads[BinOpArgTypes{LeftType: leftType, RightType: rightType}]
	return binOp, ok
}

var BinOps = map[opcode.Op]*BinOpOverloads{
	opcode.Plus: {
		overloads: map[BinOpArgTypes]*BinOp{
			{LeftType: types.Int, RightType: types.Int}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Int, RightType: types.Int},
				ReturnType:    types.Int,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					l := datum.MustBeInt(left)
					r := datum.MustBeInt(right)
					// TODO: Check for overflow.
					return datum.NewInt(int64(l) + int64(r)), nil
				},
			},
			{LeftType: types.Float, RightType: types.Float}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Float, RightType: types.Float},
				ReturnType:    types.Float,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					l := datum.MustBeFloat(left)
					r := datum.MustBeFloat(right)
					return datum.NewFloat(float64(l) + float64(r)), nil
				},
			},
			{LeftType: types.Decimal, RightType: types.Decimal}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Decimal, RightType: types.Decimal},
				ReturnType:    types.Decimal,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					l := datum.MustBeDecimal(left).Decimal
					r := datum.MustBeDecimal(right).Decimal
					d := &datum.Decimal{}
					_, err := apd.BaseContext.Add(&d.Decimal, &l, &r)
					if err != nil {
						return nil, err
					}
					return d, nil
				},
			},
			{LeftType: types.Int, RightType: types.Float}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Int, RightType: types.Float},
				ReturnType:    types.Float,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					panic("unimplemented")
				},
			},
			{LeftType: types.Float, RightType: types.Int}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Float, RightType: types.Int},
				ReturnType:    types.Float,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					panic("unimplemented")
				},
			},
			{LeftType: types.Int, RightType: types.Decimal}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Int, RightType: types.Decimal},
				ReturnType:    types.Decimal,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					panic("unimplemented")
				},
			},
			{LeftType: types.Decimal, RightType: types.Int}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Decimal, RightType: types.Int},
				ReturnType:    types.Decimal,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					panic("unimplemented")
				},
			},
			{LeftType: types.Float, RightType: types.Decimal}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Float, RightType: types.Decimal},
				ReturnType:    types.Decimal,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					panic("unimplemented")
				},
			},
			{LeftType: types.Decimal, RightType: types.Float}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Decimal, RightType: types.Float},
				ReturnType:    types.Decimal,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					panic("unimplemented")
				},
			},
			{LeftType: types.Date, RightType: types.Interval}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Date, RightType: types.Interval},
				ReturnType:    types.Date,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					panic("unimplemented")
				},
			},
			{LeftType: types.Time, RightType: types.Interval}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Time, RightType: types.Interval},
				ReturnType:    types.Time,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					panic("unimplemented")
				},
			},
			{LeftType: types.TimeTZ, RightType: types.Interval}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.TimeTZ, RightType: types.Interval},
				ReturnType:    types.TimeTZ,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					panic("unimplemented")
				},
			},
			{LeftType: types.Timestamp, RightType: types.Interval}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Timestamp, RightType: types.Interval},
				ReturnType:    types.Timestamp,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					panic("unimplemented")
				},
			},
			{LeftType: types.TimestampTZ, RightType: types.Interval}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.TimestampTZ, RightType: types.Interval},
				ReturnType:    types.TimestampTZ,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					panic("unimplemented")
				},
			},
		},
	},
	opcode.EQ: {
		overloads: map[BinOpArgTypes]*BinOp{
			{LeftType: types.Bool, RightType: types.Bool}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Bool, RightType: types.Bool},
				ReturnType:    types.Bool,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					l := datum.MustBeBool(left)
					r := datum.MustBeBool(right)
					return datum.NewBool(l == r), nil
				},
			},
			{LeftType: types.Int, RightType: types.Int}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Int, RightType: types.Int},
				ReturnType:    types.Bool,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					l := datum.MustBeInt(left)
					r := datum.MustBeInt(right)
					return datum.NewBool(l == r), nil
				},
			},
			{LeftType: types.Float, RightType: types.Float}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Float, RightType: types.Float},
				ReturnType:    types.Bool,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					l := datum.MustBeFloat(left)
					r := datum.MustBeFloat(right)
					return datum.NewBool(l == r), nil
				},
			},
			{LeftType: types.String, RightType: types.String}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.String, RightType: types.String},
				ReturnType:    types.Bool,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					l := datum.MustBeString(left)
					r := datum.MustBeString(right)
					return datum.NewBool(l == r), nil
				},
			},
		},
	},
	opcode.LogicAnd: {
		overloads: map[BinOpArgTypes]*BinOp{
			{LeftType: types.Bool, RightType: types.Bool}: {
				BinOpArgTypes: BinOpArgTypes{LeftType: types.Bool, RightType: types.Bool},
				ReturnType:    types.Bool,
				Fn: func(_ *EvalContext, left datum.Datum, right datum.Datum) (datum.Datum, error) {
					l := datum.MustBeBool(left)
					r := datum.MustBeBool(right)
					return datum.NewBool(bool(l) && bool(r)), nil
				},
			},
		},
	},
}
