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
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/cockroachdb/apd/v3"
	"github.com/vescale/zgraph/datum"
	"github.com/vescale/zgraph/parser/opcode"
	"github.com/vescale/zgraph/types"
)

var _ Expression = &BinaryExpr{}

type BinaryExpr struct {
	Op    opcode.Op
	Left  Expression
	Right Expression

	overloadSet binOpOverloadSet
	inferredOp  *binOp
}

func NewBinaryExpr(op opcode.Op, left, right Expression) (*BinaryExpr, error) {
	overloadSet, ok := binOps[op]
	if !ok {
		return nil, fmt.Errorf("unsupported binary operator: %s", op)
	}
	expr := &BinaryExpr{
		Op:          op,
		Left:        left,
		Right:       right,
		overloadSet: overloadSet,
	}

	leftType, rightType := left.ReturnType(), right.ReturnType()
	if leftType != types.Unknown && rightType != types.Unknown {
		if inferredOp, ok := overloadSet.lookupImpl(leftType, rightType); ok {
			expr.inferredOp = inferredOp
		} else {
			// TODO: Do we need to return an error here?
		}
	}
	return expr, nil
}

func (expr *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", expr.Left, expr.Op, expr.Right)
}

func (expr *BinaryExpr) ReturnType() types.T {
	if expr.inferredOp != nil {
		return expr.inferredOp.returnType
	}
	return expr.overloadSet.inferReturnType(expr.Left.ReturnType(), expr.Right.ReturnType())
}

// Eval implements the Expression interface.
// TODO: Move logical operators out of BinaryExpr, since they have too many special cases.
func (expr *BinaryExpr) Eval(evalCtx *EvalContext) (datum.Datum, error) {
	left, err := expr.Left.Eval(evalCtx)
	if err != nil || left == datum.Null {
		return left, err
	}
	right, err := expr.Right.Eval(evalCtx)
	if err != nil || right == datum.Null {
		return right, err
	}
	op := expr.inferredOp
	if op == nil {
		op, _ = expr.overloadSet.lookupImpl(left.Type(), right.Type())
	}
	if op == nil {
		// TODO: Do we need to return an error here?
		return datum.Null, nil
	}
	return op.fn(evalCtx, left, right)
}

type binOp struct {
	leftType   types.T
	rightType  types.T
	returnType types.T
	fn         func(_ *EvalContext, left, right datum.Datum) (datum.Datum, error)
}

type binOpOverloadSet interface {
	lookupImpl(leftType, rightType types.T) (*binOp, bool)
	inferReturnType(leftType, rightType types.T) types.T
}

type genericBinOpOverloadSet struct {
	overloads []*binOp
}

func (s *genericBinOpOverloadSet) lookupImpl(leftType, rightType types.T) (*binOp, bool) {
	for _, overload := range s.overloads {
		if leftType == overload.leftType && rightType == overload.rightType {
			return overload, true
		}
	}
	return nil, false
}

func (s *genericBinOpOverloadSet) inferReturnType(leftType, rightType types.T) types.T {
	for _, binOp := range s.overloads {
		if leftType == binOp.leftType && rightType == binOp.rightType {
			return binOp.returnType
		}
	}
	return types.Unknown
}

type cmpOpOverloadSet struct {
	overload *binOp
}

func (s *cmpOpOverloadSet) lookupImpl(_, _ types.T) (*binOp, bool) {
	return s.overload, true
}

func (s *cmpOpOverloadSet) inferReturnType(_, _ types.T) types.T {
	return types.Bool
}

func (s *cmpOpOverloadSet) callOnNullInput() bool {
	return false
}

type logicalOpOverloadSet struct {
	overload *binOp
}

func (s *logicalOpOverloadSet) lookupImpl(leftType, rightType types.T) (*binOp, bool) {
	if leftType == types.Bool && rightType == types.Bool {
		return s.overload, true
	}
	return nil, false
}

func (s *logicalOpOverloadSet) inferReturnType(leftType, rightType types.T) types.T {
	return types.Bool
}

var binOps = map[opcode.Op]binOpOverloadSet{
	opcode.Plus: &genericBinOpOverloadSet{overloads: []*binOp{
		{types.Int, types.Int, types.Int, plusIntInt},
		{types.Int, types.Float, types.Float, plusIntFloat},
		{types.Int, types.Decimal, types.Decimal, plusIntDecimal},
		{types.Float, types.Int, types.Float, plusFloatInt},
		{types.Float, types.Float, types.Float, plusFloatFloat},
		{types.Float, types.Decimal, types.Decimal, plusFloatDecimal},
		{types.Decimal, types.Int, types.Decimal, plusDecimalInt},
		{types.Decimal, types.Float, types.Decimal, plusDecimalFloat},
		{types.Decimal, types.Decimal, types.Decimal, plusDecimalDecimal},
		{types.Date, types.Interval, types.Date, plusDateInterval},
		{types.Time, types.Interval, types.Time, plusTimeInterval},
		{types.TimeTZ, types.Interval, types.TimeTZ, plusTimeTZInterval},
		{types.Timestamp, types.Interval, types.Timestamp, plusTimestampInterval},
		{types.TimestampTZ, types.Interval, types.TimestampTZ, plusTimestampTZInterval},
	}},
	opcode.Minus: &genericBinOpOverloadSet{overloads: []*binOp{
		{types.Int, types.Int, types.Int, minusIntInt},
		{types.Int, types.Float, types.Float, minusIntFloat},
		{types.Int, types.Decimal, types.Decimal, minusIntDecimal},
		{types.Float, types.Int, types.Float, minusFloatInt},
		{types.Float, types.Float, types.Float, minusFloatFloat},
		{types.Float, types.Decimal, types.Decimal, minusFloatDecimal},
		{types.Decimal, types.Int, types.Decimal, minusDecimalInt},
		{types.Decimal, types.Float, types.Decimal, minusDecimalFloat},
		{types.Decimal, types.Decimal, types.Decimal, minusDecimalDecimal},
		{types.Date, types.Interval, types.Date, minusDateInterval},
		{types.Time, types.Interval, types.Time, minusTimeInterval},
		{types.TimeTZ, types.Interval, types.TimeTZ, minusTimeTZInterval},
		{types.Timestamp, types.Interval, types.Timestamp, minusTimestampInterval},
		{types.TimestampTZ, types.Interval, types.TimestampTZ, minusTimestampTZInterval},
	}},
	opcode.Mul: &genericBinOpOverloadSet{overloads: []*binOp{
		{types.Int, types.Int, types.Int, mulIntInt},
		{types.Int, types.Float, types.Float, mulIntFloat},
		{types.Int, types.Decimal, types.Decimal, mulIntDecimal},
		{types.Float, types.Int, types.Float, mulFloatInt},
		{types.Float, types.Float, types.Float, mulFloatFloat},
		{types.Float, types.Decimal, types.Decimal, mulFloatDecimal},
		{types.Decimal, types.Int, types.Decimal, mulDecimalInt},
		{types.Decimal, types.Float, types.Decimal, mulDecimalFloat},
		{types.Decimal, types.Decimal, types.Decimal, mulDecimalDecimal},
	}},
	opcode.Div: &genericBinOpOverloadSet{overloads: []*binOp{
		{types.Int, types.Int, types.Int, divIntInt},
		{types.Int, types.Float, types.Float, divIntFloat},
		{types.Int, types.Decimal, types.Decimal, divIntDecimal},
		{types.Float, types.Int, types.Float, divFloatInt},
		{types.Float, types.Float, types.Float, divFloatFloat},
		{types.Float, types.Decimal, types.Decimal, divFloatDecimal},
		{types.Decimal, types.Int, types.Decimal, divDecimalInt},
		{types.Decimal, types.Float, types.Decimal, divDecimalFloat},
		{types.Decimal, types.Decimal, types.Decimal, divDecimalDecimal},
	}},
	opcode.Mod: &genericBinOpOverloadSet{overloads: []*binOp{
		{types.Int, types.Int, types.Int, modIntInt},
		{types.Int, types.Float, types.Float, modIntFloat},
		{types.Int, types.Decimal, types.Decimal, modIntDecimal},
		{types.Float, types.Int, types.Float, modFloatInt},
		{types.Float, types.Float, types.Float, modFloatFloat},
		{types.Float, types.Decimal, types.Decimal, modFloatDecimal},
		{types.Decimal, types.Int, types.Decimal, modDecimalInt},
		{types.Decimal, types.Float, types.Decimal, modDecimalFloat},
		{types.Decimal, types.Decimal, types.Decimal, modDecimalDecimal},
	}},
	opcode.EQ: &cmpOpOverloadSet{overload: &binOp{returnType: types.Bool,
		fn: func(evalCtx *EvalContext, left, right datum.Datum) (datum.Datum, error) {
			cmp, canCmp := compareDatum(evalCtx, left, right)
			if !canCmp {
				return datum.Null, nil
			}
			return datum.NewBool(cmp == 0), nil
		},
	}},
	opcode.NE: &cmpOpOverloadSet{overload: &binOp{returnType: types.Bool,
		fn: func(evalCtx *EvalContext, left, right datum.Datum) (datum.Datum, error) {
			cmp, canCmp := compareDatum(evalCtx, left, right)
			if !canCmp {
				return datum.Null, nil
			}
			return datum.NewBool(cmp != 0), nil
		},
	}},
	opcode.LT: &cmpOpOverloadSet{overload: &binOp{returnType: types.Bool,
		fn: func(evalCtx *EvalContext, left, right datum.Datum) (datum.Datum, error) {
			cmp, canCmp := compareDatum(evalCtx, left, right)
			if !canCmp {
				return datum.Null, nil
			}
			return datum.NewBool(cmp < 0), nil
		},
	}},
	opcode.LE: &cmpOpOverloadSet{overload: &binOp{returnType: types.Bool,
		fn: func(evalCtx *EvalContext, left, right datum.Datum) (datum.Datum, error) {
			cmp, canCmp := compareDatum(evalCtx, left, right)
			if !canCmp {
				return datum.Null, nil
			}
			return datum.NewBool(cmp <= 0), nil
		},
	}},
	opcode.GT: &cmpOpOverloadSet{overload: &binOp{returnType: types.Bool,
		fn: func(evalCtx *EvalContext, left, right datum.Datum) (datum.Datum, error) {
			cmp, canCmp := compareDatum(evalCtx, left, right)
			if !canCmp {
				return datum.Null, nil
			}
			return datum.NewBool(cmp > 0), nil
		},
	}},
	opcode.GE: &cmpOpOverloadSet{overload: &binOp{returnType: types.Bool,
		fn: func(evalCtx *EvalContext, left, right datum.Datum) (datum.Datum, error) {
			cmp, canCmp := compareDatum(evalCtx, left, right)
			if !canCmp {
				return datum.Null, nil
			}
			return datum.NewBool(cmp >= 0), nil
		},
	}},
	opcode.LogicAnd: &logicalOpOverloadSet{
		overload: &binOp{
			leftType:   types.Bool,
			rightType:  types.Bool,
			returnType: types.Bool,
			fn: func(evalCtx *EvalContext, left, right datum.Datum) (datum.Datum, error) {
				l := datum.MustBeBool(left)
				r := datum.MustBeBool(right)
				return datum.NewBool(bool(l) && bool(r)), nil
			},
		},
	},
	opcode.LogicOr: &logicalOpOverloadSet{
		overload: &binOp{
			leftType:   types.Bool,
			rightType:  types.Bool,
			returnType: types.Bool,
			fn: func(evalCtx *EvalContext, left, right datum.Datum) (datum.Datum, error) {
				l := datum.MustBeBool(left)
				r := datum.MustBeBool(right)
				return datum.NewBool(bool(l) || bool(r)), nil
			},
		},
	},
}

func plusIntInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeInt(right)
	return datum.NewInt(int64(l) + int64(r)), nil
}

func plusIntFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeFloat(right)
	return datum.NewFloat(float64(l) + float64(r)), nil
}

func plusIntDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeDecimal(right).Decimal
	d := &datum.Decimal{}
	d.SetInt64(int64(l))
	_, err := apd.BaseContext.Add(&d.Decimal, &d.Decimal, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func plusFloatInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeInt(right)
	return datum.NewFloat(float64(l) + float64(r)), nil
}

func plusFloatFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeFloat(right)
	return datum.NewFloat(float64(l) + float64(r)), nil
}

func plusFloatDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeDecimal(right).Decimal
	d := &datum.Decimal{}
	d.SetFloat64(float64(l))
	_, err := apd.BaseContext.Add(&d.Decimal, &d.Decimal, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func plusDecimalInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeInt(right)
	d := &datum.Decimal{}
	d.SetInt64(int64(r))
	_, err := apd.BaseContext.Add(&d.Decimal, &l, &d.Decimal)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func plusDecimalFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeFloat(right)
	d := &datum.Decimal{}
	d.SetFloat64(float64(r))
	_, err := apd.BaseContext.Add(&d.Decimal, &l, &d.Decimal)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func plusDecimalDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeDecimal(right).Decimal
	d := &datum.Decimal{}
	_, err := apd.BaseContext.Add(&d.Decimal, &l, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func plusDateInterval(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("plusDateInterval unimplemented")
}

func plusTimeInterval(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("plusTimeInterval unimplemented")
}

func plusTimeTZInterval(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("plusTimeTZInterval unimplemented")
}

func plusTimestampInterval(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("plusTimestampInterval unimplemented")
}

func plusTimestampTZInterval(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("plusTimestampTZInterval unimplemented")
}

func minusIntInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeInt(right)
	return datum.NewInt(int64(l) - int64(r)), nil
}

func minusIntFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeFloat(right)
	return datum.NewFloat(float64(l) - float64(r)), nil
}

func minusIntDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeDecimal(right).Decimal
	d := &datum.Decimal{}
	d.SetInt64(int64(l))
	_, err := apd.BaseContext.Sub(&d.Decimal, &d.Decimal, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func minusFloatInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeInt(right)
	return datum.NewFloat(float64(l) - float64(r)), nil
}

func minusFloatFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeFloat(right)
	return datum.NewFloat(float64(l) - float64(r)), nil
}

func minusFloatDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeDecimal(right).Decimal
	d := &datum.Decimal{}
	d.SetFloat64(float64(l))
	_, err := apd.BaseContext.Sub(&d.Decimal, &d.Decimal, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func minusDecimalInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeInt(right)
	d := &datum.Decimal{}
	d.SetInt64(int64(r))
	_, err := apd.BaseContext.Sub(&d.Decimal, &l, &d.Decimal)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func minusDecimalFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeFloat(right)
	d := &datum.Decimal{}
	d.SetFloat64(float64(r))
	_, err := apd.BaseContext.Sub(&d.Decimal, &l, &d.Decimal)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func minusDecimalDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeDecimal(right).Decimal
	d := &datum.Decimal{}
	_, err := apd.BaseContext.Sub(&d.Decimal, &l, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func minusDateInterval(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("minusDateInterval unimplemented")
}

func minusTimeInterval(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("minusTimeInterval unimplemented")
}

func minusTimeTZInterval(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("minusTimeTZInterval unimplemented")
}

func minusTimestampInterval(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("minusTimestampInterval unimplemented")
}

func minusTimestampTZInterval(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("minusTimestampTZInterval unimplemented")
}

func mulIntInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeInt(right)
	return datum.NewInt(int64(l) * int64(r)), nil
}

func mulIntFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeFloat(right)
	return datum.NewFloat(float64(l) * float64(r)), nil
}

func mulIntDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeDecimal(right).Decimal
	d := &datum.Decimal{}
	d.SetInt64(int64(l))
	_, err := apd.BaseContext.Mul(&d.Decimal, &d.Decimal, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func mulFloatInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeInt(right)
	return datum.NewFloat(float64(l) * float64(r)), nil
}

func mulFloatFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeFloat(right)
	return datum.NewFloat(float64(l) * float64(r)), nil
}

func mulFloatDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeDecimal(right).Decimal
	d := &datum.Decimal{}
	d.SetFloat64(float64(l))
	_, err := apd.BaseContext.Mul(&d.Decimal, &d.Decimal, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func mulDecimalInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeInt(right)
	d := &datum.Decimal{}
	d.SetInt64(int64(r))
	_, err := apd.BaseContext.Mul(&d.Decimal, &l, &d.Decimal)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func mulDecimalFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeFloat(right)
	d := &datum.Decimal{}
	d.SetFloat64(float64(r))
	_, err := apd.BaseContext.Mul(&d.Decimal, &l, &d.Decimal)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func mulDecimalDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeDecimal(right).Decimal
	d := &datum.Decimal{}
	_, err := apd.BaseContext.Mul(&d.Decimal, &l, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func divIntInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeInt(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	return datum.NewInt(int64(l) / int64(r)), nil
}

func divIntFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeFloat(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	return datum.NewFloat(float64(l) / float64(r)), nil
}

func divIntDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeDecimal(right).Decimal
	if r.IsZero() {
		return nil, errors.New("division by zero")
	}
	d := &datum.Decimal{}
	d.SetInt64(int64(l))
	_, err := apd.BaseContext.Quo(&d.Decimal, &d.Decimal, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func divFloatInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeInt(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	return datum.NewFloat(float64(l) / float64(r)), nil
}

func divFloatFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeFloat(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	return datum.NewFloat(float64(l) / float64(r)), nil
}

func divFloatDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeDecimal(right).Decimal
	if r.IsZero() {
		return nil, errors.New("division by zero")
	}
	d := &datum.Decimal{}
	d.SetFloat64(float64(l))
	_, err := apd.BaseContext.Quo(&d.Decimal, &d.Decimal, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func divDecimalInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeInt(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	d := &datum.Decimal{}
	d.SetInt64(int64(r))
	_, err := apd.BaseContext.Quo(&d.Decimal, &l, &d.Decimal)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func divDecimalFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeFloat(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	d := &datum.Decimal{}
	d.SetFloat64(float64(r))
	_, err := apd.BaseContext.Quo(&d.Decimal, &l, &d.Decimal)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func divDecimalDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeDecimal(right).Decimal
	if r.IsZero() {
		return nil, errors.New("division by zero")
	}
	d := &datum.Decimal{}
	_, err := apd.BaseContext.Quo(&d.Decimal, &l, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func modIntInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeInt(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	return datum.NewInt(int64(l) % int64(r)), nil
}

func modIntFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeFloat(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	return datum.NewFloat(math.Mod(float64(l), float64(r))), nil
}

func modIntDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeDecimal(right).Decimal
	if r.IsZero() {
		return nil, errors.New("division by zero")
	}
	d := &datum.Decimal{}
	d.SetInt64(int64(l))
	_, err := apd.BaseContext.Rem(&d.Decimal, &d.Decimal, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func modFloatInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeInt(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	return datum.NewFloat(math.Mod(float64(l), float64(r))), nil
}

func modFloatFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeFloat(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	return datum.NewFloat(math.Mod(float64(l), float64(r))), nil
}

func modFloatDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeDecimal(right).Decimal
	if r.IsZero() {
		return nil, errors.New("division by zero")
	}
	d := &datum.Decimal{}
	d.SetFloat64(float64(l))
	_, err := apd.BaseContext.Rem(&d.Decimal, &d.Decimal, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func modDecimalInt(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeInt(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	d := &datum.Decimal{}
	d.SetInt64(int64(r))
	_, err := apd.BaseContext.Rem(&d.Decimal, &l, &d.Decimal)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func modDecimalFloat(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeFloat(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	d := &datum.Decimal{}
	d.SetFloat64(float64(r))
	_, err := apd.BaseContext.Rem(&d.Decimal, &l, &d.Decimal)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func modDecimalDecimal(_ *EvalContext, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeDecimal(right).Decimal
	if r.IsZero() {
		return nil, errors.New("division by zero")
	}
	d := &datum.Decimal{}
	_, err := apd.BaseContext.Rem(&d.Decimal, &l, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func compareDatum(_ *EvalContext, left, right datum.Datum) (cmp int, canCmp bool) {
	switch l := left.(type) {
	case *datum.Bool:
		switch r := right.(type) {
		case *datum.Bool:
			return compareBool(bool(*l), bool(*r)), true
		}
	case *datum.Int:
		switch r := right.(type) {
		case *datum.Int:
			return compareInt(int64(*l), int64(*r)), true
		case *datum.Float:
			return compareFloat(float64(*l), float64(*r)), true
		}
	case *datum.Float:
		switch r := right.(type) {
		case *datum.Int:
			return compareFloat(float64(*l), float64(*r)), true
		case *datum.Float:
			return compareFloat(float64(*l), float64(*r)), true
		}
	case *datum.Decimal:
	case *datum.Bytes:
		switch r := right.(type) {
		case *datum.Bytes:
			return bytes.Compare(*l, *r), true
		case *datum.String:
			return bytes.Compare(*l, []byte(*r)), true
		}
	case *datum.String:
		switch r := right.(type) {
		case *datum.Bytes:
			return bytes.Compare([]byte(*l), *r), true
		case *datum.String:
			return strings.Compare(string(*l), string(*r)), true
		}
	case *datum.Date:
	case *datum.Time:
	case *datum.Timestamp:
	case *datum.Interval:
	case *datum.Vertex:
		switch r := right.(type) {
		case *datum.Vertex:
			return compareInt(l.ID, r.ID), true
		}
	case *datum.Edge:
	}
	return 0, false
}

func compareBool(a, b bool) int {
	if a == b {
		return 0
	}
	if a {
		return 1
	}
	return -1
}

func compareInt(a, b int64) int {
	if a == b {
		return 0
	}
	if a > b {
		return 1
	}
	return -1
}

func compareFloat(a, b float64) int {
	if a == b {
		return 0
	}
	if a > b {
		return 1
	}
	return -1
}
