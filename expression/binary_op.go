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

	"github.com/cockroachdb/apd/v3"
	"github.com/vescale/zgraph/datum"
	"github.com/vescale/zgraph/parser/opcode"
	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/types"
)

var _ Expression = &BinaryExpr{}

type BinaryExpr struct {
	Op     opcode.Op
	Left   Expression
	Right  Expression
	EvalOp BinaryEvalOp
}

func (expr *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", expr.Left, expr.Op, expr.Right)
}

func (expr *BinaryExpr) ReturnType() types.T {
	leftType := expr.Left.ReturnType()
	rightType := expr.Right.ReturnType()
	return expr.EvalOp.InferReturnType(leftType, rightType)
}

func (expr *BinaryExpr) Eval(stmtCtx *stmtctx.Context, input datum.Row) (datum.Datum, error) {
	left, err := expr.Left.Eval(stmtCtx, input)
	if err != nil {
		return nil, err
	}
	if left == datum.Null && !expr.EvalOp.CallOnNullInput() {
		return datum.Null, nil
	}
	right, err := expr.Right.Eval(stmtCtx, input)
	if err != nil {
		return nil, err
	}
	if right == datum.Null && !expr.EvalOp.CallOnNullInput() {
		return datum.Null, nil
	}
	return expr.EvalOp.Eval(stmtCtx, left, right)
}

func NewBinaryExpr(op opcode.Op, left, right Expression) (*BinaryExpr, error) {
	binOp, ok := binOps[op]
	if !ok {
		return nil, fmt.Errorf("unsupported binary operator: %s", op)
	}
	return &BinaryExpr{
		Op:     op,
		Left:   left,
		Right:  right,
		EvalOp: binOp,
	}, nil
}

type BinaryEvalOp interface {
	InferReturnType(leftType, rightType types.T) types.T
	CallOnNullInput() bool
	Eval(stmtCtx *stmtctx.Context, left, right datum.Datum) (datum.Datum, error)
}

var binOps = map[opcode.Op]BinaryEvalOp{
	opcode.Plus:     makeArithOp(opcode.Plus),
	opcode.Minus:    makeArithOp(opcode.Minus),
	opcode.Mul:      makeArithOp(opcode.Mul),
	opcode.Div:      makeArithOp(opcode.Div),
	opcode.Mod:      makeArithOp(opcode.Mod),
	opcode.LogicAnd: logicalAndOp{},
	opcode.LogicOr:  logicalOrOp{},
	opcode.EQ:       makeCmpOp(opcode.EQ),
	opcode.NE:       makeNegateCmpOp(opcode.EQ), // NE(left, right) is implemented as !EQ(left, right)
	opcode.LT:       makeCmpOp(opcode.LT),
	opcode.LE:       makeFlippedNegateCmpOp(opcode.LT), // LE(left, right) is implemented as !LT(right, left)
	opcode.GE:       makeNegateCmpOp(opcode.LT),        // GE(left, right) is implemented as !LT(left, right)
	opcode.GT:       makeFlippedCmpOp(opcode.LT),       // GT(left, right) is implemented as LT(right, left)
}

type typePair struct {
	left  types.T
	right types.T
}

type binEvalFunc func(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error)

func makeBinEvalFuncWithLeftCast(eval binEvalFunc, cast castFunc) binEvalFunc {
	return func(stmtCtx *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
		left, err := cast(stmtCtx, left)
		if err != nil {
			return nil, err
		}
		return eval(stmtCtx, left, right)
	}
}

func makeBinEvalFuncWithRightCast(eval binEvalFunc, cast castFunc) binEvalFunc {
	return func(stmtCtx *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
		right, err := cast(stmtCtx, right)
		if err != nil {
			return nil, err
		}
		return eval(stmtCtx, left, right)
	}
}

var numericOpReturnTypes = map[typePair]types.T{
	{types.Int, types.Int}:         types.Int,
	{types.Int, types.Float}:       types.Float,
	{types.Int, types.Decimal}:     types.Decimal,
	{types.Float, types.Int}:       types.Float,
	{types.Float, types.Float}:     types.Float,
	{types.Float, types.Decimal}:   types.Decimal,
	{types.Decimal, types.Int}:     types.Decimal,
	{types.Decimal, types.Float}:   types.Decimal,
	{types.Decimal, types.Decimal}: types.Decimal,
}

var arithOpReturnTypes = func() map[opcode.Op]map[typePair]types.T {
	result := make(map[opcode.Op]map[typePair]types.T)
	for _, op := range []opcode.Op{opcode.Plus, opcode.Minus, opcode.Mul, opcode.Div, opcode.Mod} {
		result[op] = numericOpReturnTypes
	}
	for _, op := range []opcode.Op{opcode.Plus, opcode.Minus} {
		result[op][typePair{types.Date, types.Interval}] = types.Date
		result[op][typePair{types.Time, types.Interval}] = types.Time
		result[op][typePair{types.TimeTZ, types.Interval}] = types.TimeTZ
		result[op][typePair{types.Timestamp, types.Interval}] = types.Timestamp
		result[op][typePair{types.TimestampTZ, types.Interval}] = types.TimestampTZ
	}
	return result
}()

var arithOpEvalFuncs = map[opcode.Op]map[typePair]binEvalFunc{
	opcode.Plus: {
		{types.Int, types.Int}:              plusInt,
		{types.Int, types.Float}:            makeBinEvalFuncWithLeftCast(plusFloat, castIntAsFloat),
		{types.Int, types.Decimal}:          makeBinEvalFuncWithLeftCast(plusDecimal, castIntAsDecimal),
		{types.Float, types.Int}:            makeBinEvalFuncWithRightCast(plusFloat, castIntAsFloat),
		{types.Float, types.Float}:          plusFloat,
		{types.Float, types.Decimal}:        makeBinEvalFuncWithLeftCast(plusDecimal, castFloatAsDecimal),
		{types.Decimal, types.Int}:          makeBinEvalFuncWithRightCast(plusDecimal, castIntAsDecimal),
		{types.Decimal, types.Float}:        makeBinEvalFuncWithRightCast(plusDecimal, castFloatAsDecimal),
		{types.Decimal, types.Decimal}:      plusDecimal,
		{types.Date, types.Interval}:        plusDateInterval,
		{types.Time, types.Interval}:        plusTimeInterval,
		{types.TimeTZ, types.Interval}:      plusTimeTZInterval,
		{types.Timestamp, types.Interval}:   plusTimestampInterval,
		{types.TimestampTZ, types.Interval}: plusTimestampTZInterval,
	},
	opcode.Minus: {
		{types.Int, types.Int}:              minusInt,
		{types.Int, types.Float}:            makeBinEvalFuncWithLeftCast(minusFloat, castIntAsFloat),
		{types.Int, types.Decimal}:          makeBinEvalFuncWithLeftCast(minusDecimal, castIntAsDecimal),
		{types.Float, types.Int}:            makeBinEvalFuncWithRightCast(minusFloat, castIntAsFloat),
		{types.Float, types.Float}:          minusFloat,
		{types.Float, types.Decimal}:        makeBinEvalFuncWithLeftCast(minusDecimal, castFloatAsDecimal),
		{types.Decimal, types.Int}:          makeBinEvalFuncWithRightCast(minusDecimal, castIntAsDecimal),
		{types.Decimal, types.Float}:        makeBinEvalFuncWithRightCast(minusDecimal, castFloatAsDecimal),
		{types.Decimal, types.Decimal}:      minusDecimal,
		{types.Date, types.Interval}:        minusDateInterval,
		{types.Time, types.Interval}:        minusTimeInterval,
		{types.TimeTZ, types.Interval}:      minusTimeTZInterval,
		{types.Timestamp, types.Interval}:   minusTimestampInterval,
		{types.TimestampTZ, types.Interval}: minusTimestampTZInterval,
	},
	opcode.Mul: {
		{types.Int, types.Int}:         mulInt,
		{types.Int, types.Float}:       makeBinEvalFuncWithLeftCast(mulFloat, castIntAsFloat),
		{types.Int, types.Decimal}:     makeBinEvalFuncWithLeftCast(mulDecimal, castIntAsDecimal),
		{types.Float, types.Int}:       makeBinEvalFuncWithRightCast(mulFloat, castIntAsFloat),
		{types.Float, types.Float}:     mulFloat,
		{types.Float, types.Decimal}:   makeBinEvalFuncWithLeftCast(mulDecimal, castFloatAsDecimal),
		{types.Decimal, types.Int}:     makeBinEvalFuncWithRightCast(mulDecimal, castIntAsDecimal),
		{types.Decimal, types.Float}:   makeBinEvalFuncWithRightCast(mulDecimal, castFloatAsDecimal),
		{types.Decimal, types.Decimal}: mulDecimal,
	},
	opcode.Div: {
		{types.Int, types.Int}:         divInt,
		{types.Int, types.Float}:       makeBinEvalFuncWithLeftCast(divFloat, castIntAsFloat),
		{types.Int, types.Decimal}:     makeBinEvalFuncWithLeftCast(divDecimal, castIntAsDecimal),
		{types.Float, types.Int}:       makeBinEvalFuncWithRightCast(divFloat, castIntAsFloat),
		{types.Float, types.Float}:     divFloat,
		{types.Float, types.Decimal}:   makeBinEvalFuncWithLeftCast(divDecimal, castFloatAsDecimal),
		{types.Decimal, types.Int}:     makeBinEvalFuncWithRightCast(divDecimal, castIntAsDecimal),
		{types.Decimal, types.Float}:   makeBinEvalFuncWithRightCast(divDecimal, castFloatAsDecimal),
		{types.Decimal, types.Decimal}: divDecimal,
	},
	opcode.Mod: {
		{types.Int, types.Int}:         modInt,
		{types.Int, types.Float}:       makeBinEvalFuncWithLeftCast(modFloat, castIntAsFloat),
		{types.Int, types.Decimal}:     makeBinEvalFuncWithLeftCast(modDecimal, castIntAsDecimal),
		{types.Float, types.Int}:       makeBinEvalFuncWithRightCast(modFloat, castIntAsFloat),
		{types.Float, types.Float}:     modFloat,
		{types.Float, types.Decimal}:   makeBinEvalFuncWithLeftCast(modDecimal, castFloatAsDecimal),
		{types.Decimal, types.Int}:     makeBinEvalFuncWithRightCast(modDecimal, castIntAsDecimal),
		{types.Decimal, types.Float}:   makeBinEvalFuncWithRightCast(modDecimal, castFloatAsDecimal),
		{types.Decimal, types.Decimal}: modDecimal,
	},
}

type arithOp struct {
	op          opcode.Op
	returnTypes map[typePair]types.T
	evalFuncs   map[typePair]binEvalFunc
}

func (op arithOp) InferReturnType(leftType, rightType types.T) types.T {
	return op.returnTypes[typePair{leftType, rightType}]
}

func (op arithOp) CallOnNullInput() bool {
	return false
}

func (op arithOp) Eval(ctx *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	evalFunc, ok := op.evalFuncs[typePair{left.Type(), right.Type()}]
	if !ok {
		return nil, fmt.Errorf("cannot evaluate %s on %s and %s", op.op, left.Type(), right.Type())
	}
	return evalFunc(ctx, left, right)
}

func makeArithOp(op opcode.Op) arithOp {
	returnTypes := arithOpReturnTypes[op]
	evalFuncs := arithOpEvalFuncs[op]
	return arithOp{
		op:          op,
		returnTypes: returnTypes,
		evalFuncs:   evalFuncs,
	}
}

func plusInt(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeInt(right)
	return datum.NewInt(int64(l) + int64(r)), nil
}

func plusFloat(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeFloat(right)
	return datum.NewFloat(float64(l) + float64(r)), nil
}

func plusDecimal(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeDecimal(right).Decimal
	d := &datum.Decimal{}
	_, err := apd.BaseContext.Add(&d.Decimal, &l, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func plusDateInterval(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("plusDateInterval unimplemented")
}

func plusTimeInterval(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("plusTimeInterval unimplemented")
}

func plusTimeTZInterval(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("plusTimeTZInterval unimplemented")
}

func plusTimestampInterval(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("plusTimestampInterval unimplemented")
}

func plusTimestampTZInterval(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("plusTimestampTZInterval unimplemented")
}

func minusInt(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeInt(right)
	return datum.NewInt(int64(l) - int64(r)), nil
}

func minusFloat(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeFloat(right)
	return datum.NewFloat(float64(l) - float64(r)), nil
}

func minusDecimal(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeDecimal(right).Decimal
	d := &datum.Decimal{}
	_, err := apd.BaseContext.Sub(&d.Decimal, &l, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func minusDateInterval(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("minusDateInterval unimplemented")
}

func minusTimeInterval(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("minusTimeInterval unimplemented")
}

func minusTimeTZInterval(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("minusTimeTZInterval unimplemented")
}

func minusTimestampInterval(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("minusTimestampInterval unimplemented")
}

func minusTimestampTZInterval(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, errors.New("minusTimestampTZInterval unimplemented")
}

func mulInt(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeInt(right)
	return datum.NewInt(int64(l) * int64(r)), nil
}

func mulFloat(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeFloat(right)
	return datum.NewFloat(float64(l) * float64(r)), nil
}

func mulDecimal(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left).Decimal
	r := datum.MustBeDecimal(right).Decimal
	d := &datum.Decimal{}
	_, err := apd.BaseContext.Mul(&d.Decimal, &l, &r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func divInt(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeInt(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	return datum.NewInt(int64(l) / int64(r)), nil
}

func divFloat(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeFloat(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	return datum.NewFloat(float64(l) / float64(r)), nil
}

func divDecimal(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
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

func modInt(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeInt(left)
	r := datum.MustBeInt(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	return datum.NewInt(int64(l) % int64(r)), nil
}

func modFloat(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeFloat(left)
	r := datum.MustBeFloat(right)
	if r == 0 {
		return nil, errors.New("division by zero")
	}
	return datum.NewFloat(math.Mod(float64(l), float64(r))), nil
}

func modDecimal(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
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

type logicalAndOp struct{}

func (logicalAndOp) InferReturnType(_, _ types.T) types.T {
	return types.Bool
}

func (logicalAndOp) CallOnNullInput() bool {
	return true
}

func (logicalAndOp) Eval(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	leftBool, leftIsBool := left.(*datum.Bool)
	rightBool, rightIsBool := right.(*datum.Bool)
	if left == datum.Null {
		if rightIsBool && !bool(*rightBool) {
			return datum.NewBool(false), nil
		}
		return datum.Null, nil
	}
	if right == datum.Null {
		if leftIsBool && !bool(*leftBool) {
			return datum.NewBool(false), nil
		}
		return datum.Null, nil
	}
	return datum.NewBool(bool(*leftBool) && bool(*rightBool)), nil
}

type logicalOrOp struct{}

func (logicalOrOp) InferReturnType(_, _ types.T) types.T {
	return types.Bool
}

func (logicalOrOp) CallOnNullInput() bool {
	return true
}

func (logicalOrOp) Eval(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	leftBool, leftIsBool := left.(*datum.Bool)
	rightBool, rightIsBool := right.(*datum.Bool)
	if left == datum.Null {
		if rightIsBool && bool(*rightBool) {
			return datum.NewBool(true), nil
		}
		return datum.Null, nil
	}
	if right == datum.Null {
		if leftIsBool && bool(*leftBool) {
			return datum.NewBool(true), nil
		}
		return datum.Null, nil
	}
	return datum.NewBool(bool(*leftBool) || bool(*rightBool)), nil
}

var cmpOpEvalFuncs = map[opcode.Op]map[typePair]binEvalFunc{
	opcode.EQ: {
		{types.Bool, types.Bool}:               cmpEqBool,
		{types.Int, types.Int}:                 cmpEqInt,
		{types.Int, types.Float}:               makeBinEvalFuncWithLeftCast(cmpEqFloat, castIntAsFloat),
		{types.Int, types.Decimal}:             makeBinEvalFuncWithLeftCast(cmpEqDecimal, castIntAsDecimal),
		{types.Float, types.Int}:               makeBinEvalFuncWithRightCast(cmpEqFloat, castIntAsFloat),
		{types.Float, types.Float}:             cmpEqFloat,
		{types.Float, types.Decimal}:           makeBinEvalFuncWithLeftCast(cmpEqDecimal, castFloatAsDecimal),
		{types.Decimal, types.Int}:             makeBinEvalFuncWithRightCast(cmpEqDecimal, castIntAsDecimal),
		{types.Decimal, types.Float}:           makeBinEvalFuncWithRightCast(cmpEqDecimal, castFloatAsDecimal),
		{types.Decimal, types.Decimal}:         cmpEqDecimal,
		{types.String, types.String}:           cmpEqString,
		{types.String, types.Bytes}:            makeBinEvalFuncWithRightCast(cmpEqString, castBytesAsString),
		{types.Bytes, types.String}:            makeBinEvalFuncWithLeftCast(cmpEqString, castBytesAsString),
		{types.Bytes, types.Bytes}:             cmpEqBytes,
		{types.Date, types.Date}:               cmpEqDate,
		{types.Time, types.Time}:               cmpEqTime,
		{types.Time, types.TimeTZ}:             makeBinEvalFuncWithLeftCast(cmpEqTimeTZ, castTimeAsTimeTZ),
		{types.TimeTZ, types.Time}:             makeBinEvalFuncWithRightCast(cmpEqTimeTZ, castTimeAsTimeTZ),
		{types.TimeTZ, types.TimeTZ}:           cmpEqTimeTZ,
		{types.Timestamp, types.Timestamp}:     cmpEqTimestamp,
		{types.Timestamp, types.TimestampTZ}:   makeBinEvalFuncWithLeftCast(cmpEqTimestampTZ, castTimestampAsTimestampTZ),
		{types.TimestampTZ, types.Timestamp}:   makeBinEvalFuncWithRightCast(cmpEqTimestampTZ, castTimestampAsTimestampTZ),
		{types.TimestampTZ, types.TimestampTZ}: cmpEqTimestampTZ,
		{types.Vertex, types.Vertex}:           cmpEqVertex,
		{types.Edge, types.Edge}:               cmpEqEdge,
	},
	opcode.LT: {
		{types.Bool, types.Bool}:               cmpLtBool,
		{types.Int, types.Int}:                 cmpLtInt,
		{types.Int, types.Float}:               makeBinEvalFuncWithLeftCast(cmpLtFloat, castIntAsFloat),
		{types.Int, types.Decimal}:             makeBinEvalFuncWithLeftCast(cmpLtDecimal, castIntAsDecimal),
		{types.Float, types.Int}:               makeBinEvalFuncWithRightCast(cmpLtFloat, castIntAsFloat),
		{types.Float, types.Float}:             cmpLtFloat,
		{types.Float, types.Decimal}:           makeBinEvalFuncWithLeftCast(cmpLtDecimal, castFloatAsDecimal),
		{types.Decimal, types.Int}:             makeBinEvalFuncWithRightCast(cmpLtDecimal, castIntAsDecimal),
		{types.Decimal, types.Float}:           makeBinEvalFuncWithRightCast(cmpLtDecimal, castFloatAsDecimal),
		{types.Decimal, types.Decimal}:         cmpLtDecimal,
		{types.String, types.String}:           cmpLtString,
		{types.String, types.Bytes}:            makeBinEvalFuncWithRightCast(cmpLtString, castBytesAsString),
		{types.Bytes, types.String}:            makeBinEvalFuncWithLeftCast(cmpLtString, castBytesAsString),
		{types.Bytes, types.Bytes}:             cmpLtBytes,
		{types.Date, types.Date}:               cmpLtDate,
		{types.Time, types.Time}:               cmpLtTime,
		{types.Time, types.TimeTZ}:             makeBinEvalFuncWithLeftCast(cmpLtTimeTZ, castTimeAsTimeTZ),
		{types.TimeTZ, types.Time}:             makeBinEvalFuncWithRightCast(cmpLtTimeTZ, castTimeAsTimeTZ),
		{types.TimeTZ, types.TimeTZ}:           cmpLtTimeTZ,
		{types.Timestamp, types.Timestamp}:     cmpLtTimestamp,
		{types.Timestamp, types.TimestampTZ}:   makeBinEvalFuncWithLeftCast(cmpLtTimestampTZ, castTimestampAsTimestampTZ),
		{types.TimestampTZ, types.Timestamp}:   makeBinEvalFuncWithRightCast(cmpLtTimestampTZ, castTimestampAsTimestampTZ),
		{types.TimestampTZ, types.TimestampTZ}: cmpLtTimestampTZ,
	},
}

type cmpOp struct {
	op        opcode.Op
	evalFuncs map[typePair]binEvalFunc
}

func (op cmpOp) InferReturnType(_, _ types.T) types.T {
	return types.Bool
}

func (op cmpOp) CallOnNullInput() bool {
	return false
}

func (op cmpOp) Eval(ctx *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	evalFunc, ok := op.evalFuncs[typePair{left.Type(), right.Type()}]
	if !ok {
		return nil, fmt.Errorf("cannot evaluate %s on %s and %s", op.op, left.Type(), right.Type())
	}
	return evalFunc(ctx, left, right)
}

func makeCmpOp(op opcode.Op) cmpOp {
	evalFuncs := cmpOpEvalFuncs[op]
	return cmpOp{
		op:        op,
		evalFuncs: evalFuncs,
	}
}

type flippedCmpOp struct {
	cmpOp
}

func (op flippedCmpOp) Eval(ctx *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return op.cmpOp.Eval(ctx, right, left)
}

func makeFlippedCmpOp(op opcode.Op) flippedCmpOp {
	return flippedCmpOp{makeCmpOp(op)}
}

type negateCmpOp struct {
	cmpOp
}

func (op negateCmpOp) Eval(ctx *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	res, err := op.cmpOp.Eval(ctx, left, right)
	if err != nil {
		return nil, err
	}
	return datum.NewBool(!bool(datum.MustBeBool(res))), nil
}

func makeNegateCmpOp(op opcode.Op) negateCmpOp {
	return negateCmpOp{makeCmpOp(op)}
}

type flippedNegateCmpOp struct {
	cmpOp
}

func (op flippedNegateCmpOp) Eval(ctx *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	res, err := op.cmpOp.Eval(ctx, right, left)
	if err != nil {
		return nil, err
	}
	return datum.NewBool(!bool(datum.MustBeBool(res))), nil
}

func makeFlippedNegateCmpOp(op opcode.Op) flippedNegateCmpOp {
	return flippedNegateCmpOp{makeCmpOp(op)}
}

func cmpEqBool(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return datum.NewBool(datum.MustBeBool(left) == datum.MustBeBool(right)), nil
}

func cmpLtBool(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	// left < right is true if left is false and right is true.
	return datum.NewBool(bool(!datum.MustBeBool(left)) && bool(datum.MustBeBool(right))), nil
}

func cmpEqInt(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return datum.NewBool(datum.MustBeInt(left) == datum.MustBeInt(right)), nil
}

func cmpLtInt(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return datum.NewBool(datum.MustBeInt(left) < datum.MustBeInt(right)), nil
}

func cmpEqFloat(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return datum.NewBool(datum.MustBeFloat(left) == datum.MustBeFloat(right)), nil
}

func cmpLtFloat(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return datum.NewBool(datum.MustBeFloat(left) < datum.MustBeFloat(right)), nil
}

func cmpEqDecimal(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left)
	r := datum.MustBeDecimal(right)
	return datum.NewBool(l.Cmp(&r.Decimal) == 0), nil
}

func cmpLtDecimal(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	l := datum.MustBeDecimal(left)
	r := datum.MustBeDecimal(right)
	return datum.NewBool(l.Cmp(&r.Decimal) < 0), nil
}

func cmpEqString(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return datum.NewBool(datum.MustBeString(left) == datum.MustBeString(right)), nil
}

func cmpLtString(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return datum.NewBool(datum.MustBeString(left) < datum.MustBeString(right)), nil
}

func cmpEqBytes(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return datum.NewBool(bytes.Equal(datum.MustBeBytes(left), datum.MustBeBytes(right))), nil
}

func cmpLtBytes(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return datum.NewBool(bytes.Compare(datum.MustBeBytes(left), datum.MustBeBytes(right)) < 0), nil
}

func cmpEqDate(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, fmt.Errorf("cmpEqDate not implemented")
}

func cmpLtDate(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, fmt.Errorf("cmpLtDate not implemented")
}

func cmpEqTime(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, fmt.Errorf("cmpEqTime not implemented")
}

func cmpLtTime(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, fmt.Errorf("cmpLtTime not implemented")
}

func cmpEqTimeTZ(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, fmt.Errorf("cmpEqTimeTZ not implemented")
}

func cmpLtTimeTZ(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, fmt.Errorf("cmpLtTimeTZ not implemented")
}

func cmpEqTimestamp(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, fmt.Errorf("cmpEqTimestamp not implemented")
}

func cmpLtTimestamp(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, fmt.Errorf("cmpLtTimestamp not implemented")
}

func cmpEqTimestampTZ(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, fmt.Errorf("cmpEqTimestampTZ not implemented")
}

func cmpLtTimestampTZ(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, fmt.Errorf("cmpLtTimestampTZ not implemented")
}

func cmpEqVertex(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, fmt.Errorf("cmpEqVertex not implemented")
}

func cmpEqEdge(_ *stmtctx.Context, left, right datum.Datum) (datum.Datum, error) {
	return nil, fmt.Errorf("cmpEqEdge not implemented")
}
