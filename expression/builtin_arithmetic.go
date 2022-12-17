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
	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/types"
)

var (
	_ functionClass = &arithmeticPlusFunctionClass{}
	_ functionClass = &arithmeticMinusFunctionClass{}
	_ functionClass = &arithmeticMultiplyFunctionClass{}
	_ functionClass = &arithmeticDivideFunctionClass{}
)

var (
	_ builtinFunc = &builtinArithmeticPlusSig{}
	_ builtinFunc = &builtinArithmeticMinusSig{}
	_ builtinFunc = &builtinArithmeticMultiplySig{}
	_ builtinFunc = &builtinArithmeticDivideSig{}
)

type arithmeticPlusFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticPlusFunctionClass) getFunction(args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	sig := &builtinArithmeticPlusSig{newBaseBuiltinFunc(args)}
	return sig, nil
}

type builtinArithmeticPlusSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticPlusSig) eval(ctx *stmtctx.Context, row Row) (types.Datum, error) {
	a, err := s.args[0].Eval(ctx, row)
	if err != nil || a.IsNull() {
		return types.Datum{}, err
	}
	b, err := s.args[1].Eval(ctx, row)
	if err != nil || b.IsNull() {
		return types.Datum{}, err
	}
	return computePlus(a, b)
}

func computePlus(a, b types.Datum) (types.Datum, error) {
	f, ok := datumPlusFuncs[types.NewKindPair(a.Kind(), b.Kind())]
	if !ok {
		return types.Datum{}, ErrInvalidOp
	}
	return f(a, b)
}

type datumPlusFunc func(a, b types.Datum) (types.Datum, error)

var datumPlusFuncs = map[types.KindPair]datumPlusFunc{
	{types.KindInt64, types.KindInt64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetInt64(a.GetInt64() + b.GetInt64())
		return d, nil
	},
	{types.KindInt64, types.KindUint64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetInt64(a.GetInt64() + int64(b.GetUint64()))
		return d, nil
	},
	{types.KindUint64, types.KindInt64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetInt64(int64(a.GetUint64()) + b.GetInt64())
		return d, nil
	},
	{types.KindUint64, types.KindUint64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetUint64(a.GetUint64() + b.GetUint64())
		return d, nil
	},
}

type arithmeticMinusFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticMinusFunctionClass) getFunction(args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	sig := &builtinArithmeticMinusSig{newBaseBuiltinFunc(args)}
	return sig, nil
}

type builtinArithmeticMinusSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticMinusSig) eval(ctx *stmtctx.Context, row Row) (types.Datum, error) {
	a, err := s.args[0].Eval(ctx, row)
	if err != nil {
		return types.Datum{}, err
	}
	b, err := s.args[1].Eval(ctx, row)
	if err != nil {
		return types.Datum{}, err
	}
	return computeMinus(a, b)
}

func computeMinus(a, b types.Datum) (types.Datum, error) {
	f, ok := datumMinusFuncs[types.NewKindPair(a.Kind(), b.Kind())]
	if !ok {
		return types.Datum{}, ErrInvalidOp
	}
	return f(a, b)
}

type datumMinusFunc func(a, b types.Datum) (types.Datum, error)

var datumMinusFuncs = map[types.KindPair]datumMinusFunc{
	{types.KindInt64, types.KindInt64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetInt64(a.GetInt64() - b.GetInt64())
		return d, nil
	},
	{types.KindInt64, types.KindUint64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetInt64(a.GetInt64() - int64(b.GetUint64()))
		return d, nil
	},
	{types.KindUint64, types.KindInt64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetInt64(int64(a.GetUint64()) - b.GetInt64())
		return d, nil
	},
	{types.KindUint64, types.KindUint64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetUint64(a.GetUint64() - b.GetUint64())
		return d, nil
	},
}

type arithmeticMultiplyFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticMultiplyFunctionClass) getFunction(args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	sig := &builtinArithmeticMultiplySig{newBaseBuiltinFunc(args)}
	return sig, nil
}

type builtinArithmeticMultiplySig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticMultiplySig) eval(ctx *stmtctx.Context, row Row) (types.Datum, error) {
	a, err := s.args[0].Eval(ctx, row)
	if err != nil {
		return types.Datum{}, err
	}
	b, err := s.args[1].Eval(ctx, row)
	if err != nil {
		return types.Datum{}, err
	}
	return computeMultiply(a, b)
}

func computeMultiply(a, b types.Datum) (types.Datum, error) {
	f, ok := datumMultiplyFuncs[types.NewKindPair(a.Kind(), b.Kind())]
	if !ok {
		return types.Datum{}, ErrInvalidOp
	}
	return f(a, b)
}

type datumMultiplyFunc func(a, b types.Datum) (types.Datum, error)

var datumMultiplyFuncs = map[types.KindPair]datumMultiplyFunc{
	{types.KindInt64, types.KindInt64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetInt64(a.GetInt64() * b.GetInt64())
		return d, nil
	},
	{types.KindInt64, types.KindUint64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetInt64(a.GetInt64() * int64(b.GetUint64()))
		return d, nil
	},
	{types.KindUint64, types.KindInt64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetInt64(int64(a.GetUint64()) * b.GetInt64())
		return d, nil
	},
	{types.KindUint64, types.KindUint64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetUint64(a.GetUint64() * b.GetUint64())
		return d, nil
	},
}

type arithmeticDivideFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticDivideFunctionClass) getFunction(args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	sig := &builtinArithmeticDivideSig{newBaseBuiltinFunc(args)}
	return sig, nil
}

type builtinArithmeticDivideSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticDivideSig) eval(ctx *stmtctx.Context, row Row) (types.Datum, error) {
	a, err := s.args[0].Eval(ctx, row)
	if err != nil {
		return types.Datum{}, err
	}
	b, err := s.args[1].Eval(ctx, row)
	if err != nil {
		return types.Datum{}, err
	}
	return computeDivide(a, b)
}

func computeDivide(a, b types.Datum) (types.Datum, error) {
	f, ok := datumDivideFuncs[types.NewKindPair(a.Kind(), b.Kind())]
	if !ok {
		return types.Datum{}, ErrInvalidOp
	}
	return f(a, b)
}

type datumDivideFunc func(a, b types.Datum) (types.Datum, error)

var datumDivideFuncs = map[types.KindPair]datumDivideFunc{
	{types.KindInt64, types.KindInt64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetInt64(a.GetInt64() / b.GetInt64())
		return d, nil
	},
	{types.KindInt64, types.KindUint64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetInt64(a.GetInt64() / int64(b.GetUint64()))
		return d, nil
	},
	{types.KindUint64, types.KindInt64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetInt64(int64(a.GetUint64()) / b.GetInt64())
		return d, nil
	},
	{types.KindUint64, types.KindUint64}: func(a, b types.Datum) (types.Datum, error) {
		var d types.Datum
		d.SetUint64(a.GetUint64() / b.GetUint64())
		return d, nil
	},
}
