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

var (
	_ functionClass = &arithmeticPlusFunctionClass{}
	_ functionClass = &arithmeticMinusFunctionClass{}
	_ functionClass = &arithmeticMultiplyFunctionClass{}
	_ functionClass = &arithmeticDivideFunctionClass{}
)

var (
	_ builtinFunc = &builtinArithmeticPlusIntSig{}
	_ builtinFunc = &builtinArithmeticPlusRealSig{}
	_ builtinFunc = &builtinArithmeticPlusDecimalSig{}

	_ builtinFunc = &builtinArithmeticMinusIntSig{}
	_ builtinFunc = &builtinArithmeticMinusRealSig{}
	_ builtinFunc = &builtinArithmeticMinusDecimalSig{}

	_ builtinFunc = &builtinArithmeticMultiplyIntSig{}
	_ builtinFunc = &builtinArithmeticMultiplyRealSig{}
	_ builtinFunc = &builtinArithmeticMultiplyDecimalSig{}

	_ builtinFunc = &builtinArithmeticDivideIntSig{}
	_ builtinFunc = &builtinArithmeticDivideRealSig{}
	_ builtinFunc = &builtinArithmeticDivideDecimalSig{}
)

type arithmeticPlusFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticPlusFunctionClass) getFunction(args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	// TODO: type infer
	sig := &builtinArithmeticPlusIntSig{newBaseBuiltinFunc(args)}
	return sig, nil
}

type builtinArithmeticPlusIntSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticPlusIntSig) evalInt(row Row) (Nullable[int64], error) {
	var result Nullable[int64]
	a, err := s.args[0].EvalInt(row)
	if a.IsNull() || err != nil {
		return result, err
	}
	b, err := s.args[1].EvalInt(row)
	if b.IsNull() || err != nil {
		return result, err
	}
	result.Set(a.Get() + b.Get())
	return result, nil
}

type builtinArithmeticPlusRealSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticPlusRealSig) evalReal(row Row) (Nullable[float64], error) {
	var result Nullable[float64]
	a, err := s.args[0].EvalReal(row)
	if a.IsNull() || err != nil {
		return result, err
	}
	b, err := s.args[1].EvalReal(row)
	if b.IsNull() || err != nil {
		return result, err
	}
	result.Set(a.Get() + b.Get())
	return result, nil
}

type builtinArithmeticPlusDecimalSig struct {
	baseBuiltinFunc
}

type arithmeticMinusFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticMinusFunctionClass) getFunction(args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	// TODO: type infer
	sig := &builtinArithmeticMinusIntSig{newBaseBuiltinFunc(args)}
	return sig, nil
}

type builtinArithmeticMinusIntSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticMinusIntSig) evalInt(row Row) (Nullable[int64], error) {
	var result Nullable[int64]
	a, err := s.args[0].EvalInt(row)
	if a.IsNull() || err != nil {
		return result, err
	}
	b, err := s.args[1].EvalInt(row)
	if b.IsNull() || err != nil {
		return result, err
	}
	result.Set(a.Get() - b.Get())
	return result, nil
}

type builtinArithmeticMinusRealSig struct {
	baseBuiltinFunc
}

type builtinArithmeticMinusDecimalSig struct {
	baseBuiltinFunc
}

type arithmeticMultiplyFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticMultiplyFunctionClass) getFunction(args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	// TODO: type infer
	sig := &builtinArithmeticMultiplyIntSig{newBaseBuiltinFunc(args)}
	return sig, nil
}

type builtinArithmeticMultiplyIntSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticMultiplyIntSig) evalInt(row Row) (Nullable[int64], error) {
	var result Nullable[int64]
	a, err := s.args[0].EvalInt(row)
	if a.IsNull() || err != nil {
		return result, err
	}
	b, err := s.args[1].EvalInt(row)
	if b.IsNull() || err != nil {
		return result, err
	}
	result.Set(a.Get() * b.Get())
	return result, nil
}

type builtinArithmeticMultiplyRealSig struct {
	baseBuiltinFunc
}

type builtinArithmeticMultiplyDecimalSig struct {
	baseBuiltinFunc
}

type arithmeticDivideFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticDivideFunctionClass) getFunction(args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	// TODO: type infer
	sig := &builtinArithmeticDivideIntSig{newBaseBuiltinFunc(args)}
	return sig, nil
}

type builtinArithmeticDivideIntSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticDivideIntSig) evalInt(row Row) (Nullable[int64], error) {
	var result Nullable[int64]
	a, err := s.args[0].EvalInt(row)
	if a.IsNull() || err != nil {
		return result, err
	}
	b, err := s.args[1].EvalInt(row)
	if b.IsNull() || err != nil {
		return result, err
	}
	result.Set(a.Get() / b.Get())
	return result, nil
}

type builtinArithmeticDivideRealSig struct {
	baseBuiltinFunc
}

type builtinArithmeticDivideDecimalSig struct {
	baseBuiltinFunc
}
