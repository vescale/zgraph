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
	"errors"

	"github.com/vescale/zgraph/parser/opcode"

	"github.com/vescale/zgraph/types"
)

type builtinFunc interface {
	getArgs() []Expression
	evalInt(row Row) (Nullable[int64], error)
	evalReal(row Row) (Nullable[float64], error)
	evalString(row Row) (Nullable[string], error)
	evalDecimal(row Row) (Nullable[*types.Decimal], error)
}

type functionClass interface {
	// getFunction gets a function instance.
	getFunction(args []Expression) (builtinFunc, error)
	// verifyArgsByCount verifies the count of parameters.
	verifyArgsByCount(l int) error
}

// funcs holds all registered builtin functions. When new function is added,
// check expression/function_traits.go to see if it should be appended to
// any set there.
var funcs = map[string]functionClass{
	opcode.Plus.String():  &arithmeticPlusFunctionClass{baseFunctionClass{opcode.Plus.String(), 2, 2}},
	opcode.Minus.String(): &arithmeticMinusFunctionClass{baseFunctionClass{opcode.Minus.String(), 2, 2}},
	opcode.Mul.String():   &arithmeticMultiplyFunctionClass{baseFunctionClass{opcode.Mul.String(), 2, 2}},
	opcode.Div.String():   &arithmeticDivideFunctionClass{baseFunctionClass{opcode.Div.String(), 2, 2}},
}

type baseBuiltinFunc struct {
	args []Expression
}

func newBaseBuiltinFunc(args []Expression) baseBuiltinFunc {
	return baseBuiltinFunc{args: args}
}

func (b *baseBuiltinFunc) getArgs() []Expression {
	return b.args
}

func (b *baseBuiltinFunc) evalInt(row Row) (Nullable[int64], error) {
	return Nullable[int64]{}, errors.New("baseBuiltinFunc.evalInt() not implemented")
}

func (b *baseBuiltinFunc) evalReal(row Row) (Nullable[float64], error) {
	return Nullable[float64]{}, errors.New("baseBuiltinFunc.evalReal() not implemented")
}

func (b *baseBuiltinFunc) evalString(row Row) (Nullable[string], error) {
	return Nullable[string]{}, errors.New("baseBuiltinFunc.evalString() not implemented")
}

func (b *baseBuiltinFunc) evalDecimal(row Row) (Nullable[*types.Decimal], error) {
	return Nullable[*types.Decimal]{}, errors.New("baseBuiltinFunc.evalDecimal() not implemented")
}

// baseFunctionClass will be contained in every struct that implement functionClass interface.
type baseFunctionClass struct {
	funcName string
	minArgs  int
	maxArgs  int
}

func (b *baseFunctionClass) verifyArgs(args []Expression) error {
	return b.verifyArgsByCount(len(args))
}

func (b *baseFunctionClass) verifyArgsByCount(l int) error {
	if l < b.minArgs || (b.maxArgs != -1 && l > b.maxArgs) {
		return ErrIncorrectParameterCount
	}
	return nil
}
