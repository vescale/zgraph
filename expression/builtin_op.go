// Copyright 2023 zGraph Authors. All rights reserved.
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
	_ functionClass = &logicAndFunctionClass{}
)

var (
	_ builtinFunc = &builtinLogicAndSig{}
)

type logicAndFunctionClass struct {
	baseFunctionClass
}

func (c *logicAndFunctionClass) getFunction(args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	return &builtinLogicAndSig{newBaseBuiltinFunc(args)}, nil
}

type builtinLogicAndSig struct {
	baseBuiltinFunc
}

func (s *builtinLogicAndSig) eval(ctx *stmtctx.Context, row Row) (types.Datum, error) {
	a, err := evalBool(ctx, row, s.args[0])
	if err != nil || !a {
		return types.NewBoolDatum(false), err
	}
	b, err := evalBool(ctx, row, s.args[1])
	if err != nil || !b {
		return types.NewBoolDatum(false), err
	}
	return types.NewBoolDatum(true), nil
}

func evalBool(ctx *stmtctx.Context, row Row, expr Expression) (bool, error) {
	val, err := expr.Eval(ctx, row)
	if err != nil || val.Kind() != types.KindBool {
		return false, err
	}
	return val.GetBool(), nil
}
