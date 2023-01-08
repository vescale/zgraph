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

// This file contains the implementation of the built-in vertex/edge functions:
// ID, LABEL, LABELS, HAS_LABEL, MATCH_NUMBER, ELEMENT_NUMBER, ALL_DIFFERENT,
// IN_DEGREE, OUT_DEGREE.
// See https://pgql-lang.org/spec/1.5/#vertex-and-edge-functions for more details.

package expression

import (
	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/types"
)

var (
	_ functionClass = &idFunctionClass{}
	_ functionClass = &labelFunctionClass{}
	_ functionClass = &labelsFunctionClass{}
)

var (
	_ builtinFunc = &builtinIDSig{}
	_ builtinFunc = &builtinLabelSig{}
	_ builtinFunc = &builtinLabelsSig{}
)

type idFunctionClass struct {
	baseFunctionClass
}

func (c *idFunctionClass) getFunction(args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	sig := &builtinIDSig{newBaseBuiltinFunc(args)}
	return sig, nil
}

type builtinIDSig struct {
	baseBuiltinFunc
}

func (s *builtinIDSig) eval(ctx *stmtctx.Context, row Row) (types.Datum, error) {
	val, err := s.args[0].Eval(ctx, row)
	if err != nil {
		return types.Datum{}, err
	}
	if val.Kind() != types.KindGraphVar {
		return types.Datum{}, ErrInvalidOp
	}
	id := val.GetGraphVar().ID
	var result types.Datum
	result.SetInt64(id)
	return result, nil
}

type labelFunctionClass struct {
	baseFunctionClass
}

func (c *labelFunctionClass) getFunction(args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	sig := &builtinLabelSig{newBaseBuiltinFunc(args)}
	return sig, nil
}

type builtinLabelSig struct {
	baseBuiltinFunc
}

func (s *builtinLabelSig) eval(ctx *stmtctx.Context, row Row) (types.Datum, error) {
	val, err := s.args[0].Eval(ctx, row)
	if err != nil {
		return types.Datum{}, err
	}
	if val.Kind() != types.KindGraphVar {
		return types.Datum{}, ErrInvalidOp
	}
	labels := val.GetGraphVar().Labels
	if len(labels) == 0 {
		return types.Datum{}, ErrNoLabel
	}
	if len(labels) > 1 {
		return types.Datum{}, ErrLabelMoreThanOne
	}
	var result types.Datum
	result.SetString(labels[0])
	return result, nil
}

type labelsFunctionClass struct {
	baseFunctionClass
}

func (c *labelsFunctionClass) getFunction(args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	sig := &builtinLabelsSig{newBaseBuiltinFunc(args)}
	return sig, nil
}

type builtinLabelsSig struct {
	baseBuiltinFunc
}

type builtinHasLabelSig struct {
	baseBuiltinFunc
}

func (s *builtinLabelsSig) eval(ctx *stmtctx.Context, row Row) (types.Datum, error) {
	return types.Datum{}, ErrFunctionNotImplemented
}
