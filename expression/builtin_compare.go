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
	"bytes"
	"strings"

	"github.com/vescale/zgraph/parser/opcode"
	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/types"
)

var (
	_ functionClass = &compareFunctionClass{}
)

var (
	_ builtinFunc = &builtinEQSig{}
)

type compareFunctionClass struct {
	baseFunctionClass

	op opcode.Op
}

func (c *compareFunctionClass) getFunction(args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	switch c.op {
	case opcode.EQ:
		return &builtinEQSig{newBaseBuiltinFunc(args)}, nil
	default:
		return nil, ErrFunctionNotExists
	}
}

type builtinEQSig struct {
	baseBuiltinFunc
}

func (s *builtinEQSig) eval(ctx *stmtctx.Context, row Row) (types.Datum, error) {
	a, err := s.args[0].Eval(ctx, row)
	if err != nil {
		return types.Datum{}, err
	}
	b, err := s.args[1].Eval(ctx, row)
	if err != nil {
		return types.Datum{}, err
	}

	var result types.Datum
	if a.IsNull() || b.IsNull() {
		result.SetBool(false)
		return result, nil
	}
	cmp, ok := compareDatum(a, b)
	if ok && cmp == 0 {
		result.SetBool(true)
	} else {
		result.SetBool(false)
	}
	return result, nil
}

func compareDatum(a, b types.Datum) (result int, ok bool) {
	switch a.Kind() {
	case types.KindInt64:
		switch b.Kind() {
		case types.KindInt64:
			return compareInt64(a.GetInt64(), b.GetInt64()), true
		case types.KindUint64:
			return compareInt64AndUint64(a.GetInt64(), b.GetUint64()), true
		}
	case types.KindUint64:
		switch b.Kind() {
		case types.KindInt64:
			return -compareInt64AndUint64(b.GetInt64(), a.GetUint64()), true
		case types.KindUint64:
			return compareUint64(a.GetUint64(), b.GetUint64()), true
		}
	case types.KindString:
		if b.Kind() == types.KindString {
			return strings.Compare(a.GetString(), b.GetString()), true
		}
	case types.KindBytes:
		if b.Kind() == types.KindBytes {
			return bytes.Compare(a.GetBytes(), b.GetBytes()), true
		}
	}
	return 0, false
}

func compareInt64(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareInt64AndUint64(a int64, b uint64) int {
	if a < 0 {
		return -1
	}
	if uint64(a) < b {
		return -1
	}
	if uint64(a) > b {
		return 1
	}
	return 0
}

func compareUint64(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
