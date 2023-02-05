// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package opcode

import (
	"github.com/vescale/zgraph/parser/format"
)

// Op is opcode type.
type Op int

// List operators.
const (
	LogicAnd Op = iota + 1
	LogicOr
	GE
	LE
	EQ
	NE
	LT
	GT
	Plus
	Minus
	And
	Or
	Mod
	Xor
	Div
	Mul
	Not
	IntDiv
	LogicXor
	NullEQ
	In
	Case
	Regexp
	IsNull
	IsTruth
	IsFalsity
	Concat
)

var ops = [...]struct {
	name      string
	literal   string
	isKeyword bool
}{
	LogicAnd: {
		name:      "and",
		literal:   "AND",
		isKeyword: true,
	},
	LogicOr: {
		name:      "or",
		literal:   "OR",
		isKeyword: true,
	},
	LogicXor: {
		name:      "xor",
		literal:   "XOR",
		isKeyword: true,
	},
	GE: {
		name:      "ge",
		literal:   ">=",
		isKeyword: false,
	},
	LE: {
		name:      "le",
		literal:   "<=",
		isKeyword: false,
	},
	EQ: {
		name:      "eq",
		literal:   "=",
		isKeyword: false,
	},
	NE: {
		name:      "ne",
		literal:   "<>",
		isKeyword: false,
	},
	LT: {
		name:      "lt",
		literal:   "<",
		isKeyword: false,
	},
	GT: {
		name:      "gt",
		literal:   ">",
		isKeyword: false,
	},
	Plus: {
		name:      "plus",
		literal:   "+",
		isKeyword: false,
	},
	Minus: {
		name:      "minus",
		literal:   "-",
		isKeyword: false,
	},
	And: {
		name:      "bitand",
		literal:   "&",
		isKeyword: false,
	},
	Or: {
		name:      "bitor",
		literal:   "|",
		isKeyword: false,
	},
	Mod: {
		name:      "mod",
		literal:   "%",
		isKeyword: false,
	},
	Xor: {
		name:      "bitxor",
		literal:   "^",
		isKeyword: false,
	},
	Div: {
		name:      "div",
		literal:   "/",
		isKeyword: false,
	},
	Mul: {
		name:      "mul",
		literal:   "*",
		isKeyword: false,
	},
	Not: {
		name:      "not",
		literal:   "not ",
		isKeyword: true,
	},
	IntDiv: {
		name:      "intdiv",
		literal:   "DIV",
		isKeyword: true,
	},
	NullEQ: {
		name:      "nulleq",
		literal:   "<=>",
		isKeyword: false,
	},
	In: {
		name:      "in",
		literal:   "IN",
		isKeyword: true,
	},
	Case: {
		name:      "case",
		literal:   "CASE",
		isKeyword: true,
	},
	Regexp: {
		name:      "regexp",
		literal:   "REGEXP",
		isKeyword: true,
	},
	IsNull: {
		name:      "isnull",
		literal:   "IS NULL",
		isKeyword: true,
	},
	IsTruth: {
		name:      "istrue",
		literal:   "IS TRUE",
		isKeyword: true,
	},
	IsFalsity: {
		name:      "isfalse",
		literal:   "IS FALSE",
		isKeyword: true,
	},
	Concat: {
		name:      "concat",
		literal:   "||",
		isKeyword: false,
	},
}

// String implements Stringer interface.
func (o Op) String() string {
	return ops[o].name
}

// IsKeyword returns whether the operator is a keyword.
func (o Op) IsKeyword() bool {
	return ops[o].isKeyword
}

// Restore the Op into a Writer
func (o Op) Restore(ctx *format.RestoreCtx) error {
	info := &ops[o]
	if info.isKeyword {
		ctx.WriteKeyWord(info.literal)
	} else {
		ctx.WritePlain(info.literal)
	}
	return nil
}
