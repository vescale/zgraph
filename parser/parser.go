// Copyright 2022 zGraph Authors. All rights reserved.
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

package parser

import (
	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/parser/ast"
)

type Parser struct {
	src    string
	lexer  *Lexer
	result []ast.StmtNode

	// the following fields are used by yyParse to reduce allocation.
	cache  []yySymType
	yylval yySymType
	yyVAL  *yySymType
}

func yySetOffset(yyVAL *yySymType, offset int) {
	if yyVAL.expr != nil {
		yyVAL.expr.SetOriginTextPosition(offset)
	}
}

func New() *Parser {
	return &Parser{
		lexer: NewLexer(""),
	}
}

func (p *Parser) Parse(sql string) (stmts []ast.StmtNode, warns []error, err error) {
	p.lexer.reset(sql)
	p.src = sql
	p.result = p.result[:0]
	yyParse(p.lexer, p)

	warns, errs := p.lexer.Errors()
	if len(warns) > 0 {
		warns = append([]error(nil), warns...)
	} else {
		warns = nil
	}
	if len(errs) != 0 {
		return nil, warns, errors.Trace(errs[0])
	}
	return p.result, warns, nil
}
