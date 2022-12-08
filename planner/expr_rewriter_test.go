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

package planner_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph/expression"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/parser/types"
	"github.com/vescale/zgraph/planner"
)

func TestRewriteExpr(t *testing.T) {
	cases := []struct {
		expr   ast.ExprNode
		expect expression.Expression
	}{
		{
			expr:   &ast.ValueExpr{Datum: types.NewDatum(1)},
			expect: &expression.Constant{Value: types.NewDatum(1)},
		},
	}

	for _, c := range cases {
		expr, err := planner.RewriteExpr(c.expr)
		assert.Nil(t, err)
		assert.Equal(t, c.expect, expr)
	}
}
