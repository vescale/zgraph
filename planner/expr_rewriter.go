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

package planner

import (
	"fmt"

	"github.com/vescale/zgraph/expression"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/parser/model"
	"golang.org/x/exp/slices"
)

type exprRewriter struct {
	p        LogicalPlan
	ctxStack []expression.Expression
	err      error
}

func RewriteExpr(expr ast.ExprNode, p LogicalPlan) (expression.Expression, error) {
	rewriter := &exprRewriter{
		p: p,
	}
	expr.Accept(rewriter)
	if rewriter.err != nil {
		return nil, rewriter.err
	}
	return rewriter.ctxStack[0], nil
}

// Enter implements the ast.Visitor interface.
func (er *exprRewriter) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch expr := n.(type) {
	case *ast.ValueExpr:
		_ = expr
	}
	return n, false
}

// Leave implements the ast.Visitor interface.
func (er *exprRewriter) Leave(n ast.Node) (node ast.Node, ok bool) {
	if er.err != nil {
		return node, false
	}

	switch expr := n.(type) {
	case *ast.ValueExpr:
		er.ctxStackAppend(&expression.Constant{Value: expr.Datum})
	case *ast.BinaryOperationExpr:
		lExpr := er.ctxStack[er.ctxStackLen()-2]
		rExpr := er.ctxStack[er.ctxStackLen()-1]
		er.ctxStackPop(2)
		opFunc, err := expression.NewFunction(expr.Op.String(), lExpr, rExpr)
		if err != nil {
			er.err = err
			return n, true
		}
		er.ctxStackAppend(opFunc)
	case *ast.VariableReference:
		idx := slices.IndexFunc(er.p.OutputNames(), func(name model.CIStr) bool {
			return expr.VariableName.Equal(name)
		})
		if idx == -1 {
			er.err = fmt.Errorf("unresolved variable %s", expr.VariableName)
			return n, true
		}
		er.ctxStackAppend(er.p.Schema().Columns[idx])
	case *ast.PropertyAccess:
		idx := slices.IndexFunc(er.p.OutputNames(), func(name model.CIStr) bool {
			return expr.VariableName.Equal(name)
		})
		if idx == -1 {
			er.err = fmt.Errorf("unresolved variable %s", expr.VariableName)
			return n, true
		}
		col := er.p.Schema().Columns[idx]
		er.ctxStackAppend(&expression.PropertyAccess{
			Column: col,
			VariableRef: &expression.VariableRef{
				Name: expr.VariableName,
			},
			PropertyRef: &expression.PropertyRef{
				Property: &model.PropertyInfo{
					ID:   0, // FIXME: Set correct ID
					Name: expr.PropertyName,
				},
			},
		})
	}

	return n, true
}

func (er *exprRewriter) ctxStackLen() int {
	return len(er.ctxStack)
}

func (er *exprRewriter) ctxStackPop(num int) {
	l := er.ctxStackLen()
	er.ctxStack = er.ctxStack[:l-num]
}

func (er *exprRewriter) ctxStackAppend(col expression.Expression) {
	er.ctxStack = append(er.ctxStack, col)
}
