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

package ast

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/parser/format"
	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/parser/opcode"
	"github.com/vescale/zgraph/types"
)

var (
	_ ExprNode = &ValueExpr{}
	_ ExprNode = &VariableReference{}
	_ ExprNode = &PropertyAccess{}
	_ ExprNode = &BindVariable{}
	_ ExprNode = &UnaryOperationExpr{}
	_ ExprNode = &BinaryOperationExpr{}
	_ ExprNode = &ParenthesesExpr{}
	_ FuncNode = &FuncCallExpr{}
	_ FuncNode = &SubstrFuncExpr{}
	_ FuncNode = &AggregateFuncExpr{}
	_ FuncNode = &ExtractFuncExpr{}
	_ ExprNode = &IsNullExpr{}
	_ FuncNode = &CastFuncExpr{}
	_ ExprNode = &CaseExpr{}
	_ ExprNode = &PatternInExpr{}
	_ ExprNode = &SubqueryExpr{}
	_ ExprNode = &ExistsSubqueryExpr{}

	_ Node = &WhenClause{}
)

// ValueExpr is the simple value expression.
type ValueExpr struct {
	exprNode

	types.Datum
}

// NewValueExpr creates a ValueExpr with value, and sets default field type.
func NewValueExpr(value interface{}) *ValueExpr {
	if ve, ok := value.(*ValueExpr); ok {
		return ve
	}
	ve := &ValueExpr{}
	ve.SetValue(value)
	return ve
}

// Restore implements Node interface.
func (n *ValueExpr) Restore(ctx *format.RestoreCtx) error {
	switch n.Kind() {
	case types.KindNull:
		ctx.WriteKeyWord("NULL")
	case types.KindBool:
		if n.GetBool() {
			ctx.WriteKeyWord("TRUE")
		} else {
			ctx.WriteKeyWord("FALSE")
		}
	case types.KindInt64:
		ctx.WritePlain(strconv.FormatInt(n.GetInt64(), 10))
	case types.KindUint64:
		ctx.WritePlain(strconv.FormatUint(n.GetUint64(), 10))
	case types.KindFloat64:
		ctx.WritePlain(strconv.FormatFloat(n.GetFloat64(), 'e', -1, 64))
	case types.KindString:
		ctx.WriteString(n.GetString())
	case types.KindBytes:
		ctx.WritePlain(fmt.Sprintf("X'%X'", n.GetBytes()))
	case types.KindDecimal:
		ctx.WritePlain(n.GetDecimal().String())
	case types.KindDate:
		ctx.WriteKeyWord("DATE ")
		ctx.WriteString(n.GetDate().String())
	case types.KindTime:
		ctx.WriteKeyWord("TIME ")
		ctx.WriteString(n.GetTime().String())
	case types.KindTimestamp:
		ctx.WriteKeyWord("TIMESTAMP ")
		ctx.WriteString(n.GetTimestamp().String())
	case types.KindInterval:
		ctx.WriteKeyWord("INTERVAL ")
		ctx.WriteString(n.GetInterval().StringValue())
		ctx.WritePlain(" ")
		ctx.WriteKeyWord(n.GetInterval().Field().String())
	default:
		return fmt.Errorf("can't format to string")
	}
	return nil
}

// GetDatumString implements the ValueExpr interface.
func (n *ValueExpr) GetDatumString() string {
	return n.GetString()
}

// Format the ExprNode into a Writer.
func (n *ValueExpr) Format(w io.Writer) {
	var s string
	switch n.Kind() {
	case types.KindNull:
		s = "NULL"
	case types.KindInt64:
		s = strconv.FormatInt(n.GetInt64(), 10)
	case types.KindFloat64:
		s = strconv.FormatFloat(n.GetFloat64(), 'e', -1, 64)
	case types.KindString, types.KindBytes:
		s = strconv.Quote(n.GetString())
	case types.KindDecimal:
		s = n.GetDecimal().String()
	default:
		panic("not implemented")
	}
	_, _ = fmt.Fprint(w, s)
}

// Accept implements Node interface.
func (n *ValueExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ValueExpr)
	return v.Leave(n)
}

type VariableReference struct {
	exprNode

	VariableName model.CIStr
}

func (n *VariableReference) Format(w io.Writer) {
	fmt.Fprint(w, n.VariableName.O)
}

func (n *VariableReference) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteName(n.VariableName.O)
	return nil
}

func (n *VariableReference) Accept(v Visitor) (node Node, ok bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

type PropertyAccess struct {
	exprNode

	VariableName model.CIStr
	PropertyName model.CIStr
}

func (n *PropertyAccess) Format(w io.Writer) {
	fmt.Print(w, n.VariableName.O)
	fmt.Fprint(w, ".")
	fmt.Fprint(w, n.PropertyName.O)
}

func (n *PropertyAccess) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteName(n.VariableName.O)
	ctx.WritePlain(".")
	ctx.WriteName(n.PropertyName.O)
	return nil
}

func (n *PropertyAccess) Accept(v Visitor) (node Node, ok bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

type BindVariable struct {
	exprNode
}

func (b *BindVariable) Restore(ctx *format.RestoreCtx) error {
	//TODO implement me
	panic("implement me")
}

func (b *BindVariable) Accept(v Visitor) (node Node, ok bool) {
	//TODO implement me
	panic("implement me")
}

func (b *BindVariable) Format(w io.Writer) {
	//TODO implement me
	panic("implement me")
}

// UnaryOperationExpr is the expression for unary operator.
type UnaryOperationExpr struct {
	exprNode
	// Op is the operator opcode.
	Op opcode.Op
	// V is the unary expression.
	V ExprNode
}

// Restore implements Node interface.
func (n *UnaryOperationExpr) Restore(ctx *format.RestoreCtx) error {
	if err := n.Op.Restore(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := n.V.Restore(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Format the ExprNode into a Writer.
func (n *UnaryOperationExpr) Format(w io.Writer) {
	n.Op.Format(w)
	n.V.Format(w)
}

// Accept implements Node Accept interface.
func (n *UnaryOperationExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*UnaryOperationExpr)
	node, ok := n.V.Accept(v)
	if !ok {
		return n, false
	}
	n.V = node.(ExprNode)
	return v.Leave(n)
}

// BinaryOperationExpr is for binary operation like `1 + 1`, `1 - 1`, etc.
type BinaryOperationExpr struct {
	exprNode
	// Op is the operator code for BinaryOperation.
	Op opcode.Op
	// L is the left expression in BinaryOperation.
	L ExprNode
	// R is the right expression in BinaryOperation.
	R ExprNode
}

func restoreBinaryOpWithSpacesAround(ctx *format.RestoreCtx, op opcode.Op) error {
	shouldInsertSpace := ctx.Flags.HasSpacesAroundBinaryOperationFlag() || op.IsKeyword()
	if shouldInsertSpace {
		ctx.WritePlain(" ")
	}
	if err := op.Restore(ctx); err != nil {
		return err // no need to annotate, the caller will annotate.
	}
	if shouldInsertSpace {
		ctx.WritePlain(" ")
	}
	return nil
}

// Restore implements Node interface.
func (n *BinaryOperationExpr) Restore(ctx *format.RestoreCtx) error {
	if ctx.Flags.HasRestoreBracketAroundBinaryOperation() {
		ctx.WritePlain("(")
	}
	if err := n.L.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred when restore BinaryOperationExpr.L")
	}
	if err := restoreBinaryOpWithSpacesAround(ctx, n.Op); err != nil {
		return errors.Annotate(err, "An error occurred when restore BinaryOperationExpr.Op")
	}
	if err := n.R.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred when restore BinaryOperationExpr.R")
	}
	if ctx.Flags.HasRestoreBracketAroundBinaryOperation() {
		ctx.WritePlain(")")
	}
	return nil
}

// Format the ExprNode into a Writer.
func (n *BinaryOperationExpr) Format(w io.Writer) {
	n.L.Format(w)
	fmt.Fprint(w, " ")
	n.Op.Format(w)
	fmt.Fprint(w, " ")
	n.R.Format(w)
}

// Accept implements Node interface.
func (n *BinaryOperationExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*BinaryOperationExpr)
	node, ok := n.L.Accept(v)
	if !ok {
		return n, false
	}
	n.L = node.(ExprNode)

	node, ok = n.R.Accept(v)
	if !ok {
		return n, false
	}
	n.R = node.(ExprNode)

	return v.Leave(n)
}

// ParenthesesExpr is the parentheses expression.
type ParenthesesExpr struct {
	exprNode
	// Expr is the expression in parentheses.
	Expr ExprNode
}

// Restore implements Node interface.
func (n *ParenthesesExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WritePlain("(")
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred when restore ParenthesesExpr.Exprs")
	}
	ctx.WritePlain(")")
	return nil
}

// Format the ExprNode into a Writer.
func (n *ParenthesesExpr) Format(w io.Writer) {
	fmt.Fprint(w, "(")
	n.Expr.Format(w)
	fmt.Fprint(w, ")")
}

// Accept implements Node Accept interface.
func (n *ParenthesesExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ParenthesesExpr)
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
	}
	return v.Leave(n)
}

type FuncCallExprType int8

const (
	FuncCallExprTypeKeyword FuncCallExprType = iota
	FuncCallExprTypeGeneric
)

// FuncCallExpr is for function expression.
type FuncCallExpr struct {
	funcNode

	Tp FuncCallExprType
	// FnName is the function name.
	FnName model.CIStr
	// Args is the function args.
	Args []ExprNode
}

// Restore implements Node interface.
func (n *FuncCallExpr) Restore(ctx *format.RestoreCtx) error {
	if n.Tp == FuncCallExprTypeGeneric {
		ctx.WriteName(n.FnName.O)
	} else {
		ctx.WriteKeyWord(n.FnName.O)
	}

	ctx.WritePlain("(")
	for i, argv := range n.Args {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := argv.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCallExpr.Args %d", i)
		}
	}
	ctx.WritePlain(")")
	return nil
}

// Format the ExprNode into a Writer.
func (n *FuncCallExpr) Format(w io.Writer) {
	fmt.Fprintf(w, "%s(", n.FnName.L)
	if !n.specialFormatArgs(w) {
		for i, arg := range n.Args {
			arg.Format(w)
			if i != len(n.Args)-1 {
				fmt.Fprint(w, ", ")
			}
		}
	}
	fmt.Fprint(w, ")")
}

// specialFormatArgs formats argument list for some special functions.
func (n *FuncCallExpr) specialFormatArgs(w io.Writer) bool {
	switch n.FnName.L {
	//case DateAdd, DateSub, AddDate, SubDate:
	//	n.Args[0].Format(w)
	//	fmt.Fprint(w, ", INTERVAL ")
	//	n.Args[1].Format(w)
	//	fmt.Fprint(w, " ")
	//	n.Args[2].Format(w)
	//	return true
	}
	return false
}

// Accept implements Node interface.
func (n *FuncCallExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncCallExpr)
	for i, val := range n.Args {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Args[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// SubstrFuncExpr is for function expression.
type SubstrFuncExpr struct {
	funcNode

	Expr  ExprNode
	Start ExprNode
	For   ExprNode
}

func (s *SubstrFuncExpr) Restore(ctx *format.RestoreCtx) error {
	//TODO implement me
	panic("implement me")
}

func (s *SubstrFuncExpr) Accept(v Visitor) (node Node, ok bool) {
	//TODO implement me
	panic("implement me")
}

func (s *SubstrFuncExpr) Format(w io.Writer) {
	//TODO implement me
	panic("implement me")
}

// AggregateFuncExpr represents aggregate function expression.
type AggregateFuncExpr struct {
	funcNode
	// F is the function name.
	F string
	// Args is the function args.
	Args []ExprNode
	// Distinct is true, function hence only aggregate distinct values.
	// For example, column c1 values are "1", "2", "2",  "sum(c1)" is "5",
	// but "sum(distinct c1)" is "3".
	Distinct bool
}

// Restore implements Node interface.
func (n *AggregateFuncExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(n.F)
	ctx.WritePlain("(")
	if n.Distinct {
		ctx.WriteKeyWord("DISTINCT ")
	}
	switch strings.ToLower(n.F) {
	case "listagg":
		for i := 0; i < len(n.Args)-1; i++ {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := n.Args[i].Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AggregateFuncExpr.Args[%d]", i)
			}
		}
		ctx.WritePlain(", ")
		if err := n.Args[len(n.Args)-1].Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AggregateFuncExpr.Args SEPARATOR")
		}
	default:
		for i, argv := range n.Args {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := argv.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AggregateFuncExpr.Args[%d]", i)
			}
		}
	}
	ctx.WritePlain(")")
	return nil
}

// Format the ExprNode into a Writer.
func (n *AggregateFuncExpr) Format(w io.Writer) {
	panic("Not implemented")
}

// Accept implements Node Accept interface.
func (n *AggregateFuncExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AggregateFuncExpr)
	for i, val := range n.Args {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Args[i] = node.(ExprNode)
	}

	return v.Leave(n)
}

type ExtractField byte

const (
	ExtractFieldYear ExtractField = iota
	ExtractFieldMonth
	ExtractFieldDay
	ExtractFieldHour
	ExtractFieldMinute
	ExtractFieldSecond
	ExtractFieldTimezoneHour
	ExtractFieldTimezoneMinute
)

// ExtractFuncExpr is for function expression.
type ExtractFuncExpr struct {
	funcNode

	ExtractField ExtractField
	Expr         ExprNode
}

func (e *ExtractFuncExpr) Restore(ctx *format.RestoreCtx) error {
	//TODO implement me
	panic("implement me")
}

func (e *ExtractFuncExpr) Accept(v Visitor) (node Node, ok bool) {
	//TODO implement me
	panic("implement me")
}

func (e *ExtractFuncExpr) Format(w io.Writer) {
	//TODO implement me
	panic("implement me")
}

// IsNullExpr is the expression for null check.
type IsNullExpr struct {
	exprNode
	// Expr is the expression to be checked.
	Expr ExprNode
	// Not is true, the expression is "is not null".
	Not bool
}

// Restore implements Node interface.
func (n *IsNullExpr) Restore(ctx *format.RestoreCtx) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Trace(err)
	}
	if n.Not {
		ctx.WriteKeyWord(" IS NOT NULL")
	} else {
		ctx.WriteKeyWord(" IS NULL")
	}
	return nil
}

// Format the ExprNode into a Writer.
func (n *IsNullExpr) Format(w io.Writer) {
	n.Expr.Format(w)
	if n.Not {
		fmt.Fprint(w, " IS NOT NULL")
		return
	}
	fmt.Fprint(w, " IS NULL")
}

// Accept implements Node Accept interface.
func (n *IsNullExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IsNullExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

type DataType uint8

const (
	DataTypeString DataType = iota
	DataTypeBoolean
	DataTypeInteger
	DataTypeFloat
	DataTypeDouble
	DataTypeDecimal
	DataTypeDate
	DataTypeTime
	DataTypeTimeWithTimeZone
	DataTypeTimestamp
	DataTypeTimestampWithTimeZone
)

func (f DataType) String() string {
	switch f {
	case DataTypeString:
		return "STRING"
	case DataTypeBoolean:
		return "BOOLEAN"
	case DataTypeInteger:
		return "INTEGER"
	case DataTypeFloat:
		return "FLOAT"
	case DataTypeDouble:
		return "DOUBLE"
	case DataTypeDecimal:
		return "DECIMAL"
	case DataTypeDate:
		return "DATE"
	case DataTypeTime:
		return "TIME"
	case DataTypeTimeWithTimeZone:
		return "TIME WITH TIME ZONE"
	case DataTypeTimestamp:
		return "TIMESTAMP"
	case DataTypeTimestampWithTimeZone:
		return "TIMESTAMP WITH TIME ZONE"
	default:
		return fmt.Sprintf("UNKNOWN<%d>", f)
	}
}

type CastFuncExpr struct {
	funcNode
	// Expr is the expression to be converted.
	Expr ExprNode
	// DataType is the conversion type.
	DataType DataType
}

// Restore implements Node interface.
func (n *CastFuncExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CAST")
	ctx.WritePlain("(")
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotatef(err, "An error occurred while restore CastFuncExpr.Exprs")
	}
	ctx.WriteKeyWord(" AS ")
	ctx.WriteKeyWord(n.DataType.String())
	ctx.WritePlain(")")
	return nil
}

// Format the ExprNode into a Writer.
func (n *CastFuncExpr) Format(w io.Writer) {
	fmt.Fprint(w, "CAST(")
	n.Expr.Format(w)
	fmt.Fprint(w, " AS ")
	fmt.Fprint(w, n.DataType.String())
	fmt.Fprint(w, ")")
}

// Accept implements Node Accept interface.
func (n *CastFuncExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CastFuncExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

// CaseExpr is the case expression.
type CaseExpr struct {
	exprNode
	// Value is the compare value expression.
	Value ExprNode
	// WhenClauses is the condition check expression.
	WhenClauses []*WhenClause
	// ElseClause is the else result expression.
	ElseClause ExprNode
}

// Restore implements Node interface.
func (n *CaseExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CASE")
	if n.Value != nil {
		ctx.WritePlain(" ")
		if err := n.Value.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CaseExpr.Value")
		}
	}
	for _, clause := range n.WhenClauses {
		ctx.WritePlain(" ")
		if err := clause.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CaseExpr.WhenClauses")
		}
	}
	if n.ElseClause != nil {
		ctx.WriteKeyWord(" ELSE ")
		if err := n.ElseClause.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CaseExpr.ElseClause")
		}
	}
	ctx.WriteKeyWord(" END")

	return nil
}

// Format the ExprNode into a Writer.
func (n *CaseExpr) Format(w io.Writer) {
	fmt.Fprint(w, "CASE")
	// Because the presence of `case when` syntax, `Value` could be nil and we need check this.
	if n.Value != nil {
		fmt.Fprint(w, " ")
		n.Value.Format(w)
	}
	for _, clause := range n.WhenClauses {
		fmt.Fprint(w, " ")
		fmt.Fprint(w, "WHEN ")
		clause.Expr.Format(w)
		fmt.Fprint(w, " THEN ")
		clause.Result.Format(w)
	}
	if n.ElseClause != nil {
		fmt.Fprint(w, " ELSE ")
		n.ElseClause.Format(w)
	}
	fmt.Fprint(w, " END")
}

// Accept implements Node Accept interface.
func (n *CaseExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*CaseExpr)
	if n.Value != nil {
		node, ok := n.Value.Accept(v)
		if !ok {
			return n, false
		}
		n.Value = node.(ExprNode)
	}
	for i, val := range n.WhenClauses {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.WhenClauses[i] = node.(*WhenClause)
	}
	if n.ElseClause != nil {
		node, ok := n.ElseClause.Accept(v)
		if !ok {
			return n, false
		}
		n.ElseClause = node.(ExprNode)
	}
	return v.Leave(n)
}

// WhenClause is the when clause in Case expression for "when condition then result".
type WhenClause struct {
	node
	// Expr is the condition expression in WhenClause.
	Expr ExprNode
	// Result is the result expression in WhenClause.
	Result ExprNode
}

// Restore implements Node interface.
func (n *WhenClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("WHEN ")
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore WhenClauses.Exprs")
	}
	ctx.WriteKeyWord(" THEN ")
	if err := n.Result.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore WhenClauses.Result")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *WhenClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*WhenClause)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)

	node, ok = n.Result.Accept(v)
	if !ok {
		return n, false
	}
	n.Result = node.(ExprNode)
	return v.Leave(n)
}

// PatternInExpr is the expression for in operator, like "expr in (1, 2, 3)" or "expr in (select c from t)".
type PatternInExpr struct {
	exprNode
	// Expr is the value expression to be compared.
	Expr ExprNode
	// List is the list expression in compare list.
	List []ExprNode
	// Not is true, the expression is "not in".
	Not bool
}

// Restore implements Node interface.
func (n *PatternInExpr) Restore(ctx *format.RestoreCtx) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PatternInExpr.Exprs")
	}
	if n.Not {
		ctx.WriteKeyWord(" NOT IN ")
	} else {
		ctx.WriteKeyWord(" IN ")
	}

	ctx.WritePlain("(")
	for i, expr := range n.List {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := expr.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore PatternInExpr.List[%d]", i)
		}
	}
	ctx.WritePlain(")")
	return nil
}

// Format the ExprNode into a Writer.
func (n *PatternInExpr) Format(w io.Writer) {
	n.Expr.Format(w)
	if n.Not {
		fmt.Fprint(w, " NOT IN (")
	} else {
		fmt.Fprint(w, " IN (")
	}
	for i, expr := range n.List {
		if i != 0 {
			fmt.Fprint(w, ",")
		}
		expr.Format(w)
	}
	fmt.Fprint(w, ")")
}

// Accept implements Node Accept interface.
func (n *PatternInExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PatternInExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	for i, val := range n.List {
		node, ok = val.Accept(v)
		if !ok {
			return n, false
		}
		n.List[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// SubqueryExpr represents a subquery.
type SubqueryExpr struct {
	exprNode
	// Query is the query SelectNode.
	Query      *SelectStmt
	Evaluated  bool
	Correlated bool
	MultiRows  bool
	Exists     bool
}

func (*SubqueryExpr) resultSet() {}

// Restore implements Node interface.
func (n *SubqueryExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WritePlain("(")
	if err := n.Query.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore SubqueryExpr.Query")
	}
	ctx.WritePlain(")")
	return nil
}

// Format the ExprNode into a Writer.
func (n *SubqueryExpr) Format(w io.Writer) {
	panic("Not implemented")
}

// Accept implements Node Accept interface.
func (n *SubqueryExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SubqueryExpr)
	//node, ok := n.Query.Accept(v)
	//if !ok {
	//	return n, false
	//}
	//n.Query = node.(ResultSetNode)
	return v.Leave(n)
}

// ExistsSubqueryExpr is the expression for "exists (select ...)".
// See https://dev.mysql.com/doc/refman/5.7/en/exists-and-not-exists-subqueries.html
type ExistsSubqueryExpr struct {
	exprNode
	// Sel is the subquery, may be rewritten to other type of expression.
	Sel ExprNode
	// Not is true, the expression is "not exists".
	Not bool
}

// Restore implements Node interface.
func (n *ExistsSubqueryExpr) Restore(ctx *format.RestoreCtx) error {
	if n.Not {
		ctx.WriteKeyWord("NOT EXISTS ")
	} else {
		ctx.WriteKeyWord("EXISTS ")
	}
	if err := n.Sel.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore ExistsSubqueryExpr.Sel")
	}
	return nil
}

// Format the ExprNode into a Writer.
func (n *ExistsSubqueryExpr) Format(w io.Writer) {
	panic("Not implemented")
}

// Accept implements Node Accept interface.
func (n *ExistsSubqueryExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExistsSubqueryExpr)
	node, ok := n.Sel.Accept(v)
	if !ok {
		return n, false
	}
	n.Sel = node.(ExprNode)
	return v.Leave(n)
}
