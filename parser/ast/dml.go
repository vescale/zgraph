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

	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/parser/format"
	"github.com/vescale/zgraph/parser/model"
)

var (
	_ Node = &InsertStmt{}
	_ Node = &DeleteStmt{}
	_ Node = &UpdateStmt{}
	_ Node = &SelectStmt{}
	_ Node = &PathPattern{}
	_ Node = &VariableSpec{}
	_ Node = &VertexPattern{}
	_ Node = &ReachabilityPathExpr{}
	_ Node = &PatternQuantifier{}
	_ Node = &PathPatternMacro{}

	_ Node = &GraphElementInsertion{}
	_ Node = &LabelsAndProperties{}
	_ Node = &PropertyAssignment{}
	_ Node = &GraphElementUpdate{}
	_ Node = &SelectElement{}
	_ Node = &ExpAsVar{}
	_ Node = &ByItem{}
	_ Node = &SelectClause{}
	_ Node = &GroupByClause{}
	_ Node = &HavingClause{}
	_ Node = &OrderByClause{}
	_ Node = &LimitClause{}

	_ ResultSetNode = &MatchClause{}
	_ ResultSetNode = &MatchClauseList{}

	_ VertexPairConnection = &EdgePattern{}
	_ VertexPairConnection = &ReachabilityPathExpr{}
	_ VertexPairConnection = &QuantifiedPathExpr{}
)

type InsertionType byte

const (
	InsertionTypeVertex InsertionType = 1
	InsertionTypeEdge   InsertionType = 2
)

// String implements the fmt.Stringer interface
func (it InsertionType) String() string {
	switch it {
	case InsertionTypeVertex:
		return "VERTEX"
	case InsertionTypeEdge:
		return "EDGE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", it)
	}
}

type GraphElementInsertion struct {
	node

	InsertionType       InsertionType
	VariableName        model.CIStr
	From                model.CIStr
	To                  model.CIStr
	LabelsAndProperties *LabelsAndProperties
}

func (g *GraphElementInsertion) Restore(ctx *format.RestoreCtx) error {
	switch g.InsertionType {
	case InsertionTypeVertex:
		ctx.WriteKeyWord("VERTEX")
		if g.VariableName.O != "" {
			ctx.WritePlain(" ")
			ctx.WriteName(g.VariableName.O)
		}
	case InsertionTypeEdge:
		ctx.WriteKeyWord("EDGE")
		if g.VariableName.O != "" {
			ctx.WritePlain(" ")
			ctx.WriteName(g.VariableName.O)
		}
		ctx.WritePlain(" BETWEEN ")
		ctx.WriteName(g.From.O)
		ctx.WriteKeyWord(" AND ")
		ctx.WriteName(g.To.O)
	}
	if g.LabelsAndProperties != nil {
		if err := g.LabelsAndProperties.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore GraphElementInsertion.LabelsAndProperties")
		}
	}
	return nil
}

func (g *GraphElementInsertion) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(g)
	if skipChildren {
		return v.Leave(newNode)
	}
	g = newNode.(*GraphElementInsertion)
	if g.LabelsAndProperties != nil {
		node, ok = g.LabelsAndProperties.Accept(v)
		if !ok {
			return node, ok
		}
		g.LabelsAndProperties = node.(*LabelsAndProperties)
	}
	return v.Leave(g)
}

type LabelsAndProperties struct {
	node

	Labels      []model.CIStr
	Assignments []*PropertyAssignment
}

func (l *LabelsAndProperties) Restore(ctx *format.RestoreCtx) error {
	if len(l.Labels) > 0 {
		ctx.WriteKeyWord(" LABELS ")
		ctx.WritePlain("(")
		for i, label := range l.Labels {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(label.O)
		}
		ctx.WritePlain(")")
	}
	if len(l.Assignments) > 0 {
		ctx.WritePlain(" PROPERTIES ")
		ctx.WritePlain("(")
		for i, assignment := range l.Assignments {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			if err := assignment.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore LabelsAndProperties.Assignments")
			}
		}
		ctx.WritePlain(")")
	}
	return nil
}

func (l *LabelsAndProperties) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(l)
	if skipChildren {
		return v.Leave(newNode)
	}
	l = newNode.(*LabelsAndProperties)
	for i, assignment := range l.Assignments {
		node, ok = assignment.Accept(v)
		if !ok {
			return node, ok
		}
		l.Assignments[i] = node.(*PropertyAssignment)
	}
	return v.Leave(l)
}

type PropertyAssignment struct {
	node

	PropertyAccess  *PropertyAccess
	ValueExpression ExprNode
}

func (p *PropertyAssignment) Restore(ctx *format.RestoreCtx) error {
	if err := p.PropertyAccess.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PropertyAssignment.PropertyAccess")
	}
	ctx.WritePlain(" = ")
	if err := p.ValueExpression.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PropertyAssignment.ValueExpression")
	}
	return nil
}

func (p *PropertyAssignment) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(p)
	if skipChildren {
		return v.Leave(newNode)
	}
	p = newNode.(*PropertyAssignment)
	node, ok = p.PropertyAccess.Accept(v)
	if !ok {
		return node, ok
	}
	p.PropertyAccess = node.(*PropertyAccess)
	node, ok = p.ValueExpression.Accept(v)
	if !ok {
		return node, ok
	}
	p.ValueExpression = node.(ExprNode)
	return v.Leave(p)
}

type GraphElementUpdate struct {
	dmlNode

	VariableName model.CIStr
	Assignments  []*PropertyAssignment
}

func (g *GraphElementUpdate) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteName(g.VariableName.O)
	ctx.WriteKeyWord(" SET ")
	ctx.WritePlain("(")
	for i, assignment := range g.Assignments {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		if err := assignment.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore GraphElementUpdate.Assignments")
		}
	}
	ctx.WritePlain(")")
	return nil
}

func (g *GraphElementUpdate) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(g)
	if skipChildren {
		return v.Leave(newNode)
	}
	g = newNode.(*GraphElementUpdate)
	for i, assignment := range g.Assignments {
		node, ok = assignment.Accept(v)
		if !ok {
			return node, ok
		}
		g.Assignments[i] = node.(*PropertyAssignment)
	}
	return v.Leave(g)
}

type ExpAsVar struct {
	node

	Expr   ExprNode
	AsName model.CIStr
}

func (e *ExpAsVar) Restore(ctx *format.RestoreCtx) error {
	if err := e.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore ByItem.Expr")
	}
	if e.AsName.O != "" {
		ctx.WriteKeyWord(" AS ")
		ctx.WriteName(e.AsName.String())
	}
	return nil
}

func (e *ExpAsVar) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(e)
	if skipChildren {
		return v.Leave(newNode)
	}
	n := newNode.(*ExpAsVar)
	nn, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = nn.(ExprNode)
	return v.Leave(n)
}

// ByItem represents an item in order by or group by.
type ByItem struct {
	node

	Expr      *ExpAsVar
	Desc      bool
	NullOrder bool
}

// Restore implements Node interface.
func (n *ByItem) Restore(ctx *format.RestoreCtx) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore ByItem.Expr")
	}
	if n.Desc {
		ctx.WriteKeyWord(" DESC")
	} else if !n.NullOrder {
		ctx.WriteKeyWord(" ASC")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ByItem) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ByItem)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(*ExpAsVar)
	return v.Leave(n)
}

type GroupByClause struct {
	node
	Items []*ByItem
}

// Restore implements Node interface.
func (n *GroupByClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("GROUP BY ")
	for i, v := range n.Items {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore GroupByClause.Items[%d]", i)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *GroupByClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*GroupByClause)
	for i, val := range n.Items {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Items[i] = node.(*ByItem)
	}
	return v.Leave(n)
}

// HavingClause represents having clause.
type HavingClause struct {
	node
	Expr ExprNode
}

// Restore implements Node interface.
func (n *HavingClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("HAVING ")
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore HavingClause.Expr")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *HavingClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*HavingClause)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

// OrderByClause represents order by clause.
type OrderByClause struct {
	node
	Items []*ByItem
}

// Restore implements Node interface.
func (n *OrderByClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ORDER BY ")
	for i, item := range n.Items {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := item.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore OrderByClause.Items[%d]", i)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *OrderByClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*OrderByClause)
	for i, val := range n.Items {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Items[i] = node.(*ByItem)
	}
	return v.Leave(n)
}

// LimitClause is the limit clause.
type LimitClause struct {
	node

	Count  ExprNode
	Offset ExprNode
}

// Restore implements Node interface.
func (n *LimitClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("LIMIT ")
	if n.Offset != nil {
		if err := n.Offset.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore LimitClause.Offset")
		}
		ctx.WritePlain(",")
	}
	if err := n.Count.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore LimitClause.Count")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *LimitClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	if n.Count != nil {
		node, ok := n.Count.Accept(v)
		if !ok {
			return n, false
		}
		n.Count = node.(ExprNode)
	}
	if n.Offset != nil {
		node, ok := n.Offset.Accept(v)
		if !ok {
			return n, false
		}
		n.Offset = node.(ExprNode)
	}

	n = newNode.(*LimitClause)
	return v.Leave(n)
}

type InsertStmt struct {
	dmlNode

	PathPatternMacros []*PathPatternMacro
	IntoGraphName     model.CIStr
	Insertions        []*GraphElementInsertion

	// Full modify query
	// ref: https://pgql-lang.org/spec/1.5/#insert
	From    *MatchClauseList
	Where   ExprNode
	GroupBy *GroupByClause
	Having  *HavingClause
	OrderBy *OrderByClause
	Limit   *LimitClause
}

func (n *InsertStmt) Restore(ctx *format.RestoreCtx) error {
	for _, p := range n.PathPatternMacros {
		if err := p.Restore(ctx); err != nil {
			return errors.New("An error occurred while restore SelectStmt.PathPatternMacros")
		}
		ctx.WritePlain(" ")
	}
	ctx.WriteKeyWord("INSERT ")
	if n.IntoGraphName.L != "" {
		ctx.WriteKeyWord("INTO ")
		ctx.WriteName(n.IntoGraphName.O)
		ctx.WritePlain(" ")
	}

	for i, in := range n.Insertions {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := in.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore InsertStmt.Insertions[%d]", i)
		}
	}

	if n.From != nil {
		ctx.WriteKeyWord(" FROM ")
		if err := n.From.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore InsertStmt.From")
		}
	}

	if n.Where != nil {
		ctx.WriteKeyWord(" WHERE ")
		if err := n.Where.Restore(ctx); err != nil {
			return errors.New("An error occurred while restore InsertStmt.Where")
		}
	}
	if n.GroupBy != nil {
		ctx.WritePlain(" ")
		if err := n.GroupBy.Restore(ctx); err != nil {
			return errors.New("An error occurred while restore InsertStmt.GroupBy")
		}
	}
	if n.Having != nil {
		ctx.WritePlain(" ")
		if err := n.Having.Restore(ctx); err != nil {
			return errors.New("An error occurred while restore InsertStmt.Having")
		}
	}
	if n.OrderBy != nil {
		ctx.WritePlain(" ")
		if err := n.OrderBy.Restore(ctx); err != nil {
			return errors.New("An error occurred while restore InsertStmt.OrderBy")
		}
	}
	if n.Limit != nil {
		ctx.WritePlain(" ")
		if err := n.Limit.Restore(ctx); err != nil {
			return errors.New("An error occurred while restore InsertStmt.Limit")
		}
	}
	return nil
}

func (n *InsertStmt) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*InsertStmt)
	for i, p := range n.PathPatternMacros {
		node, ok = p.Accept(v)
		if !ok {
			return n, false
		}
		n.PathPatternMacros[i] = node.(*PathPatternMacro)
	}
	for i, in := range n.Insertions {
		node, ok = in.Accept(v)
		if !ok {
			return n, false
		}
		n.Insertions[i] = node.(*GraphElementInsertion)
	}
	if n.From != nil {
		node, ok = n.From.Accept(v)
		if !ok {
			return n, false
		}
		n.From = node.(*MatchClauseList)
	}
	if n.Where != nil {
		node, ok = n.Where.Accept(v)
		if !ok {
			return n, false
		}
		n.Where = node.(ExprNode)
	}
	if n.GroupBy != nil {
		node, ok = n.GroupBy.Accept(v)
		if !ok {
			return n, false
		}
		n.GroupBy = node.(*GroupByClause)
	}
	if n.Having != nil {
		node, ok = n.Having.Accept(v)
		if !ok {
			return n, false
		}
		n.Having = node.(*HavingClause)
	}
	if n.OrderBy != nil {
		node, ok = n.OrderBy.Accept(v)
		if !ok {
			return n, false
		}
		n.OrderBy = node.(*OrderByClause)
	}
	if n.Limit != nil {
		node, ok = n.Limit.Accept(v)
		if !ok {
			return n, false
		}
		n.Limit = node.(*LimitClause)
	}
	return v.Leave(n)
}

type DeleteStmt struct {
	dmlNode

	PathPatternMacros []*PathPatternMacro
	VariableNames     []model.CIStr
	From              *MatchClauseList
	Where             ExprNode
	GroupBy           *GroupByClause
	Having            *HavingClause
	OrderBy           *OrderByClause
	Limit             *LimitClause
}

func (d *DeleteStmt) Restore(ctx *format.RestoreCtx) error {
	if len(d.PathPatternMacros) > 0 {
		for i, macro := range d.PathPatternMacros {
			if i != 0 {
				ctx.WritePlain(" ")
			}
			if err := macro.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore DeleteStmt.PathPatternMacros[%d]", i)
			}
		}
	}
	ctx.WriteKeyWord("DELETE ")

	for i, name := range d.VariableNames {
		if i != 0 {
			ctx.WritePlain(",")
		}
		ctx.WriteName(name.O)
	}

	if d.From != nil {
		ctx.WriteKeyWord(" FROM ")
		if err := d.From.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore DeleteStmt.From")
		}
	}
	if d.Where != nil {
		ctx.WriteKeyWord(" WHERE ")
		if err := d.Where.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DeleteStmt.Where")
		}
	}
	if d.GroupBy != nil {
		if err := d.GroupBy.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DeleteStmt.GroupBy")
		}
	}
	if d.Having != nil {
		if err := d.Having.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DeleteStmt.Having")
		}
	}
	if d.OrderBy != nil {
		if err := d.OrderBy.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DeleteStmt.OrderBy")
		}
	}
	if d.Limit != nil {
		if err := d.Limit.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DeleteStmt.Limit")
		}
	}

	return nil
}

func (d *DeleteStmt) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(d)
	if skipChildren {
		return v.Leave(newNode)
	}

	n := newNode.(*DeleteStmt)
	if len(n.PathPatternMacros) > 0 {
		for i, macro := range n.PathPatternMacros {
			nn, ok := macro.Accept(v)
			if !ok {
				return n, false
			}
			n.PathPatternMacros[i] = nn.(*PathPatternMacro)
		}
	}

	nn, ok := d.From.Accept(v)
	if !ok {
		return n, false
	}
	n.From = nn.(*MatchClauseList)
	if n.Where != nil {
		nn, ok := d.Where.Accept(v)
		if !ok {
			return n, false
		}
		n.Where = nn.(ExprNode)
	}
	if n.GroupBy != nil {
		nn, ok := d.GroupBy.Accept(v)
		if !ok {
			return n, false
		}
		n.GroupBy = nn.(*GroupByClause)
	}
	if n.Having != nil {
		nn, ok := d.Having.Accept(v)
		if !ok {
			return n, false
		}
		n.Having = nn.(*HavingClause)
	}
	if n.OrderBy != nil {
		nn, ok := d.OrderBy.Accept(v)
		if !ok {
			return n, false
		}
		n.OrderBy = nn.(*OrderByClause)
	}
	if n.Limit != nil {
		nn, ok := d.Limit.Accept(v)
		if !ok {
			return n, false
		}
		n.Limit = nn.(*LimitClause)
	}
	return v.Leave(n)
}

type UpdateStmt struct {
	dmlNode

	PathPatternMacros []*PathPatternMacro
	Updates           []*GraphElementUpdate

	// Full modify query
	// ref: https://pgql-lang.org/spec/1.5/#insert
	From    *MatchClauseList
	Where   ExprNode
	GroupBy *GroupByClause
	Having  *HavingClause
	OrderBy *OrderByClause
	Limit   *LimitClause
}

func (n *UpdateStmt) Restore(ctx *format.RestoreCtx) error {
	for _, p := range n.PathPatternMacros {
		if err := p.Restore(ctx); err != nil {
			return errors.New("An error occurred while restore UpdateStmt.PathPatternMacros")
		}
		ctx.WritePlain(" ")
	}
	ctx.WriteKeyWord("UPDATE ")
	for i, update := range n.Updates {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := update.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore UpdateStmt.Updates[%d]", i)
		}
	}
	if n.From != nil {
		ctx.WriteKeyWord(" FROM ")
		if err := n.From.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore UpdateStmt.From")
		}
	}
	if n.Where != nil {
		ctx.WriteKeyWord(" WHERE ")
		if err := n.Where.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore UpdateStmt.Where")
		}
	}
	if n.GroupBy != nil {
		ctx.WritePlain(" ")
		if err := n.GroupBy.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore UpdateStmt.GroupBy")
		}
	}
	if n.Having != nil {
		ctx.WritePlain(" ")
		if err := n.Having.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore UpdateStmt.Having")
		}
	}
	if n.OrderBy != nil {
		ctx.WritePlain(" ")
		if err := n.OrderBy.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore UpdateStmt.OrderBy")
		}
	}
	if n.Limit != nil {
		ctx.WritePlain(" ")
		if err := n.Limit.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore UpdateStmt.Limit")
		}
	}
	return nil
}

func (n *UpdateStmt) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*UpdateStmt)
	for i, p := range n.PathPatternMacros {
		node, ok = p.Accept(v)
		if !ok {
			return n, false
		}
		n.PathPatternMacros[i] = node.(*PathPatternMacro)
	}
	for i, u := range n.Updates {
		node, ok = u.Accept(v)
		if !ok {
			return n, false
		}
		n.Updates[i] = node.(*GraphElementUpdate)
	}
	if n.From != nil {
		node, ok = n.From.Accept(v)
		if !ok {
			return n, false
		}
		n.From = node.(*MatchClauseList)
	}
	if n.Where != nil {
		node, ok = n.Where.Accept(v)
		if !ok {
			return n, false
		}
		n.Where = node.(ExprNode)
	}
	if n.GroupBy != nil {
		node, ok = n.GroupBy.Accept(v)
		if !ok {
			return n, false
		}
		n.GroupBy = node.(*GroupByClause)
	}
	if n.Having != nil {
		node, ok = n.Having.Accept(v)
		if !ok {
			return n, false
		}
		n.Having = node.(*HavingClause)
	}
	if n.OrderBy != nil {
		node, ok = n.OrderBy.Accept(v)
		if !ok {
			return n, false
		}
		n.OrderBy = node.(*OrderByClause)
	}
	if n.Limit != nil {
		node, ok = n.Limit.Accept(v)
		if !ok {
			return n, false
		}
		n.Limit = node.(*LimitClause)
	}
	return v.Leave(n)
}

// SelectElement represents a result field which can be a property from a label,
// or an expression in select field. It is a generated property during
// binding process. SelectElement is the key element to evaluate a PropertyNameExpr.
type SelectElement struct {
	node

	ExpAsVar *ExpAsVar

	// All Properties with optional prefix
	Identifier string
	Prefix     string
}

func (n *SelectElement) Restore(ctx *format.RestoreCtx) error {
	if n.ExpAsVar != nil {
		if err := n.ExpAsVar.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore SelectElement.ExpAsVar")
		}
		return nil
	}

	ctx.WritePlain(n.Identifier + ".*")
	if n.Prefix != "" {
		ctx.WriteKeyWord(" PREFIX ")
		ctx.WritePlain(n.Prefix)
	}
	return nil
}

func (n *SelectElement) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	nn := newNode.(*SelectElement)
	if nn.ExpAsVar != nil {
		node, ok := nn.ExpAsVar.Accept(v)
		if !ok {
			return nn, false
		}
		nn.ExpAsVar = node.(*ExpAsVar)
	}
	return v.Leave(nn)
}

type SelectClause struct {
	node

	Star     bool
	Distinct bool
	Elements []*SelectElement
}

func (n *SelectClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SELECT ")
	if n.Star {
		ctx.WritePlain("*")
		return nil
	}
	if n.Distinct {
		ctx.WriteKeyWord("DISTINCT ")
	}
	for i, e := range n.Elements {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := e.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore SelectClause.Elements[%d]", i)
		}
	}
	return nil
}

func (n *SelectClause) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	nn := newNode.(*SelectClause)
	for i, e := range nn.Elements {
		node, ok := e.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Elements[i] = node.(*SelectElement)
	}
	return v.Leave(nn)
}

type SelectStmt struct {
	dmlNode

	PathPatternMacros []*PathPatternMacro
	Select            *SelectClause
	From              *MatchClauseList
	Where             ExprNode
	GroupBy           *GroupByClause
	Having            *HavingClause
	OrderBy           *OrderByClause
	Limit             *LimitClause
}

func (n *SelectStmt) Restore(ctx *format.RestoreCtx) error {
	for _, p := range n.PathPatternMacros {
		if err := p.Restore(ctx); err != nil {
			return errors.New("An error occurred while restore SelectStmt.PathPatternMacros")
		}
		ctx.WritePlain(" ")
	}
	if err := n.Select.Restore(ctx); err != nil {
		return errors.New("An error occurred while restore SelectStmt.Select")
	}
	ctx.WriteKeyWord(" FROM ")
	if err := n.From.Restore(ctx); err != nil {
		return errors.New("An error occurred while restore SelectStmt.From")
	}
	if n.Where != nil {
		ctx.WriteKeyWord(" WHERE ")
		if err := n.Where.Restore(ctx); err != nil {
			return errors.New("An error occurred while restore SelectStmt.Where")
		}
	}
	if n.GroupBy != nil {
		ctx.WritePlain(" ")
		if err := n.GroupBy.Restore(ctx); err != nil {
			return errors.New("An error occurred while restore SelectStmt.GroupBy")
		}
	}
	if n.Having != nil {
		ctx.WritePlain(" ")
		if err := n.Having.Restore(ctx); err != nil {
			return errors.New("An error occurred while restore SelectStmt.Having")
		}
	}
	if n.OrderBy != nil {
		ctx.WritePlain(" ")
		if err := n.OrderBy.Restore(ctx); err != nil {
			return errors.New("An error occurred while restore SelectStmt.OrderBy")
		}
	}
	if n.Limit != nil {
		ctx.WritePlain(" ")
		if err := n.Limit.Restore(ctx); err != nil {
			return errors.New("An error occurred while restore SelectStmt.Limit")
		}
	}
	return nil
}

func (n *SelectStmt) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*SelectStmt)

	for i, p := range nn.PathPatternMacros {
		node, ok := p.Accept(v)
		if !ok {
			return nn, false
		}
		nn.PathPatternMacros[i] = node.(*PathPatternMacro)
	}

	if nn.Where != nil {
		node, ok := nn.Where.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Where = node.(ExprNode)
	}

	if nn.GroupBy != nil {
		node, ok := nn.GroupBy.Accept(v)
		if !ok {
			return nn, false
		}
		nn.GroupBy = node.(*GroupByClause)
	}

	if nn.Having != nil {
		node, ok := nn.Having.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Having = node.(*HavingClause)
	}

	if nn.OrderBy != nil {
		node, ok := nn.OrderBy.Accept(v)
		if !ok {
			return nn, false
		}
		nn.OrderBy = node.(*OrderByClause)
	}

	if nn.Limit != nil {
		node, ok := nn.Limit.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Limit = node.(*LimitClause)
	}

	return v.Leave(nn)
}

type MatchClauseList struct {
	node

	Matches []*MatchClause
}

func (n *MatchClauseList) resultSet() {}

func (n *MatchClauseList) Restore(ctx *format.RestoreCtx) error {
	for i, m := range n.Matches {
		if i > 0 {
			ctx.WritePlain(",")
		}
		if err := m.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore MatchClauseList.Matches[%d]", i)
		}
	}
	return nil
}

func (n *MatchClauseList) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*MatchClauseList)
	for i, m := range nn.Matches {
		node, ok := m.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Matches[i] = node.(*MatchClause)
	}
	return v.Leave(nn)
}

type MatchClause struct {
	node

	Graph model.CIStr
	Paths []*PathPattern
}

func (n *MatchClause) resultSet() {}

func (n *MatchClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("MATCH ")
	switch len(n.Paths) {
	case 0:
		return errors.New("MatchClause must have at least one PathPattern")
	case 1:
		if err := n.Paths[0].Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore MatchClause.Paths")
		}
	default:
		ctx.WritePlain("(")
		for i, p := range n.Paths {
			if i > 0 {
				ctx.WritePlain(",")
			}
			if err := p.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore MatchClause.Paths[%d]", i)
			}
		}
		ctx.WritePlain(")")
	}
	if !n.Graph.IsEmpty() {
		ctx.WriteKeyWord(" ON ")
		ctx.WriteName(n.Graph.String())
	}

	return nil
}

func (n *MatchClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*MatchClause)
	for i, p := range nn.Paths {
		node, ok := p.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Paths[i] = node.(*PathPattern)
	}
	return v.Leave(nn)
}

type PathPatternType int

const (
	PathPatternSimple PathPatternType = iota
	PathPatternAny
	PathPatternAnyShortest
	PathPatternAllShortest
	PathPatternTopKShortest
	PathPatternAnyCheapest
	PathPatternAllCheapest
	PathPatternTopKCheapest
	PathPatternAll
)

type PathPattern struct {
	node

	Tp          PathPatternType
	TopK        int64
	Vertices    []*VertexPattern
	Connections []VertexPairConnection
}

func (n *PathPattern) Restore(ctx *format.RestoreCtx) error {
	if len(n.Vertices) == 0 {
		return errors.New("PathPattern must have at least one vertex pattern")
	}
	if len(n.Vertices) != len(n.Connections)+1 {
		return errors.Errorf("PathPattern vertices must be exactly one more than connections, but got %d vertices and %d connections", len(n.Vertices), len(n.Connections))
	}
	if n.Tp != PathPatternSimple && len(n.Vertices) != 2 {
		return errors.Errorf("variable-length paths can only have exactly two vertices, but got %d", len(n.Vertices))
	}
	switch n.Tp {
	case PathPatternSimple:
	case PathPatternAny:
		ctx.WriteKeyWord("ANY ")
	case PathPatternAnyShortest:
		ctx.WriteKeyWord("ANY SHORTEST ")
	case PathPatternAllShortest:
		ctx.WriteKeyWord("ALL SHORTEST ")
	case PathPatternTopKShortest:
		ctx.WriteKeyWord("TOP ")
		ctx.WritePlainf("%v", n.TopK)
		ctx.WriteKeyWord(" SHORTEST ")
	case PathPatternAnyCheapest:
		ctx.WriteKeyWord("ANY CHEAPEST ")
	case PathPatternAllCheapest:
		ctx.WriteKeyWord("ALL CHEAPEST ")
	case PathPatternTopKCheapest:
		ctx.WriteKeyWord("TOP ")
		ctx.WritePlainf("%v", n.TopK)
		ctx.WriteKeyWord(" CHEAPEST ")
	case PathPatternAll:
		ctx.WriteKeyWord("ALL ")
	default:
		return errors.Errorf("unknown PathPatternType: %v", n.Tp)
	}
	if err := n.Vertices[0].Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PathPattern.Vertices[0]")
	}
	for i := 0; i < len(n.Connections); i++ {
		ctx.WritePlain(" ")
		if err := n.Connections[i].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore PathPattern.Connections[%d]", i)
		}
		ctx.WritePlain(" ")
		if err := n.Vertices[i+1].Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore PathPattern.Vertices[%d]", i+1)
		}
	}
	return nil
}

func (n *PathPattern) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*PathPattern)
	for i, vertex := range nn.Vertices {
		node, ok := vertex.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Vertices[i] = node.(*VertexPattern)
	}
	for i, conn := range nn.Connections {
		node, ok := conn.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Connections[i] = node.(VertexPairConnection)
	}
	return v.Leave(nn)
}

type VertexPattern struct {
	node

	Variable *VariableSpec
}

func (n *VertexPattern) Restore(ctx *format.RestoreCtx) error {
	ctx.WritePlain("(")
	if err := n.Variable.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore VertexPattern.Variable")
	}
	ctx.WritePlain(")")
	return nil
}

func (n *VertexPattern) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*VertexPattern)
	node, ok := nn.Variable.Accept(v)
	if !ok {
		return nn, false
	}
	nn.Variable = node.(*VariableSpec)
	return v.Leave(nn)
}

type EdgeDirection int

const (
	EdgeDirectionOutgoing = iota
	EdgeDirectionIncoming
	EdgeDirectionAnyDirected
)

type VertexPairConnection interface {
	Node

	vertexPairConnection()
}

type EdgePattern struct {
	node

	Variable  *VariableSpec
	Direction EdgeDirection
}

func (n *EdgePattern) vertexPairConnection() {}

func (n *EdgePattern) Restore(ctx *format.RestoreCtx) error {
	switch n.Direction {
	case EdgeDirectionOutgoing:
		if n.Variable == nil {
			ctx.WritePlain("->")
		} else {
			ctx.WritePlain("-[")
			if err := n.Variable.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore EdgePattern.Variable")
			}
			ctx.WritePlain("]->")
		}
	case EdgeDirectionIncoming:
		if n.Variable == nil {
			ctx.WritePlain("<-")
		} else {
			ctx.WritePlain("<-[")
			if err := n.Variable.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore EdgePattern.Variable")
			}
			ctx.WritePlain("]-")
		}
	case EdgeDirectionAnyDirected:
		if n.Variable == nil {
			ctx.WritePlain("-")
		} else {
			ctx.WritePlain("-[")
			if err := n.Variable.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore EdgePattern.Variable")
			}
			ctx.WritePlain("]-")
		}
	default:
		return errors.Errorf("unknown edge direction: %v", n.Direction)
	}
	return nil
}

func (n *EdgePattern) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*EdgePattern)
	if nn.Variable != nil {
		node, ok := nn.Variable.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Variable = node.(*VariableSpec)
	}
	return v.Leave(nn)
}

type ReachabilityPathExpr struct {
	node

	Labels     []model.CIStr
	Direction  EdgeDirection
	Quantifier *PatternQuantifier
	// Variable name is not supported in ReachabilityPathExpr.
	// But we need an anonymous name for building logical plan.
	AnonymousName model.CIStr
	// Macros will be assigned in MacroExpansion stage.
	// LabelName(lower) -> PathPattern
	Macros map[string]*PathPattern
}

func (n *ReachabilityPathExpr) vertexPairConnection() {}

func (n *ReachabilityPathExpr) Restore(ctx *format.RestoreCtx) error {
	var prefix, suffix string
	switch n.Direction {
	case EdgeDirectionOutgoing:
		prefix = "-/"
		suffix = "/->"
	case EdgeDirectionIncoming:
		prefix = "<-/"
		suffix = "/-"
	case EdgeDirectionAnyDirected:
		prefix = "-/"
		suffix = "/-"
	default:
		return errors.Errorf("unknown edge direction: %v", n.Direction)
	}
	ctx.WritePlain(prefix)
	if len(n.Labels) == 0 {
		return errors.New("ReachabilityPathExpr must have at least one label predicate")
	}
	ctx.WritePlain(":")
	ctx.WriteName(n.Labels[0].String())
	for i := 1; i < len(n.Labels); i++ {
		ctx.WritePlain("|")
		ctx.WriteName(n.Labels[i].String())
	}
	if n.Quantifier != nil {
		if err := n.Quantifier.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore ReachabilityPathExpr.Quantifier")
		}
	}
	ctx.WritePlain(suffix)
	return nil
}

func (n *ReachabilityPathExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*ReachabilityPathExpr)
	if nn.Quantifier != nil {
		node, ok := nn.Quantifier.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Quantifier = node.(*PatternQuantifier)
	}
	return v.Leave(nn)
}

type QuantifiedPathExpr struct {
	node

	Edge        *EdgePattern
	Quantifier  *PatternQuantifier
	Source      *VertexPattern
	Destination *VertexPattern
	Where       ExprNode
	Cost        ExprNode
}

func (n *QuantifiedPathExpr) vertexPairConnection() {}

func (n *QuantifiedPathExpr) shouldParenthesize() bool {
	return n.Source != nil || n.Destination != nil || n.Where != nil || n.Cost != nil
}

func (n *QuantifiedPathExpr) Restore(ctx *format.RestoreCtx) error {
	shouldParenthesize := n.shouldParenthesize()
	if shouldParenthesize {
		ctx.WritePlain("(")
	}
	if n.Source != nil {
		if err := n.Source.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore QuantifiedPathExpr.Source")
		}
		ctx.WritePlain(" ")
	}
	if err := n.Edge.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore QuantifiedPathExpr.Edge")
	}
	if n.Destination != nil {
		ctx.WritePlain(" ")
		if err := n.Destination.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore QuantifiedPathExpr.Destination")
		}
	}
	if n.Where != nil {
		ctx.WriteKeyWord(" WHERE ")
		if err := n.Where.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore QuantifiedPathExpr.Where")
		}
	}
	if n.Cost != nil {
		ctx.WriteKeyWord(" COST ")
		if err := n.Cost.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore QuantifiedPathExpr.Cost")
		}
	}
	if shouldParenthesize {
		ctx.WritePlain(")")
	}
	if n.Quantifier != nil {
		if err := n.Quantifier.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore QuantifiedPathExpr.Quantifier")
		}
	}
	return nil
}

func (n *QuantifiedPathExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*QuantifiedPathExpr)
	node, ok := nn.Edge.Accept(v)
	if !ok {
		return nn, false
	}
	nn.Edge = node.(*EdgePattern)
	if nn.Quantifier != nil {
		node, ok = nn.Quantifier.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Quantifier = node.(*PatternQuantifier)
	}
	if nn.Source != nil {
		node, ok = nn.Source.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Source = node.(*VertexPattern)
	}
	if nn.Destination != nil {
		node, ok = nn.Destination.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Destination = node.(*VertexPattern)
	}
	if nn.Where != nil {
		node, ok = nn.Where.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Where = node.(ExprNode)
	}
	if nn.Cost != nil {
		node, ok = nn.Cost.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Cost = node.(ExprNode)
	}
	return v.Leave(nn)
}

type PatternQuantifierType int

const (
	PatternQuantifierZeroOrMore = iota
	PatternQuantifierOneOrMore
	PatternQuantifierOptional
	PatternQuantifierExactlyN
	PatternQuantifierNOrMore
	PatternQuantifierBetweenNAndM
	PatternQuantifierBetweenZeroAndM
)

type PatternQuantifier struct {
	node

	Tp PatternQuantifierType
	N  int64
	M  int64
}

func (n *PatternQuantifier) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case PatternQuantifierZeroOrMore:
		ctx.WritePlain("*")
	case PatternQuantifierOneOrMore:
		ctx.WritePlain("+")
	case PatternQuantifierOptional:
		ctx.WritePlain("?")
	case PatternQuantifierExactlyN:
		ctx.WritePlainf("{%d}", n.N)
	case PatternQuantifierNOrMore:
		ctx.WritePlainf("{%d,}", n.N)
	case PatternQuantifierBetweenNAndM:
		ctx.WritePlainf("{%d,%d}", n.N, n.M)
	case PatternQuantifierBetweenZeroAndM:
		ctx.WritePlainf("{,%d}", n.M)
	default:
		return errors.Errorf("unknown PatternQuantifierType: %v", n.Tp)
	}
	return nil
}

func (n *PatternQuantifier) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

type VariableSpec struct {
	node

	Name      model.CIStr
	Labels    []model.CIStr
	Anonymous bool
}

func (n *VariableSpec) Restore(ctx *format.RestoreCtx) error {
	if name := n.Name.String(); name != "" && !n.Anonymous {
		ctx.WriteName(name)
	}
	if len(n.Labels) > 0 {
		ctx.WritePlain(":")
		ctx.WriteName(n.Labels[0].String())
	}
	for i := 1; i < len(n.Labels); i++ {
		ctx.WritePlain("|")
		ctx.WriteName(n.Labels[i].String())
	}
	return nil
}

func (n *VariableSpec) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

type PathPatternMacro struct {
	node

	Name  model.CIStr
	Path  *PathPattern
	Where ExprNode
}

func (n *PathPatternMacro) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("PATH ")
	ctx.WriteName(n.Name.String())
	ctx.WriteKeyWord(" AS ")
	if err := n.Path.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PathPatternMacro.Path")
	}
	if n.Where != nil {
		ctx.WriteKeyWord(" WHERE ")
		if err := n.Where.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore PathPatternMacro.Where")
		}
	}
	return nil
}

func (n *PathPatternMacro) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*PathPatternMacro)
	node, ok := nn.Path.Accept(v)
	if !ok {
		return nn, false
	}
	nn.Path = node.(*PathPattern)
	if nn.Where != nil {
		node, ok = nn.Where.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Where = node.(ExprNode)
	}
	return v.Leave(nn)
}
