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

import "errors"

package ast

import (
"github.com/pingcap/errors"
"github.com/pingcap/tidb/parser/format"
"github.com/pingcap/tidb/parser/model"
)

var (
	_ DDLNode = &CreatePropertyGraphStmt{}
	_ DDLNode = &DropPropertyGraphStmt{}

	_ Node = &GraphTable{}
	_ Node = &GraphName{}
	_ Node = &KeyClause{}
	_ Node = &LabelClause{}
	_ Node = &PropertiesClause{}
	_ Node = &VertexTableRef{}
	_ Node = &Property{}
	_ Node = &PathPattern{}
	_ Node = &VariableSpec{}
	_ Node = &VertexPattern{}
	_ Node = &ReachabilityPathExpr{}
	_ Node = &PatternQuantifier{}
	_ Node = &PathPatternMacro{}

	_ ResultSetNode = &MatchClause{}
	_ ResultSetNode = &MatchClauseList{}

	_ VertexPairConnection = &EdgePattern{}
	_ VertexPairConnection = &ReachabilityPathExpr{}
	_ VertexPairConnection = &QuantifiedPathExpr{}
)

type CreatePropertyGraphStmt struct {
	ddlNode

	Graph        *GraphName
	VertexTables []*GraphTable
	EdgeTables   []*GraphTable
}

func (n *CreatePropertyGraphStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE PROPERTY GRAPH ")
	if err := n.Graph.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreatePropertyGraphStmt.GraphName")
	}

	ctx.WriteKeyWord(" VERTEX TABLES ")
	ctx.WritePlain("(")
	for i, tbl := range n.VertexTables {
		if i > 0 {
			ctx.WritePlain(",")
		}
		if err := tbl.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreatePropertyGraphStmt.VertexTables[%d]", i)
		}
	}
	ctx.WritePlain(")")

	if len(n.EdgeTables) > 0 {
		ctx.WriteKeyWord(" EDGE TABLES ")
		ctx.WritePlain("(")
		for i, tbl := range n.EdgeTables {
			if i > 0 {
				ctx.WritePlain(",")
			}
			if err := tbl.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore CreatePropertyGraphStmt.EdgeTables[%d]", i)
			}
		}
		ctx.WritePlain(")")
	}

	return nil
}

func (n *CreatePropertyGraphStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*CreatePropertyGraphStmt)
	if nn.Graph != nil {
		node, ok := nn.Graph.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Graph = node.(*GraphName)
	}
	for i, tbl := range nn.VertexTables {
		node, ok := tbl.Accept(v)
		if !ok {
			return nn, false
		}
		nn.VertexTables[i] = node.(*GraphTable)
	}
	for i, tbl := range nn.EdgeTables {
		node, ok := tbl.Accept(v)
		if !ok {
			return nn, false
		}
		nn.EdgeTables[i] = node.(*GraphTable)
	}
	return v.Leave(nn)
}

type DropPropertyGraphStmt struct {
	ddlNode

	Graph *GraphName
}

func (n *DropPropertyGraphStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP PROPERTY GRAPH ")
	if err := n.Graph.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore DropPropertyGraphStmt.GraphName")
	}
	return nil
}

func (n *DropPropertyGraphStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*DropPropertyGraphStmt)
	if nn.Graph != nil {
		node, ok := nn.Graph.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Graph = node.(*GraphName)
	}
	return v.Leave(nn)
}

type GraphName struct {
	node

	Schema model.CIStr
	Name   model.CIStr
}

func (n *GraphName) Restore(ctx *format.RestoreCtx) error {
	if n.Schema.String() != "" {
		ctx.WriteName(n.Schema.String())
		ctx.WritePlain(".")
	}
	ctx.WriteName(n.Name.String())
	return nil
}

func (n *GraphName) Accept(v Visitor) (node Node, ok bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

type GraphTable struct {
	node

	Table      *TableName
	AsName     model.CIStr
	Key        *KeyClause
	Label      *LabelClause
	Properties *PropertiesClause
	// For edge table only. Source and Destination must be both non-nil or both nil.
	Source      *VertexTableRef
	Destination *VertexTableRef
}

func (n *GraphTable) Restore(ctx *format.RestoreCtx) error {
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore GraphTable.Table")
	}
	if asName := n.AsName.String(); asName != "" {
		ctx.WriteKeyWord(" AS ")
		ctx.WriteName(asName)
	}
	if n.Key != nil {
		ctx.WritePlain(" ")
		if err := n.Key.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore GraphTable.Key")
		}
	}

	if (n.Source == nil) != (n.Destination == nil) {
		return errors.New("GraphTable.Source and GraphTable.Destination must be both nil for vertex tables or both non-nil for edge tables")
	}
	if n.Source != nil {
		ctx.WriteKeyWord(" SOURCE ")
		if err := n.Source.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore GraphTable.Source")
		}
		ctx.WriteKeyWord(" DESTINATION ")
		if err := n.Destination.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore GraphTable.Destination")
		}
	}

	if n.Label != nil {
		ctx.WritePlain(" ")
		if err := n.Label.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore GraphTable.Label")
		}
	}
	if n.Properties != nil {
		ctx.WritePlain(" ")
		if err := n.Properties.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore GraphTable.Properties")
		}
	}
	return nil
}

func (n *GraphTable) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*GraphTable)
	node, ok := nn.Table.Accept(v)
	if !ok {
		return nn, false
	}
	nn.Table = node.(*TableName)
	if nn.Key != nil {
		node, ok = nn.Key.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Key = node.(*KeyClause)
	}
	if nn.Source != nil {
		node, ok = nn.Source.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Source = node.(*VertexTableRef)
	}
	if nn.Destination != nil {
		node, ok = nn.Destination.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Destination = node.(*VertexTableRef)
	}
	if nn.Label != nil {
		node, ok = nn.Label.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Label = node.(*LabelClause)
	}
	if nn.Properties != nil {
		node, ok = nn.Properties.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Properties = node.(*PropertiesClause)
	}
	return v.Leave(nn)
}

type KeyClause struct {
	node

	Cols []*ColumnName
}

func (n *KeyClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("KEY ")
	ctx.WritePlain("(")
	for i, col := range n.Cols {
		if i > 0 {
			ctx.WritePlain(",")
		}
		if err := col.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore KeyClause.Cols[%d]", i)
		}
	}
	ctx.WritePlain(")")
	return nil
}

func (n *KeyClause) Accept(v Visitor) (node Node, ok bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

type VertexTableRef struct {
	node

	Key   *KeyClause
	Table *TableName
}

func (n *VertexTableRef) Restore(ctx *format.RestoreCtx) error {
	if n.Key != nil {
		if err := n.Key.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore VertexTableRef.Key")
		}
		ctx.WriteKeyWord(" REFERENCES ")
	}
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore VertexTableRef.Table")
	}
	return nil
}

func (n *VertexTableRef) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*VertexTableRef)
	if nn.Key != nil {
		node, ok := nn.Key.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Key = node.(*KeyClause)
	}
	node, ok := nn.Table.Accept(v)
	if !ok {
		return nn, false
	}
	nn.Table = node.(*TableName)
	return v.Leave(nn)
}

type LabelClause struct {
	node

	Name model.CIStr
}

func (n *LabelClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("LABEL ")
	ctx.WriteName(n.Name.String())
	return nil
}

func (n *LabelClause) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

type PropertiesClause struct {
	node

	AllCols      bool
	ExceptCols   []*ColumnName
	Properties   []*Property
	NoProperties bool
}

func (n *PropertiesClause) Restore(ctx *format.RestoreCtx) error {
	switch {
	case n.AllCols:
		ctx.WriteKeyWord("PROPERTIES ARE ALL COLUMNS")
		if len(n.ExceptCols) > 0 {
			ctx.WriteKeyWord(" EXCEPT ")
			ctx.WritePlain("(")
			for i, col := range n.ExceptCols {
				if i > 0 {
					ctx.WritePlain(",")
				}
				if err := col.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore PropertiesClause.ExceptCols[%d]", i)
				}
			}
			ctx.WritePlain(")")
		}
	case len(n.Properties) > 0:
		ctx.WriteKeyWord("PROPERTIES ")
		ctx.WritePlain("(")
		for i, prop := range n.Properties {
			if i > 0 {
				ctx.WritePlain(",")
			}
			if err := prop.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore PropertiesClause.Properties[%d]", i)
			}
		}
		ctx.WritePlain(")")
	case n.NoProperties:
		ctx.WriteKeyWord("NO PROPERTIES")
	}
	return nil
}

func (n *PropertiesClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*PropertiesClause)
	switch {
	case nn.AllCols:
		for i, col := range n.ExceptCols {
			node, ok := col.Accept(v)
			if !ok {
				return nn, false
			}
			nn.ExceptCols[i] = node.(*ColumnName)
		}
	case len(nn.Properties) > 0:
		for i, prop := range nn.Properties {
			node, ok := prop.Accept(v)
			if !ok {
				return nn, false
			}
			nn.Properties[i] = node.(*Property)
		}
	}
	return v.Leave(nn)
}

type Property struct {
	node

	Expr   ExprNode
	AsName model.CIStr
}

func (n *Property) Restore(ctx *format.RestoreCtx) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore Property.Expr")
	}
	if asName := n.AsName.String(); asName != "" {
		ctx.WriteKeyWord(" AS ")
		ctx.WriteName(asName)
	}
	return nil
}

func (n *Property) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	nn := newNode.(*Property)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return nn, false
	}
	nn.Expr = node.(ExprNode)
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

	Graph *GraphName
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
	if n.Graph != nil {
		ctx.WriteKeyWord(" ON ")
		if err := n.Graph.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore MatchClause.Graph")
		}
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
	if nn.Graph != nil {
		node, ok := nn.Graph.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Graph = node.(*GraphName)
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
	TopK        uint64
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
	N  uint64
	M  uint64
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
