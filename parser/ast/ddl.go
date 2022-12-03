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
	_ DDLNode = &CreateGraphStmt{}
	_ DDLNode = &DropGraphStmt{}
	_ DDLNode = &CreateLabelStmt{}
	_ DDLNode = &DropLabelStmt{}
	_ DDLNode = &CreateIndexStmt{}
	_ DDLNode = &DropIndexStmt{}
)

type CreateGraphStmt struct {
	ddlNode

	IfNotExists bool
	Graph       model.CIStr
}

func (n *CreateGraphStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE GRAPH ")
	ctx.WriteName(n.Graph.String())
	return nil
}

func (n *CreateGraphStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

type DropGraphStmt struct {
	ddlNode

	IfExists bool
	Graph    model.CIStr
}

func (n *DropGraphStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP GRAPH ")
	ctx.WriteName(n.Graph.String())
	return nil
}

func (n *DropGraphStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

type CreateLabelStmt struct {
	ddlNode

	IfNotExists bool
	Label       model.CIStr
	Properties  []*LabelProperty
}

// Restore implements Node interface.
func (n *CreateLabelStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	ctx.WriteKeyWord("LABEL ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	ctx.WriteName(n.Label.String())

	ctx.WritePlain(" (")
	for i, prop := range n.Properties {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		err := prop.Restore(ctx)
		if err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateLabelStmt.Properties[%d]", i)
		}
	}
	ctx.WritePlain(")")

	return nil
}

// Accept implements Node Accept interface.
func (n *CreateLabelStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	nn := newNode.(*CreateLabelStmt)
	for i, prop := range nn.Properties {
		node, ok := prop.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Properties[i] = node.(*LabelProperty)
	}

	return v.Leave(newNode)
}

type LabelProperty struct {
	node

	Name    model.CIStr
	Type    DataType
	Options []*LabelPropertyOption
}

// Restore implements Node interface.
func (n *LabelProperty) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteName(n.Name.String())
	ctx.WritePlain(" ")
	ctx.WriteKeyWord(n.Type.String())
	for i, opt := range n.Options {
		ctx.WritePlain(" ")
		err := opt.Restore(ctx)
		if err != nil {
			return errors.Annotatef(err, "An error occurred while restore LabelProperty.Options[%d]", i)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *LabelProperty) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	nn := newNode.(*LabelProperty)
	for i, opt := range nn.Options {
		node, ok := opt.Accept(v)
		if !ok {
			return nn, false
		}
		nn.Options[i] = node.(*LabelPropertyOption)
	}

	return v.Leave(newNode)
}

type LabelPropertyOptionType byte

const (
	LabelPropertyOptionTypeNotNull LabelPropertyOptionType = iota
	LabelPropertyOptionTypeNull
	LabelPropertyOptionTypeDefault
	LabelPropertyOptionTypeComment
)

type LabelPropertyOption struct {
	node

	Type LabelPropertyOptionType
	Data interface{}
}

func (n *LabelPropertyOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Type {
	case LabelPropertyOptionTypeNotNull:
		ctx.WriteKeyWord("NOT NULL")
	case LabelPropertyOptionTypeNull:
		ctx.WriteKeyWord("NULL")
	case LabelPropertyOptionTypeDefault:
		ctx.WriteKeyWord("DEFAULT ")
		expr := n.Data.(ExprNode)
		err := expr.Restore(ctx)
		return errors.Annotate(err, "An error occurred while restore LabelPropertyOption.Default")
	case LabelPropertyOptionTypeComment:
		ctx.WriteKeyWord("COMMENT ")
		ctx.WriteString(n.Data.(string))
	default:
		return fmt.Errorf("UNKNOWN<%d>", n.Type)
	}
	return nil
}

func (n *LabelPropertyOption) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

type DropLabelStmt struct {
	ddlNode

	IfExists bool
	Label    model.CIStr
}

func (n *DropLabelStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP LABEL ")
	ctx.WriteName(n.Label.String())
	return nil
}

func (n *DropLabelStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

// IndexKeyType is the type for index key.
type IndexKeyType int

// Index key types.
const (
	IndexKeyTypeNone IndexKeyType = iota
	IndexKeyTypeUnique
)

// CreateIndexStmt is a statement to create an index.
// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type CreateIndexStmt struct {
	ddlNode

	KeyType     IndexKeyType
	IfNotExists bool

	IndexName  model.CIStr
	LabelName  model.CIStr
	Properties []model.CIStr
}

// Restore implements Node interface.
func (n *CreateIndexStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	switch n.KeyType {
	case IndexKeyTypeUnique:
		ctx.WriteKeyWord("UNIQUE ")
	}
	ctx.WriteKeyWord("INDEX ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	ctx.WriteName(n.IndexName.String())
	ctx.WriteKeyWord(" ON ")
	ctx.WriteName(n.LabelName.String())

	ctx.WritePlain(" (")
	for i, propName := range n.Properties {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteName(propName.String())
	}
	ctx.WritePlain(")")

	return nil
}

// Accept implements Node Accept interface.
func (n *CreateIndexStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

// DropIndexStmt is a statement to drop the index.
// See https://dev.mysql.com/doc/refman/5.7/en/drop-index.html
type DropIndexStmt struct {
	ddlNode

	IfExists  bool
	IndexName model.CIStr
	LabelName model.CIStr
}

// Restore implements Node interface.
func (n *DropIndexStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP INDEX ")
	if n.IfExists {
		ctx.WriteWithSpecialComments("", func() {
			ctx.WriteKeyWord("IF EXISTS ")
		})
	}
	ctx.WriteName(n.IndexName.String())
	ctx.WriteKeyWord(" ON ")
	ctx.WriteName(n.LabelName.String())

	return nil
}

// Accept implements Node Accept interface.
func (n *DropIndexStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}
