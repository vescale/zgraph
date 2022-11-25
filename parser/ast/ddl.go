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
	"github.com/vescale/zgraph/parser/format"
	"github.com/vescale/zgraph/parser/model"
)

var (
	_ DDLNode = &CreateGraphStmt{}
	_ DDLNode = &DropGraphStmt{}
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
