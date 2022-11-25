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
	_ Node = &UseStmt{}
	_ Node = &BeginStmt{}
	_ Node = &RollbackStmt{}
	_ Node = &CommitStmt{}
	_ Node = &ExplainStmt{}
)

type UseStmt struct {
	stmtNode

	GraphName model.CIStr
}

func (u *UseStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("USE ")
	ctx.WriteName(u.GraphName.String())
	return nil
}

func (u *UseStmt) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(u)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

type BeginStmt struct {
	stmtNode
}

func (b *BeginStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("BEGIN")
	return nil
}

func (b *BeginStmt) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(b)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

type RollbackStmt struct {
	node
}

func (r *RollbackStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ROLLBACK")
	return nil
}

func (r *RollbackStmt) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(r)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

type CommitStmt struct {
	stmtNode
}

func (c *CommitStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("COMMIT")
	return nil
}

func (c *CommitStmt) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(c)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

type ExplainStmt struct {
	stmtNode

	Select *SelectStmt
}

func (e *ExplainStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("EXPLAIN ")
	return e.Select.Restore(ctx)
}

func (e *ExplainStmt) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChildren := v.Enter(e)
	if skipChildren {
		return v.Leave(newNode)
	}

	nn := newNode.(*ExplainStmt)
	n, ok := nn.Select.Accept(v)
	if !ok {
		return nn, false
	}
	nn.Select = n.(*SelectStmt)

	return v.Leave(nn)
}
