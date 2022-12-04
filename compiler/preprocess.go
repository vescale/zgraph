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

package compiler

import (
	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/meta"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/stmtctx"
)

// Preprocess is used to validate the AST to ensure the AST is valid.
type Preprocess struct {
	sc      *stmtctx.Context
	catalog *catalog.Catalog
	err     error
}

// NewPreprocess returns a preprocess visitor.
func NewPreprocess(sc *stmtctx.Context, catalog *catalog.Catalog) *Preprocess {
	return &Preprocess{
		sc:      sc,
		catalog: catalog,
	}
}

// Enter implements the ast.Visitor interface.
func (p *Preprocess) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch stmt := n.(type) {
	case *ast.CreateGraphStmt:
		p.checkCreateGraphStmt(stmt)
	case *ast.CreateLabelStmt:
		p.checkCreateLabelStmt(stmt)
	case *ast.CreateIndexStmt:
		p.checkCreateIndexStmt(stmt)
	case *ast.DropGraphStmt:
		p.checkDropGraphStmt(stmt)
	case *ast.DropLabelStmt:
		p.checkDropLabelStmt(stmt)
	case *ast.DropIndexStmt:
		p.checkDropIndexStmt(stmt)
	case *ast.UseStmt:
		p.checkUseStmt(stmt)
	}
	return n, p.err != nil
}

// Leave implements the ast.Visitor interface.
func (p *Preprocess) Leave(n ast.Node) (node ast.Node, ok bool) {
	switch n.(type) {
	// TODO: post checks
	}
	return n, p.err != nil
}

// Error returns the internal error of preprocess.
func (p *Preprocess) Error() error {
	return p.err
}

func (p *Preprocess) checkCreateGraphStmt(stmt *ast.CreateGraphStmt) {
	if isIncorrectName(stmt.Graph.L) {
		p.err = ErrIncorrectGraphName
		return
	}

	if !stmt.IfNotExists {
		graph := p.catalog.Graph(stmt.Graph.L)
		if graph != nil {
			p.err = meta.ErrGraphExists
			return
		}
	}
}

func (p *Preprocess) checkCreateLabelStmt(stmt *ast.CreateLabelStmt) {
	if isIncorrectName(stmt.Label.L) {
		p.err = ErrIncorrectLabelName
		return
	}

	if !stmt.IfNotExists {
		graphName := p.sc.CurrentGraph()
		if graphName == "" {
			p.err = ErrGraphNotChosen
			return
		}

		graph := p.catalog.Graph(graphName)
		if graph == nil {
			p.err = meta.ErrGraphNotExists
			return
		}
		label := graph.Label(stmt.Label.L)
		if label != nil {
			p.err = meta.ErrLabelExists
			return
		}
	}
}

func (p *Preprocess) checkCreateIndexStmt(stmt *ast.CreateIndexStmt) {
	if isIncorrectName(stmt.IndexName.L) {
		p.err = ErrIncorrectIndexName
		return
	}

	graphName := p.sc.CurrentGraph()
	if graphName == "" {
		p.err = ErrGraphNotChosen
		return
	}

	graph := p.catalog.Graph(graphName)
	if graph == nil {
		p.err = meta.ErrGraphNotExists
		return
	}
	label := graph.Label(stmt.LabelName.L)
	if label == nil {
		p.err = meta.ErrLabelNotExists
		return
	}

	index := label.Index(stmt.IndexName.L)
	if index != nil && !stmt.IfNotExists {
		p.err = meta.ErrIndexExists
		return
	}

	// TODO: check property names
}

func (p *Preprocess) checkDropGraphStmt(stmt *ast.DropGraphStmt) {
	graph := p.catalog.Graph(stmt.Graph.L)
	if graph == nil && !stmt.IfExists {
		p.err = meta.ErrGraphNotExists
		return
	}
}

func (p *Preprocess) checkDropLabelStmt(stmt *ast.DropLabelStmt) {
	graphName := p.sc.CurrentGraph()
	if graphName == "" {
		p.err = ErrGraphNotChosen
		return
	}

	graph := p.catalog.Graph(graphName)
	if graph == nil && !stmt.IfExists {
		p.err = meta.ErrGraphNotExists
		return
	}
	label := graph.Label(stmt.Label.L)
	if label == nil && !stmt.IfExists {
		p.err = meta.ErrLabelNotExists
		return
	}
}

func (p *Preprocess) checkDropIndexStmt(stmt *ast.DropIndexStmt) {
	graphName := p.sc.CurrentGraph()
	if graphName == "" {
		p.err = ErrGraphNotChosen
		return
	}

	graph := p.catalog.Graph(graphName)
	if graph == nil && !stmt.IfExists {
		p.err = meta.ErrGraphNotExists
		return
	}
	label := graph.Label(stmt.LabelName.L)
	if label == nil && !stmt.IfExists {
		p.err = meta.ErrLabelNotExists
		return
	}
	index := label.Index(stmt.IndexName.L)
	if index == nil && !stmt.IfExists {
		p.err = meta.ErrIndexNotExists
		return
	}
}

func (p *Preprocess) checkUseStmt(stmt *ast.UseStmt) {
	if isIncorrectName(stmt.GraphName.L) {
		p.err = ErrIncorrectGraphName
		return
	}

	graph := p.catalog.Graph(stmt.GraphName.L)
	if graph == nil {
		p.err = meta.ErrGraphNotExists
		return
	}
}

// isIncorrectName checks if the identifier is incorrect.
// See https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
func isIncorrectName(name string) bool {
	if len(name) == 0 {
		return true
	}
	if name[len(name)-1] == ' ' {
		return true
	}
	return false
}
