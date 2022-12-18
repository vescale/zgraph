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
	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/meta"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/stmtctx"
)

// Preprocess is used to validate the AST to ensure the AST is valid.
type Preprocess struct {
	sc  *stmtctx.Context
	err error
}

// NewPreprocess returns a preprocess visitor.
func NewPreprocess(sc *stmtctx.Context) *Preprocess {
	return &Preprocess{
		sc: sc,
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
	case *ast.InsertStmt:
		p.checkInsertStmt(stmt)
	case *ast.SelectStmt:
		p.checkSelectStmt(stmt)
	}
	return n, p.err != nil
}

// Leave implements the ast.Visitor interface.
func (p *Preprocess) Leave(n ast.Node) (node ast.Node, ok bool) {
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
		graph := p.sc.Catalog().Graph(stmt.Graph.L)
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
		// An ErrGraphNotChosen are expected if users didn't choose graph via USE <graphName>
		graphName := p.sc.CurrentGraphName()
		if graphName == "" {
			p.err = ErrGraphNotChosen
			return
		}
		// change it from CurrentGraph to Catalog.Graph,  this will reduce the overhead of a lock,
		// and since we got the name above, there is no need to use CurrentGraph
		graph := p.sc.Catalog().Graph(graphName)
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

	graphName := p.sc.CurrentGraphName()
	if graphName == "" {
		p.err = ErrGraphNotChosen
		return
	}

	graph := p.sc.Catalog().Graph(graphName)
	if graph == nil {
		p.err = meta.ErrGraphNotExists
		return
	}

	index := graph.Index(stmt.IndexName.L)
	if index != nil && !stmt.IfNotExists {
		p.err = meta.ErrIndexExists
		return
	}

	for _, prop := range stmt.Properties {
		property := graph.Property(prop.L)
		if property == nil {
			p.err = errors.Annotatef(meta.ErrPropertyNotExists, "property %s", prop.L)
			return
		}
	}
}

func (p *Preprocess) checkDropGraphStmt(stmt *ast.DropGraphStmt) {
	graph := p.sc.Catalog().Graph(stmt.Graph.L)
	if graph == nil && !stmt.IfExists {
		p.err = meta.ErrGraphNotExists
		return
	}
}

func (p *Preprocess) checkDropLabelStmt(stmt *ast.DropLabelStmt) {
	graph := p.sc.CurrentGraph()
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
	graph := p.sc.CurrentGraph()
	if graph == nil && !stmt.IfExists {
		p.err = meta.ErrGraphNotExists
		return
	}
	index := graph.Index(stmt.IndexName.L)
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

	graph := p.sc.Catalog().Graph(stmt.GraphName.L)
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

func (p *Preprocess) checkInsertStmt(stmt *ast.InsertStmt) {
	var intoGraph string
	if !stmt.IntoGraphName.IsEmpty() {
		if isIncorrectName(stmt.IntoGraphName.L) {
			p.err = ErrIncorrectGraphName
			return
		}
		intoGraph = stmt.IntoGraphName.L
	}
	var graph *catalog.Graph
	if intoGraph == "" {
		graph = p.sc.CurrentGraph()
	} else {
		graph = p.sc.Catalog().Graph(intoGraph)
	}
	if graph == nil {
		p.err = errors.Annotatef(meta.ErrGraphNotExists, "graph: %s", intoGraph)
		return
	}

	// Make sure all labels exists.
	for _, insertion := range stmt.Insertions {
		variables := map[string]struct{}{}
		switch insertion.InsertionType {
		case ast.InsertionTypeVertex:
			if !insertion.VariableName.IsEmpty() {
				variables[insertion.VariableName.L] = struct{}{}
			}
		case ast.InsertionTypeEdge:
			if !insertion.VariableName.IsEmpty() {
				variables[insertion.VariableName.L] = struct{}{}
			}
			variables[insertion.From.L] = struct{}{}
			variables[insertion.From.L] = struct{}{}
		}

		// All labels reference variable names need exists.
		lps := insertion.LabelsAndProperties
		if len(lps.Labels) > 0 {
			for _, lbl := range lps.Labels {
				label := graph.Label(lbl.L)
				if label == nil {
					p.err = errors.Annotatef(meta.ErrLabelNotExists, "label: %s", lbl.L)
					return
				}
			}
		}
		if len(lps.Assignments) > 0 {
			for _, a := range lps.Assignments {
				_, ok := variables[a.PropertyAccess.VariableName.L]
				if !ok {
					p.err = errors.Annotatef(ErrVariableReferenceNotExits, "variable: %s", a.PropertyAccess.VariableName.L)
					return
				}
			}
		}
	}
}

func (p *Preprocess) checkSelectStmt(_ *ast.SelectStmt) {}
