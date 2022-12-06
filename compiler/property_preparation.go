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
	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/storage/kv"
)

// PropertyPreparation is used to create property lazily. In zGraph: only Graph/Label/Index
// objects are required to create explicitly. All properties will be created at the first
// time to be used.
// The PropertyPreparation will visit the whole AST and find
type PropertyPreparation struct {
	sc *stmtctx.Context
	// Missing properties (lower case)
	missing []string
	graph   *catalog.Graph
}

func NewPropertyPreparation(sc *stmtctx.Context) *PropertyPreparation {
	return &PropertyPreparation{
		sc:    sc,
		graph: sc.Catalog().Graph(sc.CurrentGraph()),
	}
}

func (p *PropertyPreparation) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch node := n.(type) {
	case *ast.InsertStmt:
		if !node.IntoGraphName.IsEmpty() {
			p.graph = p.sc.Catalog().Graph(node.IntoGraphName.L)
		}
	case *ast.PropertyAccess:
		p.checkExistence(node.PropertyName)

	}
	return n, false
}

func (p *PropertyPreparation) checkExistence(propName model.CIStr) {
	prop := p.graph.Property(propName.L)
	if prop == nil {
		p.missing = append(p.missing, propName.L)
	}
}

func (p *PropertyPreparation) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, true
}

// CreateMissing creates the missing properties.
func (p *PropertyPreparation) CreateMissing() error {
	if len(p.missing) == 0 {
		return nil
	}

	p.sc.Catalog().MDLock()
	defer p.sc.Catalog().MDUnlock()

	var patch *catalog.PatchProperties
	err := kv.RunNewTxn(p.sc.Store(), func(txn kv.Transaction) error {
		graphInfo := p.graph.Meta()
		nextPropID := graphInfo.NextPropID
		meta := meta.New(txn)
		var properties []*model.PropertyInfo
		for _, propName := range p.missing {
			nextPropID++
			propertyInfo := &model.PropertyInfo{
				ID:   nextPropID,
				Name: model.NewCIStr(propName),
			}
			err := meta.CreateProperty(graphInfo.ID, propertyInfo)
			if err != nil {
				return errors.Annotatef(err, "create property")
			}
			properties = append(properties, propertyInfo)
		}

		cloned := graphInfo.Clone()
		cloned.NextPropID = nextPropID
		err := meta.UpdateGraph(cloned)
		if err != nil {
			return errors.Annotatef(err, "update graph")
		}

		patch = &catalog.PatchProperties{
			MaxPropID:  nextPropID,
			GraphID:    graphInfo.ID,
			Properties: properties,
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	p.sc.Catalog().Apply(&catalog.Patch{
		Type: catalog.PatchTypeCreateProperties,
		Data: patch,
	})

	return nil
}
