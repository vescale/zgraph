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

package compiler_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph"
	"github.com/vescale/zgraph/compiler"
	"github.com/vescale/zgraph/parser"
	"github.com/vescale/zgraph/stmtctx"
)

func TestPropertyPreparation(t *testing.T) {
	assert := assert.New(t)
	tempDir := t.TempDir()
	initCatalog(assert, tempDir)

	db, err := zgraph.Open(tempDir, nil)
	assert.Nil(err)

	cases := []struct {
		query string
		check func()
	}{
		{
			query: "INSERT INTO graph1 VERTEX x LABELS (label1) PROPERTIES ( x.prop1 = 'test')",
			check: func() {
				graph := db.Catalog().Graph("graph1")
				assert.NotNil(graph.Property("prop1"))
			},
		},
		{
			query: "INSERT INTO graph2 VERTEX x LABELS (label1) PROPERTIES ( x.property = 'test')",
			check: func() {
				graph := db.Catalog().Graph("graph1")
				assert.NotNil(2, len(graph.Properties()))
			},
		},
		{
			query: "INSERT INTO graph2 VERTEX x LABELS (label1) PROPERTIES ( x.property2 = 'test')",
			check: func() {
				graph := db.Catalog().Graph("graph1")
				assert.NotNil(3, len(graph.Properties()))
				assert.NotNil(graph.Property("property2"))
			},
		},
	}

	for _, c := range cases {
		parser := parser.New()
		stmt, err := parser.ParseOneStmt(c.query)
		assert.Nil(err)
		sc := stmtctx.New(db.Store(), db.Catalog())

		prep := compiler.NewPropertyPreparation(sc)
		stmt.Accept(prep)
		err = prep.CreateMissing()
		assert.Nil(err)
	}
}
