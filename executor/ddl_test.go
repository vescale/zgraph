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

package executor_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph"
	"github.com/vescale/zgraph/compiler"
	"github.com/vescale/zgraph/parser"
)

func TestDDLExec_Next(t *testing.T) {
	assert := assert.New(t)
	db, err := zgraph.Open(t.TempDir(), nil)
	assert.Nil(err)

	catalog := db.Catalog()

	cases := []struct {
		query string
		graph string
		check func()
	}{
		{
			query: "create graph g1",
			check: func() {
				assert.NotNil(catalog.Graph("g1"))
			},
		},
		{
			graph: "g1",
			query: "create label l1",
			check: func() {
				graph := catalog.Graph("g1")
				label := graph.Label("l1")
				assert.NotNil(label)
				labelInfo := label.Meta()
				assert.Equal("l1", labelInfo.Name.L)
			},
		},
		{
			graph: "g1",
			query: "drop label l1",
			check: func() {
				graph := catalog.Graph("g1")
				label := graph.Label("l1")
				assert.Nil(label)
			},
		},
		{
			query: "drop graph g1",
			check: func() {
				assert.Nil(catalog.Graph("g1"))
			},
		},
	}

	ctx := context.Background()
	for _, c := range cases {
		parser := parser.New()
		stmt, err := parser.ParseOneStmt(c.query)
		assert.Nil(err)

		s := db.NewSession()
		sc := s.StmtContext()
		if c.graph != "" {
			sc.SetCurrentGraphName(c.graph)
		}
		exec, err := compiler.Compile(sc, stmt)
		assert.Nil(err)

		err = exec.Open(ctx)
		assert.Nil(err)
		_, err = exec.Next(ctx)
		assert.Nil(err)

		if c.check != nil {
			c.check()
		}
	}
}
