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
	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/compiler"
	"github.com/vescale/zgraph/executor"
	"github.com/vescale/zgraph/parser"
	"github.com/vescale/zgraph/parser/model"
)

func TestCompile(t *testing.T) {
	t.Skip()
	assert := assert.New(t)
	db, err := zgraph.Open(t.TempDir(), nil)
	assert.Nil(err)

	db.Catalog().Apply(&catalog.Patch{
		Type: catalog.PatchTypeCreateGraph,
		Data: &model.GraphInfo{
			ID:   1,
			Name: model.NewCIStr("g1"),
		},
	})

	ddl := func(exec executor.Executor) {
		_, ok := exec.(*executor.DDLExec)
		assert.True(ok)
	}

	simple := func(exec executor.Executor) {
		_, ok := exec.(*executor.SimpleExec)
		assert.True(ok)
	}

	cases := []struct {
		query string
		check func(exec executor.Executor)
	}{
		{
			query: "create graph g2",
			check: ddl,
		},
		{
			query: "create graph if not exists g1",
			check: ddl,
		},
		{
			query: "create label if not exists l1",
			check: ddl,
		},
		{
			query: "create index if not exists i1 (a)",
			check: ddl,
		},
		{
			query: "use g1",
			check: simple,
		},
	}

	sc := db.NewSession().StmtContext()
	sc.SetCurrentGraphName("g1")

	for _, c := range cases {
		parser := parser.New()
		stmt, err := parser.ParseOneStmt(c.query)
		assert.Nil(err, c.query)
		exec, err := compiler.Compile(sc, stmt)
		assert.Nil(err, c.query)
		c.check(exec)
	}
}
