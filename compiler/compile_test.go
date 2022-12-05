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
	"github.com/vescale/zgraph/executor"
	"github.com/vescale/zgraph/parser"
)

func TestCompile(t *testing.T) {
	assert := assert.New(t)
	db, err := zgraph.Open(t.TempDir(), nil)
	assert.Nil(err)

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
			query: "create graph g1",
			check: ddl,
		},
		{
			query: "create label if not exists l1 (a string)",
			check: ddl,
		},
		{
			query: "create index if not exists i1 on l1 (a)",
			check: ddl,
		},
		{
			query: "use g1",
			check: simple,
		},
	}

	for _, c := range cases {
		parser := parser.New()
		stmt, err := parser.ParseOneStmt(c.query)
		assert.Nil(err)
		sc := db.NewSession().StmtContext()
		exec, err := compiler.Compile(sc, stmt)
		assert.Nil(err)
		c.check(exec)
	}
}
