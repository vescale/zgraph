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
	"github.com/vescale/zgraph/planner"
	"github.com/vescale/zgraph/stmtctx"
)

func TestBuilder_BuildDDL(t *testing.T) {
	assert := assert.New(t)

	cases := []string{
		"create graph if not exists graph5",
		"create graph graph5",
		"create label label1 (a string, b integer)",
		"create label if not exists label1 (a string, b integer)",
		"create index index1 on label1 (a, b)",
		"create index if not exists index1 on label1 (a, b)",
		"drop graph graph5",
		"drop label label1",
		"drop index index1 on label1",
	}

	db, err := zgraph.Open(t.TempDir(), nil)
	assert.Nil(err)

	for _, c := range cases {
		parser := parser.New()
		stmt, err := parser.ParseOneStmt(c)
		assert.Nil(err)

		builder := compiler.NewBuilder(stmtctx.New(), db.Catalog())
		plan, err := builder.Build(stmt)
		assert.Nil(err)

		ddl, ok := plan.(*planner.DDL)
		assert.True(ok)
		assert.Equal(stmt, ddl.Statement)
	}
}
