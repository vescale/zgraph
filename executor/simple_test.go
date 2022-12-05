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
	"github.com/vescale/zgraph/session"
)

func TestSimpleExec(t *testing.T) {
	assert := assert.New(t)
	db, err := zgraph.Open(t.TempDir(), nil)
	assert.Nil(err)

	catalog := db.Catalog()

	cases := []struct {
		query string
		check func(session *session.Session)
	}{
		{
			query: "create graph g1",
			check: func(_ *session.Session) {
				assert.NotNil(catalog.Graph("g1"))
			},
		},
		{
			query: "use g1",
			check: func(s *session.Session) {
				assert.Equal("g1", s.StmtContext().CurrentGraph())
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
		exec, err := compiler.Compile(sc, stmt)
		assert.Nil(err)

		err = exec.Open(ctx)
		assert.Nil(err)
		err = exec.Next(ctx, nil)
		assert.Nil(err)

		if c.check != nil {
			c.check(s)
		}
	}
}
