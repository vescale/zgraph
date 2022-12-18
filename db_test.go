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

package zgraph

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vescale/zgraph/session"
)

func TestOpen(t *testing.T) {
	db, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	require.NotNil(t, db)
}

type TestKit struct {
	t    *testing.T
	sess *session.Session
}

func NewTestKit(t *testing.T, sess *session.Session) *TestKit {
	return &TestKit{
		t:    t,
		sess: sess,
	}
}

func (tk *TestKit) MustExec(ctx context.Context, query string) {
	rs, err := tk.sess.Execute(ctx, query)
	require.NoError(tk.t, err)
	require.NoError(tk.t, rs.Next(ctx))
}

func TestDB_DDL(t *testing.T) {
	db, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()

	catalog := db.Catalog()
	sess := db.NewSession()
	require.NotNil(t, sess)

	tk := NewTestKit(t, sess)

	ctx := context.Background()
	tk.MustExec(ctx, "CREATE GRAPH graph101")
	require.NoError(t, err)
	graph := catalog.Graph("graph101")
	require.NotNil(t, graph)

	sess.StmtContext().SetCurrentGraphName("graph101")
	tk.MustExec(ctx, "CREATE LABEL label01")
	require.NoError(t, err)
	require.NotNil(t, graph.Label("label01"))

	tk.MustExec(ctx, "CREATE LABEL IF NOT EXISTS label01")
	require.NoError(t, err)

	tk.MustExec(ctx, "DROP LABEL label01")
	require.NoError(t, err)
	require.Nil(t, graph.Label("label01"))

	tk.MustExec(ctx, "DROP LABEL IF EXISTS label01")
	require.NoError(t, err)
	require.Nil(t, graph.Label("label01"))

	tk.MustExec(ctx, "DROP GRAPH graph101")
	require.NoError(t, err)
	require.Nil(t, catalog.Graph("graph101"))
}

func TestDB_Select(t *testing.T) {
	t.Skip()

	db, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()

	sess := db.NewSession()
	require.NotNil(t, sess)
	tk := NewTestKit(t, sess)

	ctx := context.Background()
	tk.MustExec(ctx, "CREATE GRAPH student_network")
	tk.MustExec(ctx, "USE student_network")
	tk.MustExec(ctx, "CREATE LABEL Person")
	tk.MustExec(ctx, "CREATE LABEL University")
	tk.MustExec(ctx, "CREATE LABEL knows")
	tk.MustExec(ctx, "CREATE LABEL studentOf")

	// A simple example in https://pgql-lang.org/spec/1.5/#edge-patterns.
	tk.MustExec(ctx, `INSERT VERTEX x LABELS (Person) PROPERTIES (x.name = 'Kathrine', x.dob = DATE '1994-01-15')`)
	tk.MustExec(ctx, `INSERT VERTEX x LABELS (Person) PROPERTIES (x.name = 'Riya', x.dob = DATE '1995-03-20')`)
	tk.MustExec(ctx, `INSERT VERTEX x LABELS (Person) PROPERTIES (x.name = 'Lee', x.dob = DATE '1996-01-20')`)
	tk.MustExec(ctx, `INSERT VERTEX x LABELS (University) PROPERTIES (x.name = 'UC Berkeley')`)
	tk.MustExec(ctx, `INSERT EDGE e BETWEEN x AND y LABELS ( knows ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Kathrine' AND y.name = 'Lee'`)
	tk.MustExec(ctx, `INSERT EDGE e BETWEEN x AND y LABELS ( knows ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Kathrine' AND y.name = 'Riya'`)
	tk.MustExec(ctx, `INSERT EDGE e BETWEEN x AND y LABELS ( knows ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Lee' AND y.name = 'Kathrine'`)
	tk.MustExec(ctx, `INSERT EDGE e BETWEEN x AND y LABELS ( studentOf ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Kathrine' AND y.name = 'UC Berkeley'`)
	tk.MustExec(ctx, `INSERT EDGE e BETWEEN x AND y LABELS ( studentOf ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Lee' AND y.name = 'UC Berkeley'`)
	tk.MustExec(ctx, `INSERT EDGE e BETWEEN x AND y LABELS ( studentOf ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Riya' AND y.name = 'UC Berkeley'`)

	rs, err := sess.Execute(ctx, `SELECT a.name AS a, b.name AS b FROM MATCH (a:Person) -[e:knows]-> (b:Person)`)
	require.NoError(t, err)
	require.Len(t, rs.Fields(), 2)

	var knows [][2]string
	for {
		require.NoError(t, rs.Next(ctx))
		if !rs.Valid() {
			break
		}
		var a, b string
		require.NoError(t, rs.Scan(&a, &b))
	}
	sort.Slice(knows, func(i, j int) bool {
		return knows[i][0] < knows[j][0] || knows[i][1] < knows[j][1]
	})
	require.Len(t, knows, 3)
	require.Equal(t, [][2]string{{"Kathrine", "Lee"}, {"Kathrine", "Riya"}, {"Lee", "Kathrine"}}, knows)
}
