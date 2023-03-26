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

func TestDDL(t *testing.T) {
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
