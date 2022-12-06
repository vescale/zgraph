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

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph/session"
)

func TestOpen(t *testing.T) {
	assert := assert.New(t)
	db, err := Open(t.TempDir(), nil)
	assert.Nil(err)
	assert.NotNil(db)
}

func runQuery(ctx context.Context, session *session.Session, query string) error {
	rs, err := session.Execute(ctx, query)
	if err != nil {
		return err
	}
	return rs.Next(ctx)
}

func TestDB_DDL(t *testing.T) {
	assert := assert.New(t)
	db, err := Open(t.TempDir(), nil)
	assert.Nil(err)
	assert.NotNil(db)
	defer db.Close()

	catalog := db.Catalog()
	session := db.NewSession()
	assert.NotNil(session)

	ctx := context.Background()
	err = runQuery(ctx, session, "CREATE GRAPH graph101")
	assert.Nil(err)
	graph := catalog.Graph("graph101")
	assert.NotNil(graph)

	session.StmtContext().SetCurrentGraph("graph101")
	err = runQuery(ctx, session, "CREATE LABEL label01")
	assert.Nil(err)
	assert.NotNil(graph.Label("label01"))

	err = runQuery(ctx, session, "CREATE LABEL IF NOT EXISTS label01")
	assert.Nil(err)

	err = runQuery(ctx, session, "DROP LABEL label01")
	assert.Nil(err)
	assert.Nil(graph.Label("label01"))

	err = runQuery(ctx, session, "DROP LABEL IF EXISTS label01")
	assert.Nil(err)
	assert.Nil(graph.Label("label01"))

	err = runQuery(ctx, session, "DROP GRAPH graph101")
	assert.Nil(err)
	assert.Nil(catalog.Graph("graph101"))
}
