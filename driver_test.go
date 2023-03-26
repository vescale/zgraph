// Copyright 2023 zGraph Authors. All rights reserved.
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

package zgraph_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestDriver(t *testing.T) {
	db, err := sql.Open("zgraph", t.TempDir())
	require.NoError(t, err)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn := lo.Must1(db.Conn(ctx))
	_ = lo.Must1(conn.ExecContext(ctx, "CREATE GRAPH g"))
	_ = lo.Must1(conn.ExecContext(ctx, "USE g"))
	_ = lo.Must1(conn.ExecContext(ctx, "INSERT VERTEX x PROPERTIES (x.a = 123)"))
	rows := lo.Must1(conn.QueryContext(ctx, "SELECT x.a FROM MATCH (x)"))

	var a int
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&a))
	require.Equal(t, 123, a)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}
