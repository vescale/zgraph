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

package tests_test

import (
	"database/sql"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
	_ "github.com/vescale/zgraph"
)

func runDataDrivenTest(t *testing.T, path string, initSQLs []string) {
	t.Helper()
	datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
		db, err := initDB(t.TempDir(), initSQLs)
		require.NoError(t, err)
		defer db.Close()

		rows, err := db.Query(d.Input)
		require.NoError(t, err)
		defer rows.Close()

		result, err := combineRows(rows)
		require.NoError(t, err)
		// Sort the result to make it deterministic.
		// TODO: It's not a good idea to sort the result, we should make query result deterministic instead.
		lines := strings.Split(strings.TrimSpace(result), "\n")
		sort.Strings(lines[1:])
		result = strings.Join(lines, "\n")
		return result
	})
}

func initDB(path string, sqls []string) (*sql.DB, error) {
	db, err := sql.Open("zgraph", path)
	if err != nil {
		return nil, err
	}
	for _, q := range sqls {
		_, err = db.Exec(q)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return db, nil
}

func combineRows(rows *sql.Rows) (string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	for i, col := range cols {
		sb.WriteString(col)
		if i+1 == len(cols) {
			sb.WriteByte('\n')
		} else {
			sb.WriteString(",")
		}
	}

	dest := make([]any, len(cols))
	for i := 0; i < len(cols); i++ {
		var anyStr sql.NullString
		dest[i] = &anyStr
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return "", err
		}
		for i := 0; i < len(cols); i++ {
			sb.WriteString(dest[i].(*sql.NullString).String)
			if i+1 == len(cols) {
				sb.WriteByte('\n')
			} else {
				sb.WriteString(",")
			}
		}
	}
	return sb.String(), nil
}
