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
	"bufio"
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"
	_ "github.com/vescale/zgraph"
)

// This file implements an end-to-end test framework for zGraph. The test files are located
// in the testdata/logic_test directory. Each file is written in Test-Script format, which
// is designed by SQLite. See https://www.sqlite.org/sqllogictest/.
//
// Test scripts are line-oriented ASCII text files. Lines starting with a '#' character are
// comments and are ignored. Test scripts consist of zero or more records. A record is a single
// statement or query or a control record. Each record is separated from its neighbors by one
// or more blank line
//
// Currently, TestLogic only supports statement and query records. The syntax is as follows:
//
//  - statement ok
//    Run the statement and expect it to succeed.
//    e.g.
//      statement ok
//      CREATE GRAPH g
//
//  - statement|query error <regexp>
//    Run the statement or query and expect it to fail with an error message matching the regexp.
//    e.g.
//      statement error graph g already exists
//      CREATE GRAPH g
//
//  - query <type-string> <sort-mode> <label>
//    Run the query and expect it to succeed. The result is compared with the expected results.
//    The expected results is separated by '----' after the query statement. If '----' is omitted,
//    the query is expected to return empty set or results hash matches the previous query with
//    the same label.
//
//    The <type-string> argument to the query statement is a short string that specifies the
//    number of result columns and the expected datatype of each result column. There is one
//    character in the <type-string> for each result column. The characters codes are "T" for
//    a text result, "I" for an integer result, and "R" for a floating-point result.
//
//    The <sort-mode> argument is optional, which specifies how the result rows should be
//    sorted before comparing with the expected results. If <sort-mode> is present, it must
//    be either "nosort", "rowsort", "valuesort".
//    - "nosort" means that the result rows should not be sorted before comparing with the
//      expected results, which is the default behavior.
//    - "rowsort" means that the result rows should be sorted by rows before comparing with
//      the expected results.
//    - "valuesort" is similar to "rowsort", but the results are sorted by values, regardless
//      of how row groupings.
//
//    The <label> argument is optional. If present, the test runner will compute a hash of
//    the results. If the same label is reused, the results must be the same.
//
//    In the results section, integer values are rendered as if by printf("%d"). Floating
//    point values are rendered as if by printf("%.3f"). NULL values are rendered as "NULL".
//    Empty strings are rendered as "(empty)". Within non-empty strings, all control characters
//    and unprintable characters are rendered as "@".
//
//    e.g.
//      query I rowsort
//      SELECT n.name FROM MATCH (n)
//      ----
//      Alice
//      Bob

const logicTestPath = "testdata/logic_test"

func TestLogic(t *testing.T) {
	err := filepath.Walk(logicTestPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		t.Run(info.Name(), func(t *testing.T) {
			t.Parallel()

			db, err := sql.Open("zgraph", t.TempDir())
			require.NoError(t, err)
			defer db.Close()

			conn, err := db.Conn(context.Background())
			require.NoError(t, err)
			defer conn.Close()

			lt := logicTest{
				t:        t,
				conn:     conn,
				labelMap: make(map[string]string),
			}
			lt.run(path)
		})
		return nil
	})
	require.NoError(t, err)
}

type lineScanner struct {
	*bufio.Scanner
	line int
}

func (ls *lineScanner) Scan() bool {
	if ls.Scanner.Scan() {
		ls.line++
		return true
	}
	return false
}

type logicTest struct {
	t    *testing.T
	conn *sql.Conn

	// labelMap is a map from label to hash.
	labelMap map[string]string
}

func (lt *logicTest) run(path string) {
	t := lt.t
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	s := lineScanner{Scanner: bufio.NewScanner(f)}
	for s.Scan() {
		line := s.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		cmd := fields[0]
		switch cmd {
		case "statement":
			stmt := logicStatement{
				pos: fmt.Sprintf("%s:%d", path, s.line),
			}

			if len(fields) < 2 {
				t.Fatalf("%s: statement command should have at least 2 arguments", stmt.pos)
			}

			if fields[1] == "error" {
				expectedErr := strings.TrimSpace(strings.TrimPrefix(line, "statement"))
				expectedErr = strings.TrimSpace(strings.TrimPrefix(expectedErr, "error"))
				stmt.expectedErr = expectedErr
			}

			var sqlStr strings.Builder
			for s.Scan() {
				line = s.Text()
				if strings.TrimSpace(line) == "" {
					break
				}
				if line == "----" {
					t.Fatalf("%s:%d: unexpected '----' after a statement", path, s.line)
				}
				fmt.Fprintf(&sqlStr, "\n%s", line)
			}
			stmt.sql = sqlStr.String()
			lt.execStatement(stmt)
		case "query":
			query := logicQuery{}
			query.pos = fmt.Sprintf("%s:%d", path, s.line)

			if len(fields) < 2 {
				t.Fatalf("%s: query command should have at least 2 arguments", query.pos)
			}

			if fields[1] == "error" {
				expectedErr := strings.TrimSpace(strings.TrimPrefix(line, "query"))
				expectedErr = strings.TrimSpace(strings.TrimPrefix(expectedErr, "error"))
				query.expectedErr = expectedErr
			} else {
				query.typeStr = fields[1]
				query.sorter = noSort
				if len(fields) >= 3 {
					switch fields[2] {
					case "nosort":
					case "rowsort":
						query.sorter = rowSort
					case "valuesort":
						query.sorter = valueSort
					default:
						t.Fatalf("%s:%d unknown sort mode: %s", path, s.line, fields[2])
					}
				}
				if len(fields) >= 4 {
					query.label = fields[3]
				}

				// Parse SQL query.
				var sqlStr strings.Builder
				var hasSeparator bool
				for s.Scan() {
					line = s.Text()
					if strings.TrimSpace(line) == "" {
						break
					}
					if line == "----" {
						if query.expectedErr != "" {
							t.Fatalf("%s:%d unexpected '----' after a query that expects an error", path, s.line)
						}
						hasSeparator = true
						break
					}
					fmt.Fprintf(&sqlStr, "\n%s", line)
				}
				query.sql = sqlStr.String()

				// Parse expected results.
				if hasSeparator {
					for s.Scan() {
						line = s.Text()
						if strings.TrimSpace(line) == "" {
							break
						}
						query.expectedResults = append(query.expectedResults, strings.Fields(line)...)
					}
				}
			}

			lt.execQuery(query)
		default:
			t.Fatalf("%s:%d unknown command: %s", path, s.line, cmd)
		}

	}
}

func (lt *logicTest) execStatement(stmt logicStatement) {
	t := lt.t

	_, err := lt.conn.ExecContext(context.Background(), stmt.sql)
	if stmt.expectedErr != "" {
		require.Error(t, err)
		require.Regexp(t, stmt.expectedErr, err.Error())
	} else {
		require.NoError(t, err)
	}
}

func (lt *logicTest) execQuery(query logicQuery) {
	t := lt.t

	rows, err := lt.conn.QueryContext(context.Background(), query.sql)
	if query.expectedErr != "" {
		require.Error(t, err)
		require.Regexp(t, query.expectedErr, err.Error())
	} else {
		require.NoError(t, err)
		defer rows.Close()
	}

	var values []string
	numCols := len(query.typeStr)
	for rows.Next() {
		cols, err := rows.Columns()
		require.NoError(t, err)
		require.Equal(t, numCols, len(cols), "number of columns mismatch")

		dest := make([]interface{}, len(cols))
		for i := range query.typeStr {
			switch query.typeStr[i] {
			case 'T':
				dest[i] = &sql.NullString{}
			case 'I':
				dest[i] = &sql.NullInt64{}
			case 'R':
				dest[i] = &sql.NullFloat64{}
			default:
				t.Fatalf("unknown type character: %c", query.typeStr[i])
			}
		}
		require.NoError(t, rows.Scan(dest...))

		for i := range dest {
			switch query.typeStr[i] {
			case 'T':
				val := *dest[i].(*sql.NullString)
				s := val.String
				if !val.Valid {
					s = "NULL"
				}
				if s == "" {
					s = "(empty)"
				}
				s = strings.Map(func(r rune) rune {
					if unicode.IsControl(r) {
						return '@'
					}
					return r
				}, s)
				// Replace consecutive spaces with a single space.
				s = strings.Join(strings.Fields(s), " ")
				values = append(values, s)
			case 'I':
				val := *dest[i].(*sql.NullInt64)
				if val.Valid {
					values = append(values, fmt.Sprintf("%d", val.Int64))
				} else {
					values = append(values, "NULL")
				}
			case 'R':
				val := *dest[i].(*sql.NullFloat64)
				if val.Valid {
					values = append(values, fmt.Sprintf("%.3f", val.Float64))
				} else {
					values = append(values, "NULL")
				}
			}
		}
	}
	require.NoError(t, rows.Err())

	values = query.sorter(numCols, values)
	// Format values so that they can be compared with expected results.
	values = strings.Fields(strings.Join(values, " "))

	if len(query.expectedResults) > 0 || query.label == "" {
		// If there are expected results, or if there is no label, then the results must match.
		require.Equal(t, query.expectedResults, values, "%s: results mismatch", query.pos)
	}

	if query.label != "" {
		hash := hashResults(values)
		if prevHash, ok := lt.labelMap[query.label]; ok {
			require.Equal(t, prevHash, hash, "%s: results for label %s mismatch", query.pos, query.label)
		}
		lt.labelMap[query.label] = hash
	}
}

func hashResults(results []string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(strings.Join(results, " "))))
}

type logicStatement struct {
	pos         string
	sql         string
	expectedErr string
}

type logicSorter func(numCols int, values []string) []string

func noSort(_ int, values []string) []string {
	return values
}

type rowSorter struct {
	numCols int
	values  []string
}

func (rs *rowSorter) Len() int {
	return len(rs.values) / rs.numCols
}

func (rs *rowSorter) Less(i, j int) bool {
	a := rs.row(i)
	b := rs.row(j)
	for k := 0; k < rs.numCols; k++ {
		if a[k] != b[k] {
			return a[k] < b[k]
		}
	}
	return false
}

func (rs *rowSorter) Swap(i, j int) {
	a := rs.row(i)
	b := rs.row(j)
	for k := 0; k < rs.numCols; k++ {
		a[k], b[k] = b[k], a[k]
	}
}

func (rs *rowSorter) row(i int) []string {
	return rs.values[i*rs.numCols : (i+1)*rs.numCols]
}

func rowSort(numCols int, values []string) []string {
	rs := rowSorter{
		numCols: numCols,
		values:  values,
	}
	sort.Sort(&rs)
	return rs.values
}

func valueSort(_ int, values []string) []string {
	sort.Strings(values)
	return values
}

type logicQuery struct {
	logicStatement

	typeStr         string
	sorter          logicSorter
	label           string
	expectedResults []string
}
