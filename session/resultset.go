// Copyright 2022 zGraph Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"context"

	"github.com/vescale/zgraph/datum"
	"github.com/vescale/zgraph/executor"
)

// ResultSet represents the result of a query.
type ResultSet interface {
	Columns() []string
	// Valid reports whether the current result set valid.
	Valid() bool
	// Next advances the current result set to the next row of query result.
	Next(ctx context.Context) error
	// Row returns the current row of query result.
	Row() datum.Row
	// Close closes the current result set, which will release all query intermediate resources..
	Close() error
}

type emptyResultSet struct{}

// Columns implements the ResultSet.Columns.
func (e emptyResultSet) Columns() []string {
	return nil
}

// Valid implements the ResultSet.Valid.
func (e emptyResultSet) Valid() bool {
	return false
}

// Next implements the ResultSet.Next.
func (e emptyResultSet) Next(_ context.Context) error {
	return nil
}

// Row implements the ResultSet.Row.
func (e emptyResultSet) Row() datum.Row {
	return nil
}

// Close implements the ResultSet.Close.
func (e emptyResultSet) Close() error {
	return nil
}

// queryResultSet is a wrapper of executor.RecordSet. It
type queryResultSet struct {
	valid bool
	row   datum.Row
	exec  executor.Executor
}

func newQueryResultSet(exec executor.Executor) ResultSet {
	return &queryResultSet{valid: true, exec: exec}
}

// Columns implements the ResultSet.Columns.
func (q *queryResultSet) Columns() []string {
	cols := make([]string, len(q.exec.Columns()))
	for i, col := range q.exec.Columns() {
		cols[i] = col.Name.O
	}
	return cols
}

// Valid implements the ResultSet.Valid.
func (q *queryResultSet) Valid() bool {
	return q.valid
}

// Next implements the ResultSet.Next.
func (q *queryResultSet) Next(ctx context.Context) error {
	r, err := q.exec.Next(ctx)
	if err != nil {
		return err
	}
	q.row = r
	if r == nil {
		q.valid = false
	}
	return nil
}

// Row implements the ResultSet.Row.
func (q *queryResultSet) Row() datum.Row {
	return q.row
}

// Close implements the ResultSet interface.
func (q *queryResultSet) Close() error {
	q.valid = false
	q.row = nil
	return q.exec.Close()
}
