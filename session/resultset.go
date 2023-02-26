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
	"fmt"

	"github.com/vescale/zgraph/datum"
	"github.com/vescale/zgraph/executor"
	"github.com/vescale/zgraph/internal/chunk"
	"github.com/vescale/zgraph/planner"
)

// Field represents a field information.
type Field struct {
	Graph        string
	Label        string
	OrgLabel     string
	Name         string
	OrgName      string
	ColumnLength uint32
}

// ResultSet represents the result of a query.
type ResultSet interface {
	// Fields returns the fields information of the current query.
	Fields() []*Field
	// Valid reports whether the current result set valid.
	Valid() bool
	// Next advances the current result set to the next row of query result.
	Next(ctx context.Context) error
	// Scan reads the current row.
	Scan(fields ...interface{}) error
	// Close closes the current result set, which will release all query intermediate resources..
	Close() error
}

type emptyResultSet struct{}

// Fields implements the ResultSet interface.
func (e emptyResultSet) Fields() []*Field {
	return []*Field{}
}

// Valid implements the ResultSet interface.
func (e emptyResultSet) Valid() bool {
	return false
}

// Next implements the ResultSet interface.
func (e emptyResultSet) Next(_ context.Context) error {
	return nil
}

// Scan implements the ResultSet interface.
func (e emptyResultSet) Scan(fields ...interface{}) error {
	return nil
}

// Close implements the ResultSet interface.
func (e emptyResultSet) Close() error {
	return nil
}

// queryResultSet is a wrapper of executor.RecordSet. It
type queryResultSet struct {
	valid  bool
	alloc  *chunk.Allocator
	row    datum.Row
	fields []*Field
	exec   executor.Executor
}

func retrieveFields(cols planner.ResultColumns) []*Field {
	fields := make([]*Field, 0, len(cols))
	for _, col := range cols {
		fields = append(fields, &Field{
			Name: col.Name.O,
		})
	}
	return fields
}

func newQueryResultSet(exec executor.Executor) ResultSet {
	alloc := chunk.NewAllocator()
	return &queryResultSet{
		alloc:  alloc,
		valid:  true,
		exec:   exec,
		fields: retrieveFields(exec.Columns()),
		// TODO: implement row
		// row:  exec.NewChunk(alloc),
	}
}

// Fields implements the ResultSet interface.
func (q *queryResultSet) Fields() []*Field {
	return q.fields
}

// Valid implements the ResultSet interface.
func (q *queryResultSet) Valid() bool {
	return q.valid
}

// Next implements the ResultSet interface.
func (q *queryResultSet) Next(ctx context.Context) error {
	r, err := q.exec.Next(ctx)
	if err != nil {
		return err
	}
	if r == nil {
		q.valid = false
		return nil
	}
	q.row = r
	return nil
}

// Scan implements the ResultSet interface.
func (q *queryResultSet) Scan(fields ...any) error {
	if len(fields) != len(q.fields) {
		return ErrFieldCountNotMatch
	}
	for i, field := range fields {
		if err := assignField(field, q.row[i]); err != nil {
			return err
		}
	}
	return nil
}

// Close implements the ResultSet interface.
func (q *queryResultSet) Close() error {
	q.valid = false
	return q.exec.Close()
}

func assignField(field any, d datum.Datum) error {
	switch f := field.(type) {
	case *string:
		*f = d.String()
	case *int:
		*f = int(datum.AsInt(d))
	case *int64:
		*f = datum.AsInt(d)
	default:
		// TODO: support more types
		return fmt.Errorf("unsupported field type: %T", field)
	}
	return nil
}
