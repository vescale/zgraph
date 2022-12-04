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
	Next() error
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
func (e emptyResultSet) Next() error {
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
