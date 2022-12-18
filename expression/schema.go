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

package expression

import "strings"

// Schema stands for the row schema information get from input.
type Schema struct {
	Columns []*Column
}

// NewSchema returns a schema made by its parameter.
func NewSchema(columns ...*Column) *Schema {
	return &Schema{Columns: columns}
}

// Len returns the number of columns in schema.
func (s *Schema) Len() int {
	return len(s.Columns)
}

// Clone copies the total schema.
func (s *Schema) Clone() *Schema {
	columns := make([]*Column, 0, s.Len())
	for _, col := range s.Columns {
		columns = append(columns, col.Clone().(*Column))
	}
	schema := NewSchema(columns...)
	return schema
}

// String implements fmt.Stringer interface.
func (s *Schema) String() string {
	colStrs := make([]string, 0, len(s.Columns))
	for _, col := range s.Columns {
		colStrs = append(colStrs, col.String())
	}
	return "Columns: [" + strings.Join(colStrs, ",") + "]"
}

func (s *Schema) ColumnIndex(col *Column) int {
	for i, c := range s.Columns {
		if c.ID == col.ID {
			return i
		}
	}
	return -1
}
