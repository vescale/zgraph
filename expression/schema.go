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
	Fields []*PropertyRef
}

// NewSchema returns a schema made by its parameter.
func NewSchema(fields ...*PropertyRef) *Schema {
	return &Schema{Fields: fields}
}

// Len returns the number of columns in schema.
func (s *Schema) Len() int {
	return len(s.Fields)
}

// Clone copies the total schema.
func (s *Schema) Clone() *Schema {
	fields := make([]*PropertyRef, 0, s.Len())
	for _, field := range s.Fields {
		fields = append(fields, field.Clone())
	}
	schema := NewSchema(fields...)
	return schema
}

// String implements fmt.Stringer interface.
func (s *Schema) String() string {
	colStrs := make([]string, 0, len(s.Fields))
	for _, col := range s.Fields {
		colStrs = append(colStrs, col.String())
	}
	return "Fields: [" + strings.Join(colStrs, ",") + "]"
}
