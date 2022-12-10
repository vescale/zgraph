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
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import "fmt"

type FieldType uint8

const (
	FieldTypeString FieldType = iota
	FieldTypeBoolean
	FieldTypeInteger
	FieldTypeFloat
	FieldTypeDouble
	FieldTypeDecimal
	FieldTypeDate
	FieldTypeTime
	FieldTypeTimeWithTimeZone
	FieldTypeTimestamp
	FieldTypeTimestampWithTimeZone
)

func (f FieldType) String() string {
	switch f {
	case FieldTypeString:
		return "STRING"
	case FieldTypeBoolean:
		return "BOOLEAN"
	case FieldTypeInteger:
		return "INTEGER"
	case FieldTypeFloat:
		return "FLOAT"
	case FieldTypeDouble:
		return "DOUBLE"
	case FieldTypeDecimal:
		return "DECIMAL"
	case FieldTypeDate:
		return "DATE"
	case FieldTypeTime:
		return "TIME"
	case FieldTypeTimeWithTimeZone:
		return "TIME WITH TIME ZONE"
	case FieldTypeTimestamp:
		return "TIMESTAMP"
	case FieldTypeTimestampWithTimeZone:
		return "TIMESTAMP WITH TIME ZONE"
	default:
		return fmt.Sprintf("UNKNOWN<%d>", f)
	}
}
