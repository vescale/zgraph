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

type DataType byte

const (
	DataTypeString DataType = iota
	DataTypeBoolean
	DataTypeInteger
	DataTypeFloat
	DataTypeDouble
	DataTypeDecimal
	DataTypeDate
	DataTypeTime
	DataTypeTimeWithZone
	DataTypeTimestamp
	DataTypeTimestampWithZone
)

func (d DataType) String() string {
	switch d {
	case DataTypeString:
		return "STRING"
	case DataTypeBoolean:
		return "BOOLEAN"
	case DataTypeInteger:
		return "INTEGER"
	case DataTypeFloat:
		return "FLOAT"
	case DataTypeDouble:
		return "DOUBLE"
	case DataTypeDecimal:
		return "DECIMAL"
	case DataTypeDate:
		return "DATE"
	case DataTypeTime:
		return "TIME"
	case DataTypeTimeWithZone:
		return "TIME WITH TIME ZONE"
	case DataTypeTimestamp:
		return "TIMESTAMP"
	case DataTypeTimestampWithZone:
		return "TIMESTAMP WITH TIME ZONE"
	default:
		return fmt.Sprintf("UNKNOWN<%d>", d)
	}
}
