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
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import "fmt"

type T uint8

const (
	Unknown     T = 0
	Bool        T = 1
	Int         T = 2
	Float       T = 3
	String      T = 4
	Bytes       T = 5
	Decimal     T = 6
	Date        T = 7
	Time        T = 8
	TimeTZ      T = 9
	Timestamp   T = 10
	TimestampTZ T = 11
	Interval    T = 12
	Vertex      T = 13
	Edge        T = 14
)

func (t T) String() string {
	switch t {
	case Unknown:
		return "Unknown"
	case Bool:
		return "Bool"
	case Int:
		return "Int"
	case Float:
		return "Float"
	case String:
		return "String"
	case Bytes:
		return "Bytes"
	case Decimal:
		return "Decimal"
	case Date:
		return "Date"
	case Time:
		return "Time"
	case TimeTZ:
		return "TimeTZ"
	case Timestamp:
		return "Timestamp"
	case TimestampTZ:
		return "TimestampTZ"
	case Interval:
		return "Interval"
	case Vertex:
		return "Vertex"
	case Edge:
		return "Edge"
	default:
		return fmt.Sprintf("unknown<%d>", t)
	}
}
