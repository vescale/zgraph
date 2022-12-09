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

import (
	"fmt"
	"math"
	"strconv"
)

type DateTimeField uint8

const (
	DateTimeFieldYear   DateTimeField = 1
	DateTimeFieldMonth  DateTimeField = 2
	DateTimeFieldDay    DateTimeField = 3
	DateTimeFieldHour   DateTimeField = 4
	DateTimeFieldMinute DateTimeField = 5
	DateTimeFieldSecond DateTimeField = 6
)

func (f DateTimeField) String() string {
	switch f {
	case DateTimeFieldYear:
		return "YEAR"
	case DateTimeFieldMonth:
		return "MONTH"
	case DateTimeFieldDay:
		return "DAY"
	case DateTimeFieldHour:
		return "HOUR"
	case DateTimeFieldMinute:
		return "MINUTE"
	case DateTimeFieldSecond:
		return "SECOND"
	default:
		return fmt.Sprintf("UNKNOWN<%d>", f)
	}
}

// Interval represents the interval type.
// The high 32 bits are used to store the DateTimeField. The low 32 bits are used to store the value.
// For year, month, day, hour, minute, the value is int32. For second, the value is float32.
type Interval uint64

func NewInterval(value string, dateTimeField DateTimeField) (Interval, error) {
	switch dateTimeField {
	case DateTimeFieldYear, DateTimeFieldMonth, DateTimeFieldDay, DateTimeFieldHour, DateTimeFieldMinute:
		v, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return 0, err
		}
		return Interval(uint64(dateTimeField)<<32 | uint64(v)), nil
	case DateTimeFieldSecond:
		v, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return 0, err
		}
		return Interval(uint64(dateTimeField)<<32 | uint64(math.Float32bits(float32(v)))), nil
	default:
		return 0, fmt.Errorf("invalid DateTimeField: %v", dateTimeField)
	}
}

func (i Interval) Field() DateTimeField {
	return DateTimeField(i >> 32)
}

func (i Interval) Int32Value() int32 {
	return int32(i)
}

func (i Interval) Float32Value() float32 {
	return math.Float32frombits(uint32(i))
}

func (i Interval) StringValue() string {
	if i.Field() == DateTimeFieldSecond {
		return fmt.Sprintf("%v", i.Float32Value())
	} else {
		return fmt.Sprintf("%d", i.Int32Value())
	}
}
