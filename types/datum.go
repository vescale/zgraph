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
	"strings"
)

// Kind represents the kind of datum.
type Kind uint8

const (
	KindNull      Kind = 0
	KindBool      Kind = 1
	KindInt64     Kind = 2
	KindUint64    Kind = 3
	KindFloat64   Kind = 4
	KindString    Kind = 5
	KindBytes     Kind = 6
	KindDecimal   Kind = 7
	KindDate      Kind = 8
	KindTime      Kind = 9
	KindTimestamp Kind = 10
	KindInterval  Kind = 11
	KindGraphVar  Kind = 12
)

type KindPair struct {
	First  Kind
	Second Kind
}

func NewKindPair(first, second Kind) KindPair {
	return KindPair{First: first, Second: second}
}

type Datum struct {
	k Kind   // datum kind
	i int64  // i can hold integer types, time, date, timestamp, interval
	b []byte // b can hold string, bytes
	d any    // d can hold decimal, graphvar
}

func NewDatum(val any) Datum {
	var d Datum
	d.SetValue(val)
	return d
}

func NewStringDatum(val string) Datum {
	var d Datum
	d.SetString(val)
	return d
}

func NewBoolDatum(val bool) Datum {
	var d Datum
	d.SetBool(val)
	return d
}

func (d *Datum) SetValue(val any) {
	switch x := val.(type) {
	case nil:
		d.SetNull()
	case bool:
		d.SetBool(x)
	case int:
		d.SetInt64(int64(x))
	case int64:
		d.SetInt64(x)
	case uint64:
		d.SetUint64(x)
	case float64:
		d.SetFloat64(x)
	case string:
		d.SetString(x)
	case []byte:
		d.SetBytes(x)
	case *Decimal:
		d.SetDecimal(x)
	case Date:
		d.SetDate(x)
	case Time:
		d.SetTime(x)
	case Timestamp:
		d.SetTimestamp(x)
	case Interval:
		d.SetInterval(x)
	case BinaryLiteral:
		d.SetBytes(x)
	case HexLiteral:
		d.SetBytes(x)
	case BitLiteral:
		d.SetBytes(x)
	case *GraphVar:
		d.SetGraphVar(x)
	default:
		panic(fmt.Sprintf("unexpected literval type %T", val))
	}
}

func (d *Datum) Kind() Kind {
	return d.k
}

func (d *Datum) IsNull() bool {
	return d.k == KindNull
}

// SetNull sets datum to nil.
func (d *Datum) SetNull() {
	d.k = KindNull
	d.b = nil
	d.d = nil
}

func (d *Datum) GetBool() bool {
	return d.i != 0
}

func (d *Datum) SetBool(b bool) {
	d.k = KindBool
	if b {
		d.i = 1
	} else {
		d.i = 0
	}
}

func (d *Datum) GetInt64() int64 {
	return d.i
}

func (d *Datum) SetInt64(i int64) {
	d.k = KindInt64
	d.i = i
}

func (d *Datum) GetUint64() uint64 {
	return uint64(d.i)
}

func (d *Datum) SetUint64(u uint64) {
	d.k = KindUint64
	d.i = int64(u)
}

func (d *Datum) GetFloat64() float64 {
	return math.Float64frombits(uint64(d.i))
}

func (d *Datum) SetFloat64(f float64) {
	d.k = KindFloat64
	d.i = int64(math.Float64bits(f))
}

func (d *Datum) GetString() string {
	return string(d.b)
}

func (d *Datum) SetString(s string) {
	d.k = KindString
	d.b = []byte(s)
}

func (d *Datum) SetBytesAsString(b []byte) {
	d.k = KindString
	d.b = b
}

func (d *Datum) GetBytes() []byte {
	return d.b
}

func (d *Datum) SetBytes(b []byte) {
	d.k = KindBytes
	d.b = b
}

func (d *Datum) GetDecimal() *Decimal {
	return d.d.(*Decimal)
}

func (d *Datum) SetDecimal(dec *Decimal) {
	d.k = KindDecimal
	d.d = dec
}

func (d *Datum) GetDate() Date {
	return NewDate(CoreTime(d.i))
}

func (d *Datum) SetDate(date Date) {
	d.k = KindDate
	d.i = int64(date.CoreTime())
}

func (d *Datum) GetTime() Time {
	return NewTime(CoreTime(d.i))
}

func (d *Datum) SetTime(t Time) {
	d.k = KindTime
	d.i = int64(t.CoreTime())
}

func (d *Datum) GetTimestamp() Timestamp {
	return NewTimestamp(CoreTime(d.i))
}

func (d *Datum) SetTimestamp(t Timestamp) {
	d.k = KindTimestamp
	d.i = int64(t.CoreTime())
}

func (d *Datum) GetInterval() Interval {
	return Interval(d.i)
}

func (d *Datum) SetInterval(i Interval) {
	d.k = KindInterval
	d.i = int64(i)
}

func (d *Datum) ToString() string {
	switch d.k {
	case KindNull:
		return "NULL"
	case KindBool:
		if d.GetBool() {
			return "TRUE"
		} else {
			return "FALSE"
		}
	case KindInt64:
		return strconv.FormatInt(d.GetInt64(), 10)
	case KindUint64:
		return strconv.FormatUint(d.GetUint64(), 10)
	case KindFloat64:
		return strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64)
	case KindString:
		return d.GetString()
	case KindBytes:
		return fmt.Sprintf("X'%X'", d.GetBytes())
	case KindDecimal:
		return d.GetDecimal().String()
	case KindDate:
		return d.GetDate().String()
	case KindTime:
		return d.GetTime().String()
	case KindTimestamp:
		return d.GetTimestamp().String()
	case KindInterval:
		return d.GetInterval().StringValue() + " " + d.GetInterval().Field().String()
	case KindGraphVar:
		return d.GetGraphVar().String()
	default:
		panic(fmt.Sprintf("unexpected datum kind %v", d.k))
	}
}

type GraphVar struct {
	ID         int64
	Labels     []string
	Properties map[string]Datum
}

func (v *GraphVar) String() string {
	return fmt.Sprintf("GraphVar{ID: %d, Labels: %s}", v.ID, strings.Join(v.Labels, ", "))
}

func (d *Datum) GetGraphVar() *GraphVar {
	return d.d.(*GraphVar)
}

func (d *Datum) SetGraphVar(v *GraphVar) {
	d.k = KindGraphVar
	d.d = v
}
