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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datum

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/vescale/zgraph/types"
)

const Null = null(0)

var (
	_ Datum = NewBool(false)
	_ Datum = NewInt(0)
	_ Datum = NewFloat(0.0)
	_ Datum = NewString("")
	_ Datum = NewBytes(nil)
	_ Datum = &Decimal{}
	_ Datum = &Date{}
	_ Datum = &Time{}
	_ Datum = &TimeTZ{}
	_ Datum = &Timestamp{}
	_ Datum = &TimestampTZ{}
	_ Datum = &Interval{}
	_ Datum = &Vertex{}
	_ Datum = &Edge{}
)

type Datum interface {
	Type() types.T
	String() string
}

type Row []Datum

type null int

func (null) Type() types.T {
	return types.Unknown
}

func (null) String() string {
	return "NULL"
}

type Bool bool

func (Bool) Type() types.T {
	return types.Bool
}

func (b Bool) String() string {
	if b {
		return "TRUE"
	} else {
		return "FALSE"
	}
}

func NewBool(b bool) *Bool {
	d := Bool(b)
	return &d
}

func ParseBool(s string) (*Bool, error) {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return nil, err
	}
	return NewBool(b), nil
}

func MustBeBool(d Datum) Bool {
	b, ok := d.(*Bool)
	if !ok {
		panic(fmt.Sprintf("expected *Bool, got %T", d))
	}
	return *b
}

type Int int64

func (Int) Type() types.T {
	return types.Int
}

func (i Int) String() string {
	return strconv.FormatInt(int64(i), 10)
}

func NewInt(i int64) *Int {
	d := Int(i)
	return &d
}

func MustBeInt(d Datum) Int {
	i, ok := d.(*Int)
	if !ok {
		panic(fmt.Sprintf("expected *Int, got %T", d))
	}
	return *i
}

type Float float64

func (Float) Type() types.T {
	return types.Float
}

func (f Float) String() string {
	return strconv.FormatFloat(float64(f), 'g', -1, 64)
}

func NewFloat(f float64) *Float {
	d := Float(f)
	return &d
}

func MustBeFloat(d Datum) Float {
	f, ok := d.(*Float)
	if !ok {
		panic(fmt.Sprintf("expected *Float, got %T", d))
	}
	return *f
}

type String string

func (String) Type() types.T {
	return types.String
}

func (s String) String() string {
	return string(s)
}

func NewString(s string) *String {
	d := String(s)
	return &d
}

func MustBeString(d Datum) String {
	s, ok := d.(*String)
	if !ok {
		panic(fmt.Sprintf("expected *String, got %T", d))
	}
	return *s
}

type Bytes []byte

func (Bytes) Type() types.T {
	return types.Bytes
}

func (b Bytes) String() string {
	return string(b)
}

func NewBytes(b []byte) *Bytes {
	d := Bytes(b)
	return &d
}

func MustBeBytes(d Datum) Bytes {
	b, ok := d.(*Bytes)
	if !ok {
		panic(fmt.Sprintf("expected *Bytes, got %T", d))
	}
	return *b
}

type Decimal struct {
	apd.Decimal
}

func (Decimal) Type() types.T {
	return types.Decimal
}

func (d Decimal) String() string {
	return d.Decimal.Text('g')
}

func ParseDecimal(s string) (*Decimal, error) {
	d := &Decimal{}
	_, _, err := d.SetString(s)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func MustBeDecimal(d Datum) Decimal {
	dec, ok := d.(*Decimal)
	if !ok {
		panic(fmt.Sprintf("expected *Decimal, got %T", d))
	}
	return *dec
}

const (
	secondsPerDay = 24 * 60 * 60
	dateLayout    = "2006-01-02"
)

type Date struct {
	days int32 // days since unix epoch
}

func (*Date) Type() types.T {
	return types.Date
}

func (d *Date) String() string {
	return time.Unix(int64(d.days)*secondsPerDay, 0).Format(dateLayout)
}

func (d *Date) UnixEpochDays() int32 {
	return d.days
}

func NewDateFromUnixEpochDays(days int32) *Date {
	return &Date{days: days}
}

func ParseDate(s string) (*Date, error) {
	t, err := time.Parse(dateLayout, s)
	if err != nil {
		return nil, err
	}
	return &Date{days: int32(t.Unix() / secondsPerDay)}, nil
}

func MustBeDate(d Datum) Date {
	date, ok := d.(*Date)
	if !ok {
		panic(fmt.Sprintf("expected *Date, got %T", d))
	}
	return *date
}

const (
	secondsPerHour   = 60 * 60
	secondsPerMinute = 60
	minutesPerHour   = 60
)

type TimeOfDay int32 // seconds since midnight

func (t *TimeOfDay) Hour() int {
	return int(*t) / secondsPerHour
}

func (t *TimeOfDay) Minute() int {
	return (int(*t) % secondsPerHour) / secondsPerMinute
}

func (t *TimeOfDay) Second() int {
	return int(*t) % secondsPerMinute
}

type Time struct {
	TimeOfDay
}

func (*Time) Type() types.T {
	return types.Time
}

func (t *Time) String() string {
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
}

func NewTime(t TimeOfDay) *Time {
	return &Time{TimeOfDay: t}
}

var timeFormatRegex = regexp.MustCompile(`^(\d{2}):(\d{2}):(\d{2})$`)

func ParseTime(s string) (*Time, error) {
	m := timeFormatRegex.FindStringSubmatch(s)
	if len(m) != 4 {
		return nil, fmt.Errorf("could not parse %q as Time", s)
	}
	hour, _ := strconv.Atoi(m[1])
	minute, _ := strconv.Atoi(m[2])
	second, _ := strconv.Atoi(m[3])
	if hour > 23 {
		return nil, errors.New("time hour out of range")
	}
	if minute > 59 {
		return nil, errors.New("time minute out of range")
	}
	if second > 59 {
		return nil, errors.New("time second out of range")
	}
	return &Time{TimeOfDay: TimeOfDay(hour*secondsPerHour + minute*secondsPerMinute + second)}, nil
}

func MustBeTime(d Datum) Time {
	t, ok := d.(*Time)
	if !ok {
		panic(fmt.Sprintf("expected *Time, got %T", d))
	}
	return *t
}

type TimeTZ struct {
	TimeOfDay
	offsetMinutes int32
}

func (t *TimeTZ) Type() types.T {
	return types.TimeTZ
}

func (t *TimeTZ) String() string {
	offsetHour := t.offsetMinutes / minutesPerHour
	offsetMinute := t.offsetMinutes % minutesPerHour
	if t.offsetMinutes >= 0 {
		return fmt.Sprintf("%02d:%02d:%02d+%02d:%02d", t.Hour(), t.Minute(), t.Second(), offsetHour, offsetMinute)
	} else {
		return fmt.Sprintf("%02d:%02d:%02d%03d:%02d", t.Hour(), t.Minute(), t.Second(), offsetHour, offsetMinute)
	}
}

var timeTZFormatRegex = regexp.MustCompile(`^(\d{2}):(\d{2}):(\d{2})([+-]\d{2}):(\d{2})$`)

func ParseTimeTZ(s string) (*TimeTZ, error) {
	m := timeTZFormatRegex.FindStringSubmatch(s)
	if len(m) != 6 {
		return nil, fmt.Errorf("could not parse %s as TimeTZ", s)
	}
	hour, _ := strconv.Atoi(m[1])
	minute, _ := strconv.Atoi(m[2])
	second, _ := strconv.Atoi(m[3])
	if hour > 23 {
		return nil, errors.New("time hour out of range")
	}
	if minute > 59 {
		return nil, errors.New("time minute out of range")
	}
	if second > 59 {
		return nil, errors.New("time second out of range")
	}
	offsetHour, _ := strconv.Atoi(m[4])
	offsetMinute, _ := strconv.Atoi(m[5])
	if offsetHour > 12 {
		return nil, errors.New("time zone offset hour out of range")
	}
	if offsetMinute > 59 {
		return nil, errors.New("time zone offset minute out of range")
	}
	return &TimeTZ{
		TimeOfDay:     TimeOfDay(hour*secondsPerHour + minute*secondsPerMinute + second),
		offsetMinutes: int32(offsetHour*minutesPerHour + offsetMinute),
	}, nil
}

func MustBeTimeTZ(d Datum) TimeTZ {
	t, ok := d.(*TimeTZ)
	if !ok {
		panic(fmt.Sprintf("expected *TimeTZ, got %T", d))
	}
	return *t
}

func ParseTimeOrTimeTZ(s string) (*Time, *TimeTZ, error) {
	t, err := ParseTime(s)
	if err == nil {
		return t, nil, nil
	}
	ttz, err := ParseTimeTZ(s)
	if err == nil {
		return nil, ttz, nil
	}
	return nil, nil, fmt.Errorf("could not parse %q as Time or TimeTZ", s)
}

const timestampLayout = "2006-01-02 15:04:05"

type Timestamp struct {
	time.Time
}

func (t *Timestamp) Type() types.T {
	return types.Timestamp
}

func (t *Timestamp) String() string {
	return t.UTC().Format(timestampLayout)
}

func ParseTimestamp(s string) (*Timestamp, error) {
	t, err := time.Parse(timestampLayout, s)
	if err != nil {
		return nil, err
	}
	return &Timestamp{Time: t}, nil
}

func MustBeTimestamp(d Datum) Timestamp {
	t, ok := d.(*Timestamp)
	if !ok {
		panic(fmt.Sprintf("expected *Timestamp, got %T", d))
	}
	return *t
}

const timestampTZLayout = "2006-01-02 15:04:05-07:00"

type TimestampTZ struct {
	time.Time
}

func (t *TimestampTZ) Type() types.T {
	return types.TimestampTZ
}

func (t *TimestampTZ) String() string {
	return t.Format(timestampTZLayout)
}

func ParseTimestampTZ(s string) (*TimestampTZ, error) {
	t, err := time.Parse(timestampTZLayout, s)
	if err != nil {
		return nil, err
	}
	return &TimestampTZ{Time: t}, nil
}

func MustBeTimestampTZ(d Datum) TimestampTZ {
	t, ok := d.(*TimestampTZ)
	if !ok {
		panic(fmt.Sprintf("expected *TimestampTZ, got %T", d))
	}
	return *t
}

func ParseTimestampOrTimestampTZ(s string) (*Timestamp, *TimestampTZ, error) {
	t, err := ParseTimestamp(s)
	if err == nil {
		return t, nil, nil
	}
	ttz, err := ParseTimestampTZ(s)
	if err == nil {
		return nil, ttz, nil
	}
	return nil, nil, fmt.Errorf("could not parse %q as Timestamp or TimestampTZ", s)
}

type IntervalUnit uint8

const (
	IntervalUnitYear IntervalUnit = iota
	IntervalUnitMonth
	IntervalUnitDay
	IntervalUnitHour
	IntervalUnitMinute
	IntervalUnitSecond
)

type Interval struct {
	months  int64
	days    int64
	seconds int64
}

func (i *Interval) Type() types.T {
	return types.Interval
}

func (i *Interval) String() string {
	if i.months != 0 {
		if i.months%12 == 0 {
			return fmt.Sprintf("%d YEAR", i.months/12)
		} else {
			return fmt.Sprintf("%d MONTH", i.months)
		}
	} else if i.days != 0 {
		return fmt.Sprintf("%d DAY", i.days)
	} else {
		if i.seconds%3600 == 0 {
			return fmt.Sprintf("%d HOUR", i.seconds/3600)
		} else if i.seconds%60 == 0 {
			return fmt.Sprintf("%d MINUTE", i.seconds/60)
		} else {
			return fmt.Sprintf("%d SECOND", i.seconds)
		}
	}
}

func NewInterval(dur int64, unit IntervalUnit) *Interval {
	switch unit {
	case IntervalUnitYear:
		return &Interval{months: dur * 12}
	case IntervalUnitMonth:
		return &Interval{months: dur}
	case IntervalUnitDay:
		return &Interval{days: dur}
	case IntervalUnitHour:
		return &Interval{seconds: dur * 3600}
	case IntervalUnitMinute:
		return &Interval{seconds: dur * 60}
	case IntervalUnitSecond:
		return &Interval{seconds: dur}
	default:
		panic(fmt.Sprintf("unknown interval unit %d", unit))
	}
}

func MustBeInterval(d Datum) Interval {
	i, ok := d.(*Interval)
	if !ok {
		panic(fmt.Sprintf("expected *Interval, got %T", d))
	}
	return *i
}

type Vertex struct {
	ID     int64
	Labels []string
	Props  map[string]Datum
}

func (v *Vertex) Type() types.T {
	return types.Vertex
}

func (v *Vertex) String() string {
	return fmt.Sprintf("VERTEX(%d)", v.ID)
}

func MustBeVertex(d Datum) Vertex {
	v, ok := d.(*Vertex)
	if !ok {
		panic(fmt.Sprintf("expected *Vertex, got %T", d))
	}
	return *v
}

type Edge struct {
	SrcID  int64
	DstID  int64
	Labels []string
	Props  map[string]Datum
}

func (e *Edge) Type() types.T {
	return types.Edge
}

func (e *Edge) String() string {
	return fmt.Sprintf("EDGE(%d, %d)", e.SrcID, e.DstID)
}

func MustBeEdge(d Datum) Edge {
	e, ok := d.(*Edge)
	if !ok {
		panic(fmt.Sprintf("expected *Edge, got %T", d))
	}
	return *e
}
