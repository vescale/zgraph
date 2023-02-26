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

const Null = dNull(0)

type Datum interface {
	Type() types.T
	String() string
	isDatum()
}

func (dNull) isDatum()        {}
func (dBool) isDatum()        {}
func (dInt) isDatum()         {}
func (dFloat) isDatum()       {}
func (dString) isDatum()      {}
func (dBytes) isDatum()       {}
func (dDecimal) isDatum()     {}
func (*Date) isDatum()        {}
func (*Time) isDatum()        {}
func (*TimeTZ) isDatum()      {}
func (*Timestamp) isDatum()   {}
func (*TimestampTZ) isDatum() {}
func (*Interval) isDatum()    {}
func (*Vertex) isDatum()      {}
func (*Edge) isDatum()        {}

type Row []Datum

type dNull int

func (dNull) Type() types.T  { return types.Unknown }
func (dNull) String() string { return "NULL" }

type dBool bool

func (dBool) Type() types.T {
	return types.Bool
}

func (d dBool) String() string {
	if d {
		return "TRUE"
	} else {
		return "FALSE"
	}
}

func NewBool(b bool) Datum {
	return dBool(b)
}

func AsBool(d Datum) bool {
	v, err := TryAsBool(d)
	if err != nil {
		panic(err)
	}
	return v
}

func TryAsBool(d Datum) (bool, error) {
	switch v := d.(type) {
	case dBool:
		return bool(v), nil
	case dString:
		return strconv.ParseBool(string(v))
	default:
		return false, fmt.Errorf("cannot convert %T to bool", d)
	}
}

type dInt int64

func (dInt) Type() types.T {
	return types.Int
}

func (d dInt) String() string {
	return strconv.FormatInt(int64(d), 10)
}

func NewInt(i int64) Datum {
	return dInt(i)
}

func AsInt(d Datum) int64 {
	v, err := TryAsInt(d)
	if err != nil {
		panic(err)
	}
	return v
}

func TryAsInt(d Datum) (int64, error) {
	switch v := d.(type) {
	case dInt:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int", d)
	}
}

type dFloat float64

func (dFloat) Type() types.T {
	return types.Float
}

func (d dFloat) String() string {
	return strconv.FormatFloat(float64(d), 'g', -1, 64)
}

func NewFloat(f float64) Datum {
	return dFloat(f)
}

func AsFloat(d Datum) float64 {
	v, err := TryAsFloat(d)
	if err != nil {
		panic(err)
	}
	return v
}

func TryAsFloat(d Datum) (float64, error) {
	switch v := d.(type) {
	case dFloat:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float", d)
	}
}

type dString string

func (dString) Type() types.T {
	return types.String
}

func (d dString) String() string {
	return string(d)
}

func NewString(s string) Datum {
	return dString(s)
}

func AsString(d Datum) string {
	v, err := TryAsString(d)
	if err != nil {
		panic(err)
	}
	return v
}

func TryAsString(d Datum) (string, error) {
	switch v := d.(type) {
	case dString:
		return string(v), nil
	case dBytes:
		return string(v), nil
	default:
		return "", fmt.Errorf("cannot convert %T to string", d)
	}
}

type dBytes []byte

func (dBytes) Type() types.T {
	return types.Bytes
}

func (d dBytes) String() string {
	return string(d)
}

func NewBytes(b []byte) Datum {
	return dBytes(b)
}

func AsBytes(d Datum) []byte {
	v, err := TryAsBytes(d)
	if err != nil {
		panic(err)
	}
	return v
}

func TryAsBytes(d Datum) ([]byte, error) {
	switch v := d.(type) {
	case dBytes:
		return v, nil
	case dString:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to bytes", d)
	}
}

type dDecimal struct {
	*apd.Decimal
}

func (dDecimal) Type() types.T {
	return types.Decimal
}

func (d dDecimal) String() string {
	return d.Decimal.Text('g')
}

func NewDecimal(d *apd.Decimal) Datum {
	return dDecimal{d}
}

func ParseDecimal(s string) (Datum, error) {
	d := &apd.Decimal{}
	_, _, err := d.SetString(s)
	if err != nil {
		return nil, err
	}
	return NewDecimal(d), nil
}

func AsDecimal(d Datum) *apd.Decimal {
	v, err := TryAsDecimal(d)
	if err != nil {
		panic(err)
	}
	return v
}

func TryAsDecimal(d Datum) (*apd.Decimal, error) {
	switch v := d.(type) {
	case dDecimal:
		return v.Decimal, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to decimal", d)
	}
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

func AsDate(d Datum) *Date {
	v, err := TryAsDate(d)
	if err != nil {
		panic(err)
	}
	return v
}

func TryAsDate(d Datum) (*Date, error) {
	switch v := d.(type) {
	case *Date:
		return v, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to date", d)
	}
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

func AsTime(d Datum) *Time {
	v, err := TryAsTime(d)
	if err != nil {
		panic(err)
	}
	return v
}

func TryAsTime(d Datum) (*Time, error) {
	switch v := d.(type) {
	case *Time:
		return v, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to time", d)
	}
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

func AsTimeTZ(d Datum) *TimeTZ {
	v, err := TryAsTimeTZ(d)
	if err != nil {
		panic(err)
	}
	return v
}

func TryAsTimeTZ(d Datum) (*TimeTZ, error) {
	switch v := d.(type) {
	case *TimeTZ:
		return v, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to time with time zone", d)
	}
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

func AsTimestamp(d Datum) *Timestamp {
	v, err := TryAsTimestamp(d)
	if err != nil {
		panic(err)
	}
	return v
}

func TryAsTimestamp(d Datum) (*Timestamp, error) {
	switch v := d.(type) {
	case *Timestamp:
		return v, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to timestamp", d)
	}
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

func AsTimestampTZ(d Datum) *TimestampTZ {
	v, err := TryAsTimestampTZ(d)
	if err != nil {
		panic(err)
	}
	return v
}

func TryAsTimestampTZ(d Datum) (*TimestampTZ, error) {
	switch v := d.(type) {
	case *TimestampTZ:
		return v, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to timestamp with time zone", d)
	}
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

func AsInterval(d Datum) *Interval {
	v, err := TryAsInterval(d)
	if err != nil {
		panic(err)
	}
	return v
}

func TryAsInterval(d Datum) (*Interval, error) {
	switch v := d.(type) {
	case *Interval:
		return v, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to interval", d)
	}
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

func AsVertex(d Datum) *Vertex {
	v, err := TryAsVertex(d)
	if err != nil {
		panic(err)
	}
	return v
}

func TryAsVertex(d Datum) (*Vertex, error) {
	switch v := d.(type) {
	case *Vertex:
		return v, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to vertex", d)
	}
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

func AsEdge(d Datum) *Edge {
	v, err := TryAsEdge(d)
	if err != nil {
		panic(err)
	}
	return v
}

func TryAsEdge(d Datum) (*Edge, error) {
	switch v := d.(type) {
	case *Edge:
		return v, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to edge", d)
	}
}
