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
	"time"
)

const (
	yearBitFieldOffset, yearBitFieldWidth               uint64 = 50, 14
	monthBitFieldOffset, monthBitFieldWidth             uint64 = 46, 4
	dayBitFieldOffset, dayBitFieldWidth                 uint64 = 41, 5
	hourBitFieldOffset, hourBitFieldWidth               uint64 = 36, 5
	minuteBitFieldOffset, minuteBitFieldWidth           uint64 = 30, 6
	secondBitFieldOffset, secondBitFieldWidth           uint64 = 24, 6
	millisecondBitFieldOffset, millisecondBitFieldWidth uint64 = 14, 10
	tzOffsetBitFieldOffset, tzOffsetBitFieldWidth       uint64 = 3, 11

	tzValidFlag uint64 = 1 << 1 // 0: invalid, 1: valid
	tzSignFlag  uint64 = 1 << 2 // 0: positive, 1: negative

	yearBitFieldMask        uint64 = ((1 << yearBitFieldWidth) - 1) << yearBitFieldOffset
	monthBitFieldMask       uint64 = ((1 << monthBitFieldWidth) - 1) << monthBitFieldOffset
	dayBitFieldMask         uint64 = ((1 << dayBitFieldWidth) - 1) << dayBitFieldOffset
	hourBitFieldMask        uint64 = ((1 << hourBitFieldWidth) - 1) << hourBitFieldOffset
	minuteBitFieldMask      uint64 = ((1 << minuteBitFieldWidth) - 1) << minuteBitFieldOffset
	secondBitFieldMask      uint64 = ((1 << secondBitFieldWidth) - 1) << secondBitFieldOffset
	millisecondBitFieldMask uint64 = ((1 << millisecondBitFieldWidth) - 1) << millisecondBitFieldOffset
	tzOffsetBitFieldMask    uint64 = ((1 << tzOffsetBitFieldWidth) - 1) << tzOffsetBitFieldOffset
)

// CoreTime is the internal representation of time.
type CoreTime uint64

func (t CoreTime) Year() int {
	return int((uint64(t) & yearBitFieldMask) >> yearBitFieldOffset)
}

func (t CoreTime) Month() int {
	return int((uint64(t) & monthBitFieldMask) >> monthBitFieldOffset)
}

func (t CoreTime) Day() int {
	return int((uint64(t) & dayBitFieldMask) >> dayBitFieldOffset)
}

func (t CoreTime) Hour() int {
	return int((uint64(t) & hourBitFieldMask) >> hourBitFieldOffset)
}

func (t CoreTime) Minute() int {
	return int((uint64(t) & minuteBitFieldMask) >> minuteBitFieldOffset)
}

func (t CoreTime) Second() int {
	return int((uint64(t) & secondBitFieldMask) >> secondBitFieldOffset)
}

func (t CoreTime) Millisecond() int {
	return int((uint64(t) & millisecondBitFieldMask) >> millisecondBitFieldOffset)
}

func (t CoreTime) TZValid() bool {
	return uint64(t)&tzValidFlag != 0
}

func (t CoreTime) tzSign() int {
	if uint64(t)&tzSignFlag == 0 {
		return 1
	} else {
		return -1
	}
}

func (t CoreTime) TZHour() int {
	offset := int((uint64(t) & tzOffsetBitFieldMask) >> tzOffsetBitFieldOffset)
	return t.tzSign() * offset / 60
}

func (t CoreTime) TZMinute() int {
	offset := int((uint64(t) & tzOffsetBitFieldMask) >> tzOffsetBitFieldOffset)
	return t.tzSign() * (offset % 60)
}

func NewCoreTime(year, month, day, hour, minute, second, millisecond int, tzOffset *int) CoreTime {
	var t CoreTime
	t |= CoreTime(uint64(year) << yearBitFieldOffset & yearBitFieldMask)
	t |= CoreTime(uint64(month) << monthBitFieldOffset & monthBitFieldMask)
	t |= CoreTime(uint64(day) << dayBitFieldOffset & dayBitFieldMask)
	t |= CoreTime(uint64(hour) << hourBitFieldOffset & hourBitFieldMask)
	t |= CoreTime(uint64(minute) << minuteBitFieldOffset & minuteBitFieldMask)
	t |= CoreTime(uint64(second) << secondBitFieldOffset & secondBitFieldMask)
	t |= CoreTime(uint64(millisecond) << millisecondBitFieldOffset & millisecondBitFieldMask)
	if tzOffset != nil {
		t |= CoreTime(tzValidFlag)
		off := *tzOffset
		if off < 0 {
			off = -off
			t |= CoreTime(tzSignFlag)
		}
		t |= CoreTime(uint64(off) << tzOffsetBitFieldOffset & tzOffsetBitFieldMask)
	}
	return t
}

type Date struct {
	coreTime CoreTime
}

func NewDate(coreTime CoreTime) Date {
	return Date{coreTime: coreTime}
}

func (d Date) CoreTime() CoreTime {
	return d.coreTime
}

func (d Date) Year() int {
	return d.coreTime.Year()
}

func (d Date) Month() int {
	return d.coreTime.Month()
}

func (d Date) Day() int {
	return d.coreTime.Day()
}

func (d Date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year(), d.Month(), d.Day())
}

func ParseDate(s string) (Date, error) {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return Date{}, err
	}
	return NewDate(NewCoreTime(t.Year(), int(t.Month()), t.Day(), 0, 0, 0, 0, nil)), nil
}

type Time struct {
	coreTime CoreTime
}

func NewTime(coreTime CoreTime) Time {
	return Time{coreTime: coreTime}
}

func (t Time) CoreTime() CoreTime {
	return t.coreTime
}

func (t Time) Hour() int {
	return t.coreTime.Hour()
}

func (t Time) Minute() int {
	return t.coreTime.Minute()
}

func (t Time) Second() int {
	return t.coreTime.Second()
}

func (t Time) TZValid() bool {
	return t.coreTime.TZValid()
}

func (t Time) TZHour() int {
	return t.coreTime.TZHour()
}

func (t Time) TZMinute() int {
	return t.coreTime.TZMinute()
}

func (t Time) String() string {
	if t.coreTime.TZValid() {
		tzHour, tzMintute := t.TZHour(), t.TZMinute()
		if tzHour >= 0 && tzMintute >= 0 {
			return fmt.Sprintf("%02d:%02d:%02d+%02d:%02d", t.Hour(), t.Minute(), t.Second(), tzHour, tzMintute)
		} else {
			return fmt.Sprintf("%02d:%02d:%02d-%02d:%02d", t.Hour(), t.Minute(), t.Second(), -tzHour, -tzMintute)
		}
	} else {
		return fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
	}
}

const (
	timeFormat       = "15:04:05"
	timeWithTZFormat = "15:04:05-07:00"
)

func ParseTime(s string) (Time, error) {
	if len(s) == len(timeFormat) {
		t, err := time.Parse(timeFormat, s)
		if err != nil {
			return Time{}, err
		}
		return NewTime(NewCoreTime(0, 0, 0, t.Hour(), t.Minute(), t.Second(), 0, nil)), nil
	}
	if len(s) == len(timeWithTZFormat) {
		t, err := time.Parse(timeWithTZFormat, s)
		if err != nil {
			return Time{}, err
		}
		_, tzOffsetSecs := t.Zone()
		tzOffset := tzOffsetSecs / 60
		return NewTime(NewCoreTime(0, 0, 0, t.Hour(), t.Minute(), t.Second(), 0, &tzOffset)), nil
	}
	return Time{}, fmt.Errorf("invalid time format: %s", s)
}

type Timestamp struct {
	coreTime CoreTime
}

func NewTimestamp(coreTime CoreTime) Timestamp {
	return Timestamp{coreTime: coreTime}
}

func (t Timestamp) CoreTime() CoreTime {
	return t.coreTime
}

func (t Timestamp) Year() int {
	return t.coreTime.Year()
}

func (t Timestamp) Month() int {
	return t.coreTime.Month()
}

func (t Timestamp) Day() int {
	return t.coreTime.Day()
}

func (t Timestamp) Hour() int {
	return t.coreTime.Hour()
}

func (t Timestamp) Minute() int {
	return t.coreTime.Minute()
}

func (t Timestamp) Second() int {
	return t.coreTime.Second()
}

func (t Timestamp) TZValid() bool {
	return t.coreTime.TZValid()
}

func (t Timestamp) TZHour() int {
	return t.coreTime.TZHour()
}

func (t Timestamp) TZMinute() int {
	return t.coreTime.TZMinute()
}

const (
	timestampFormat       = "2006-01-02 15:04:05"
	timestampWithTZFormat = "2006-01-02 15:04:05-07:00"
)

func ParseTimestamp(s string) (Timestamp, error) {
	if len(s) == len(timestampFormat) {
		t, err := time.Parse(timestampFormat, s)
		if err != nil {
			return Timestamp{}, err
		}
		return NewTimestamp(NewCoreTime(t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, nil)), nil
	}
	if len(s) == len(timestampWithTZFormat) {
		t, err := time.Parse(timestampWithTZFormat, s)
		if err != nil {
			return Timestamp{}, err
		}
		_, tzOffsetSecs := t.Zone()
		tzOffset := tzOffsetSecs / 60
		return NewTimestamp(NewCoreTime(t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, &tzOffset)), nil
	}
	return Timestamp{}, fmt.Errorf("invalid timestamp format: %s", s)
}

func (t Timestamp) String() string {
	if t.coreTime.TZValid() {
		tzHour, tzMintute := t.TZHour(), t.TZMinute()
		if tzHour >= 0 && tzMintute >= 0 {
			return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d+%02d:%02d",
				t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), tzHour, tzMintute)
		} else {
			return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d-%02d:%02d",
				t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), -tzHour, -tzMintute)
		}
	} else {
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	}
}
