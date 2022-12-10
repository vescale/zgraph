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
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
)

// BinaryLiteral is the internal type for storing bit / hex literal type.
type BinaryLiteral []byte

// BitLiteral is the bit literal type.
type BitLiteral BinaryLiteral

// HexLiteral is the hex literal type.
type HexLiteral BinaryLiteral

// ZeroBinaryLiteral is a BinaryLiteral literal with zero value.
var ZeroBinaryLiteral = BinaryLiteral{}

// String implements fmt.Stringer interface.
func (b BinaryLiteral) String() string {
	if len(b) == 0 {
		return ""
	}
	return "0x" + hex.EncodeToString(b)
}

// ToString returns the string representation for the literal.
func (b BinaryLiteral) ToString() string {
	return string(b)
}

// ToBitLiteralString returns the bit literal representation for the literal.
func (b BinaryLiteral) ToBitLiteralString(trimLeadingZero bool) string {
	if len(b) == 0 {
		return "b''"
	}
	var buf bytes.Buffer
	for _, data := range b {
		fmt.Fprintf(&buf, "%08b", data)
	}
	ret := buf.Bytes()
	if trimLeadingZero {
		ret = bytes.TrimLeft(ret, "0")
		if len(ret) == 0 {
			ret = []byte{'0'}
		}
	}
	return fmt.Sprintf("b'%s'", string(ret))
}

// ParseBitStr parses bit string.
// The string format can be b'val', B'val' or 0bval, val must be 0 or 1.
// See https://dev.mysql.com/doc/refman/5.7/en/bit-value-literals.html
func ParseBitStr(s string) (BinaryLiteral, error) {
	if len(s) == 0 {
		return nil, errors.Errorf("invalid empty string for parsing bit type")
	}

	if s[0] == 'b' || s[0] == 'B' {
		// format is b'val' or B'val'
		s = strings.Trim(s[1:], "'")
	} else if strings.HasPrefix(s, "0b") {
		s = s[2:]
	} else {
		// here means format is not b'val', B'val' or 0bval.
		return nil, errors.Errorf("invalid bit type format %s", s)
	}

	if len(s) == 0 {
		return ZeroBinaryLiteral, nil
	}

	alignedLength := (len(s) + 7) &^ 7
	s = ("00000000" + s)[len(s)+8-alignedLength:] // Pad with zero (slice from `-alignedLength`)
	byteLength := len(s) >> 3
	buf := make([]byte, byteLength)

	for i := 0; i < byteLength; i++ {
		strPosition := i << 3
		val, err := strconv.ParseUint(s[strPosition:strPosition+8], 2, 8)
		if err != nil {
			return nil, errors.Trace(err)
		}
		buf[i] = byte(val)
	}

	return buf, nil
}

// NewBitLiteral parses bit string as BitLiteral type.
func NewBitLiteral(s string) (BitLiteral, error) {
	b, err := ParseBitStr(s)
	if err != nil {
		return BitLiteral{}, err
	}
	return BitLiteral(b), nil
}

// ToString implement ast.BinaryLiteral interface
func (b BitLiteral) ToString() string {
	return BinaryLiteral(b).ToString()
}

// ParseHexStr parses hexadecimal string literal.
// See https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
func ParseHexStr(s string) (BinaryLiteral, error) {
	if len(s) == 0 {
		return nil, errors.Errorf("invalid empty string for parsing hexadecimal literal")
	}

	if s[0] == 'x' || s[0] == 'X' {
		// format is x'val' or X'val'
		s = strings.Trim(s[1:], "'")
		if len(s)%2 != 0 {
			return nil, errors.Errorf("invalid hexadecimal format, must even numbers, but %d", len(s))
		}
	} else if strings.HasPrefix(s, "0x") {
		s = s[2:]
	} else {
		// here means format is not x'val', X'val' or 0xval.
		return nil, errors.Errorf("invalid hexadecimal format %s", s)
	}

	if len(s) == 0 {
		return ZeroBinaryLiteral, nil
	}

	if len(s)%2 != 0 {
		s = "0" + s
	}
	buf, err := hex.DecodeString(s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return buf, nil
}

// NewHexLiteral parses hexadecimal string as HexLiteral type.
func NewHexLiteral(s string) (HexLiteral, error) {
	h, err := ParseHexStr(s)
	if err != nil {
		return HexLiteral{}, err
	}
	return HexLiteral(h), nil
}

// ToString implement ast.BinaryLiteral interface
func (b HexLiteral) ToString() string {
	return BinaryLiteral(b).ToString()
}
