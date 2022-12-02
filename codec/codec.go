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

package codec

import (
	"encoding/binary"
	"math"
	"runtime"
	"unsafe"

	"github.com/pingcap/errors"
)

const (
	encGroupSize        = 8
	encMarker           = byte(0xFF)
	encPad              = byte(0x0)
	signMask     uint64 = 0x8000000000000000
)

var (
	pads = make([]byte, encGroupSize)
)

// reallocBytes is like realloc.
func reallocBytes(b []byte, n int) []byte {
	newSize := len(b) + n
	if cap(b) < newSize {
		bs := make([]byte, len(b), newSize)
		copy(bs, b)
		return bs
	}

	// slice b has capability to store n bytes
	return b
}

// EncodeBytes guarantees the encoded value is in ascending order for comparison,
// encoding with the following rule:
//
//	[group1][marker1]...[groupN][markerN]
//	group is 8 bytes slice which is padding with 0.
//	marker is `0xFF - padding 0 count`
//
// For example:
//
//	[] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
//	[1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
//	[1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
//	[1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
//
// Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
func EncodeBytes(b []byte, data []byte) []byte {
	// Allocate more space to avoid unnecessary slice growing.
	// Assume that the byte slice size is about `(len(data) / encGroupSize + 1) * (encGroupSize + 1)` bytes,
	// that is `(len(data) / 8 + 1) * 9` in our implement.
	dLen := len(data)
	reallocSize := (dLen/encGroupSize + 1) * (encGroupSize + 1)
	result := reallocBytes(b, reallocSize)
	for idx := 0; idx <= dLen; idx += encGroupSize {
		remain := dLen - idx
		padCount := 0
		if remain >= encGroupSize {
			result = append(result, data[idx:idx+encGroupSize]...)
		} else {
			padCount = encGroupSize - remain
			result = append(result, data[idx:]...)
			result = append(result, pads[:padCount]...)
		}

		marker := encMarker - byte(padCount)
		result = append(result, marker)
	}

	return result
}

// EncodeUintDesc appends the encoded value to slice b and returns the appended slice.
// EncodeUintDesc guarantees that the encoded value is in descending order for comparison.
func EncodeUintDesc(b []byte, v uint64) []byte {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], ^v)
	return append(b, data[:]...)
}

func decodeBytes(b []byte, buf []byte, reverse bool) ([]byte, []byte, error) {
	if buf == nil {
		buf = make([]byte, 0, len(b))
	}
	buf = buf[:0]
	for {
		if len(b) < encGroupSize+1 {
			return nil, nil, errors.New("insufficient bytes to decode value")
		}

		groupBytes := b[:encGroupSize+1]

		group := groupBytes[:encGroupSize]
		marker := groupBytes[encGroupSize]

		var padCount byte
		if reverse {
			padCount = marker
		} else {
			padCount = encMarker - marker
		}
		if padCount > encGroupSize {
			return nil, nil, errors.Errorf("invalid marker byte, group bytes %q", groupBytes)
		}

		realGroupSize := encGroupSize - padCount
		buf = append(buf, group[:realGroupSize]...)
		b = b[encGroupSize+1:]

		if padCount != 0 {
			var padByte = encPad
			if reverse {
				padByte = encMarker
			}
			// Check validity of padding bytes.
			for _, v := range group[realGroupSize:] {
				if v != padByte {
					return nil, nil, errors.Errorf("invalid padding byte, group bytes %q", groupBytes)
				}
			}
			break
		}
	}
	if reverse {
		reverseBytes(buf)
	}
	return b, buf, nil
}

// DecodeBytes decodes bytes which is encoded by EncodeBytes before,
// returns the leftover bytes and decoded value if no error.
// `buf` is used to buffer data to avoid the cost of makeslice in decodeBytes when DecodeBytes is called by Decoder.DecodeOne.
func DecodeBytes(b []byte, buf []byte) ([]byte, []byte, error) {
	return decodeBytes(b, buf, false)
}

// See https://golang.org/src/crypto/cipher/xor.go
const wordSize = int(unsafe.Sizeof(uintptr(0)))
const supportsUnaligned = runtime.GOARCH == "386" || runtime.GOARCH == "amd64"

func fastReverseBytes(b []byte) {
	n := len(b)
	w := n / wordSize
	if w > 0 {
		bw := *(*[]uintptr)(unsafe.Pointer(&b))
		for i := 0; i < w; i++ {
			bw[i] = ^bw[i]
		}
	}

	for i := w * wordSize; i < n; i++ {
		b[i] = ^b[i]
	}
}

func safeReverseBytes(b []byte) {
	for i := range b {
		b[i] = ^b[i]
	}
}

func reverseBytes(b []byte) {
	if supportsUnaligned {
		fastReverseBytes(b)
		return
	}

	safeReverseBytes(b)
}

// EncodeIntToCmpUint make int v to comparable uint type
func EncodeIntToCmpUint(v int64) uint64 {
	return uint64(v) ^ signMask
}

// DecodeCmpUintToInt decodes the u that encoded by EncodeIntToCmpUint
func DecodeCmpUintToInt(u uint64) int64 {
	return int64(u ^ signMask)
}

// EncodeInt appends the encoded value to slice b and returns the appended slice.
// EncodeInt guarantees that the encoded value is in ascending order for comparison.
func EncodeInt(b []byte, v int64) []byte {
	var data [8]byte
	u := EncodeIntToCmpUint(v)
	binary.BigEndian.PutUint64(data[:], u)
	return append(b, data[:]...)
}

// EncodeIntDesc appends the encoded value to slice b and returns the appended slice.
// EncodeIntDesc guarantees that the encoded value is in descending order for comparison.
func EncodeIntDesc(b []byte, v int64) []byte {
	var data [8]byte
	u := EncodeIntToCmpUint(v)
	binary.BigEndian.PutUint64(data[:], ^u)
	return append(b, data[:]...)
}

// DecodeInt decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeInt(b []byte) ([]byte, int64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	u := binary.BigEndian.Uint64(b[:8])
	v := DecodeCmpUintToInt(u)
	b = b[8:]
	return b, v, nil
}

// DecodeIntDesc decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeIntDesc(b []byte) ([]byte, int64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	u := binary.BigEndian.Uint64(b[:8])
	v := DecodeCmpUintToInt(^u)
	b = b[8:]
	return b, v, nil
}

// EncodeUint appends the encoded value to slice b and returns the appended slice.
// EncodeUint guarantees that the encoded value is in ascending order for comparison.
func EncodeUint(b []byte, v uint64) []byte {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], v)
	return append(b, data[:]...)
}

// DecodeUint decodes value encoded by EncodeUint before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeUint(b []byte) ([]byte, uint64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	v := binary.BigEndian.Uint64(b[:8])
	b = b[8:]
	return b, v, nil
}

// DecodeUintDesc decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeUintDesc(b []byte) ([]byte, uint64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	data := b[:8]
	v := binary.BigEndian.Uint64(data)
	b = b[8:]
	return b, ^v, nil
}

// EncodeVarint appends the encoded value to slice b and returns the appended slice.
// Note that the encoded result is not memcomparable.
func EncodeVarint(b []byte, v int64) []byte {
	var data [binary.MaxVarintLen64]byte
	n := binary.PutVarint(data[:], v)
	return append(b, data[:n]...)
}

// DecodeVarint decodes value encoded by EncodeVarint before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeVarint(b []byte) ([]byte, int64, error) {
	v, n := binary.Varint(b)
	if n > 0 {
		return b[n:], v, nil
	}
	if n < 0 {
		return nil, 0, errors.New("value larger than 64 bits")
	}
	return nil, 0, errors.New("insufficient bytes to decode value")
}

// EncodeUvarint appends the encoded value to slice b and returns the appended slice.
// Note that the encoded result is not memcomparable.
func EncodeUvarint(b []byte, v uint64) []byte {
	var data [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(data[:], v)
	return append(b, data[:n]...)
}

// DecodeUvarint decodes value encoded by EncodeUvarint before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeUvarint(b []byte) ([]byte, uint64, error) {
	v, n := binary.Uvarint(b)
	if n > 0 {
		return b[n:], v, nil
	}
	if n < 0 {
		return nil, 0, errors.New("value larger than 64 bits")
	}
	return nil, 0, errors.New("insufficient bytes to decode value")
}

const (
	negativeTagEnd   = 8        // negative tag is (negativeTagEnd - length).
	positiveTagStart = 0xff - 8 // Positive tag is (positiveTagStart + length).
)

// EncodeComparableVarint encodes an int64 to a mem-comparable bytes.
func EncodeComparableVarint(b []byte, v int64) []byte {
	if v < 0 {
		// All negative value has a tag byte prefix (negativeTagEnd - length).
		// Smaller negative value encodes to more bytes, has smaller tag.
		if v >= -0xff {
			return append(b, negativeTagEnd-1, byte(v))
		} else if v >= -0xffff {
			return append(b, negativeTagEnd-2, byte(v>>8), byte(v))
		} else if v >= -0xffffff {
			return append(b, negativeTagEnd-3, byte(v>>16), byte(v>>8), byte(v))
		} else if v >= -0xffffffff {
			return append(b, negativeTagEnd-4, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		} else if v >= -0xffffffffff {
			return append(b, negativeTagEnd-5, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		} else if v >= -0xffffffffffff {
			return append(b, negativeTagEnd-6, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
				byte(v))
		} else if v >= -0xffffffffffffff {
			return append(b, negativeTagEnd-7, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
				byte(v>>8), byte(v))
		}
		return append(b, negativeTagEnd-8, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
			byte(v>>16), byte(v>>8), byte(v))
	}
	return EncodeComparableUvarint(b, uint64(v))
}

// EncodeComparableUvarint encodes uint64 into mem-comparable bytes.
func EncodeComparableUvarint(b []byte, v uint64) []byte {
	// The first byte has 256 values, [0, 7] is reserved for negative tags,
	// [248, 255] is reserved for larger positive tags,
	// So we can store value [0, 239] in a single byte.
	// Values cannot be stored in single byte has a tag byte prefix (positiveTagStart+length).
	// Larger value encodes to more bytes, has larger tag.
	if v <= positiveTagStart-negativeTagEnd {
		return append(b, byte(v)+negativeTagEnd)
	} else if v <= 0xff {
		return append(b, positiveTagStart+1, byte(v))
	} else if v <= 0xffff {
		return append(b, positiveTagStart+2, byte(v>>8), byte(v))
	} else if v <= 0xffffff {
		return append(b, positiveTagStart+3, byte(v>>16), byte(v>>8), byte(v))
	} else if v <= 0xffffffff {
		return append(b, positiveTagStart+4, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	} else if v <= 0xffffffffff {
		return append(b, positiveTagStart+5, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	} else if v <= 0xffffffffffff {
		return append(b, positiveTagStart+6, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
			byte(v))
	} else if v <= 0xffffffffffffff {
		return append(b, positiveTagStart+7, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
			byte(v>>8), byte(v))
	}
	return append(b, positiveTagStart+8, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
		byte(v>>16), byte(v>>8), byte(v))
}

var (
	errDecodeInsufficient = errors.New("insufficient bytes to decode value")
	errDecodeInvalid      = errors.New("invalid bytes to decode value")
)

// DecodeComparableUvarint decodes mem-comparable uvarint.
func DecodeComparableUvarint(b []byte) ([]byte, uint64, error) {
	if len(b) == 0 {
		return nil, 0, errDecodeInsufficient
	}
	first := b[0]
	b = b[1:]
	if first < negativeTagEnd {
		return nil, 0, errors.WithStack(errDecodeInvalid)
	}
	if first <= positiveTagStart {
		return b, uint64(first) - negativeTagEnd, nil
	}
	length := int(first) - positiveTagStart
	if len(b) < length {
		return nil, 0, errors.WithStack(errDecodeInsufficient)
	}
	var v uint64
	for _, c := range b[:length] {
		v = (v << 8) | uint64(c)
	}
	return b[length:], v, nil
}

// DecodeComparableVarint decodes mem-comparable varint.
func DecodeComparableVarint(b []byte) ([]byte, int64, error) {
	if len(b) == 0 {
		return nil, 0, errors.WithStack(errDecodeInsufficient)
	}
	first := b[0]
	if first >= negativeTagEnd && first <= positiveTagStart {
		return b, int64(first) - negativeTagEnd, nil
	}
	b = b[1:]
	var length int
	var v uint64
	if first < negativeTagEnd {
		length = negativeTagEnd - int(first)
		v = math.MaxUint64 // negative value has all bits on by default.
	} else {
		length = int(first) - positiveTagStart
	}
	if len(b) < length {
		return nil, 0, errors.WithStack(errDecodeInsufficient)
	}
	for _, c := range b[:length] {
		v = (v << 8) | uint64(c)
	}
	if first > positiveTagStart && v > math.MaxInt64 {
		return nil, 0, errors.WithStack(errDecodeInvalid)
	} else if first < negativeTagEnd && v <= math.MaxInt64 {
		return nil, 0, errors.WithStack(errDecodeInvalid)
	}
	return b[length:], int64(v), nil
}

// EncodedBytesLength returns the length of data after encoded
func EncodedBytesLength(dataLen int) int {
	mod := dataLen % encGroupSize
	padCount := encGroupSize - mod
	return dataLen + padCount + 1 + dataLen/encGroupSize
}

// EncodeBytesDesc first encodes bytes using EncodeBytes, then bitwise reverses
// encoded value to guarantee the encoded value is in descending order for comparison.
func EncodeBytesDesc(b []byte, data []byte) []byte {
	n := len(b)
	b = EncodeBytes(b, data)
	reverseBytes(b[n:])
	return b
}

// DecodeBytesDesc decodes bytes which is encoded by EncodeBytesDesc before,
// returns the leftover bytes and decoded value if no error.
func DecodeBytesDesc(b []byte, buf []byte) ([]byte, []byte, error) {
	return decodeBytes(b, buf, true)
}

// EncodeBytesExt is an extension of `EncodeBytes`, which will not encode for `isRawKv = true` but just append `data` to `b`.
func EncodeBytesExt(b []byte, data []byte, isRawKv bool) []byte {
	if isRawKv {
		return append(b, data...)
	}
	return EncodeBytes(b, data)
}
