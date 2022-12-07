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
	"reflect"
	"unsafe"

	"github.com/pingcap/errors"
)

var errInvalidCodecVer = errors.New("invalid codec version")

// RowFlag represents the flag of first byte of value encoded bytes.
type RowFlag byte

// Flag:
// 0 0 0 0 0 0 0 0
// |         |   |
// |         +---+----- Encode Version (max to 8 versions)
// |+--------+--------- Reserved flag bits

// rowBytes is used to encode/decode the value bytes.
// Value Encode:
// Flag[1byte]+PropertyCount(varint)+PropertyIDs+PropertyOffsets+Data
type rowBytes struct {
	propertyIDs []uint16
	offsets     []uint32
	data        []byte
}

// getData gets the row data at index `i`.
func (r *rowBytes) getData(i int) []byte {
	var start, end uint32
	if i > 0 {
		start = r.offsets[i-1]
	}
	end = r.offsets[i]
	return r.data[start:end]
}

// findProperty finds the property index of the row. -1 returned if not found.
func (r *rowBytes) findProperty(propertyID uint16) int {
	i, j := 0, len(r.propertyIDs)
	for i < j {
		// avoid overflow when computing h
		h := int(uint(i+j) >> 1)
		// i ≤ h < j
		v := r.propertyIDs[h]
		if v < propertyID {
			i = h + 1
		} else if v == propertyID {
			return h
		} else {
			j = h
		}
	}
	return -1
}

func (r *rowBytes) toBytes(buf []byte) []byte {
	// Currently, our version number is zero.
	buf = append(buf, 0)
	buf = EncodeVarint(buf, int64(len(r.propertyIDs)))
	buf = append(buf, u16SliceToBytes(r.propertyIDs)...)
	buf = append(buf, u32SliceToBytes(r.offsets)...)
	buf = append(buf, r.data...)
	return buf
}

func (r *rowBytes) fromBytes(rowData []byte) error {
	if rowData[0] != 0 {
		return errInvalidCodecVer
	}
	// Decode property cound.
	rowData, n, err := DecodeVarint(rowData[1:])
	if err != nil {
		return err
	}

	propertyLength := n * 2
	offsetLength := n * 4

	r.propertyIDs = bytes2U16Slice(rowData[:propertyLength])
	r.offsets = bytesToU32Slice(rowData[propertyLength : propertyLength+offsetLength])
	r.data = rowData[propertyLength+offsetLength:]

	return nil
}

func bytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}

func bytes2U16Slice(b []byte) []uint16 {
	if len(b) == 0 {
		return nil
	}
	var u16s []uint16
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u16s))
	hdr.Len = len(b) / 2
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u16s
}

func u16SliceToBytes(u16s []uint16) []byte {
	if len(u16s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u16s) * 2
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u16s[0]))
	return b
}

func u32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}
