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
	"sort"
	"unsafe"

	"github.com/pingcap/errors"
)

var errInvalidCodecVer = errors.New("invalid codec version")

// rowFlag represents the flag of first byte of value encoded bytes.
type rowFlag byte

// Flag:
// 0 0 0 0 0 0 0 0
// |         |   |
// |         +---+----- Encode Version (max to 8 versions)
// |+--------+--------- Reserved flag bits

const (
	version             = 0
	versionMask rowFlag = 0x07
)

func (f rowFlag) version() byte {
	return byte(f & versionMask)
}

// rowBytes is used to encode/decode the value bytes.
// Value Encode:
// Flag[1byte]+LabelCount(varint)+LabelIDs+PropertyCount(varint)+PropertyIDs+PropertyOffsets+Data
type rowBytes struct {
	labelIDs    []uint16
	propertyIDs []uint16
	offsets     []uint16
	data        []byte
}

// getData gets the row data at index `i`.
func (r *rowBytes) getData(i int) []byte {
	var start, end uint16
	if i > 0 {
		start = r.offsets[i-1]
	}
	end = r.offsets[i]
	return r.data[start:end]
}

func (r *rowBytes) hasLabel(labelID uint16) bool {
	i := sort.Search(len(r.labelIDs), func(i int) bool {
		return r.labelIDs[i] >= labelID
	})
	return i < len(r.labelIDs) && r.labelIDs[i] == labelID
}

// findProperty finds the property index of the row. -1 returned if not found.
func (r *rowBytes) findProperty(propertyID uint16) int {
	i := sort.Search(len(r.propertyIDs), func(i int) bool {
		return r.propertyIDs[i] >= propertyID
	})
	if i < len(r.propertyIDs) && r.propertyIDs[i] == propertyID {
		return i
	} else {
		return -1
	}
}

func (r *rowBytes) toBytes(buf []byte) []byte {
	buf = append(buf, version)
	buf = EncodeUvarint(buf, uint64(len(r.labelIDs)))
	buf = append(buf, u16SliceToBytes(r.labelIDs)...)
	buf = EncodeUvarint(buf, uint64(len(r.propertyIDs)))
	buf = append(buf, u16SliceToBytes(r.propertyIDs)...)
	buf = append(buf, u16SliceToBytes(r.offsets)...)
	buf = append(buf, r.data...)
	return buf
}

func (r *rowBytes) fromBytes(rowData []byte) error {
	if rowFlag(rowData[0]).version() != version {
		return errInvalidCodecVer
	}

	rowData, labelCount, err := DecodeUvarint(rowData[1:])
	if err != nil {
		return err
	}
	r.labelIDs = bytes2U16Slice(rowData[:labelCount*2])
	rowData = rowData[labelCount*2:]

	rowData, propertyCount, err := DecodeUvarint(rowData)
	if err != nil {
		return err
	}
	r.propertyIDs = bytes2U16Slice(rowData[:propertyCount*2])
	rowData = rowData[propertyCount*2:]

	r.offsets = bytes2U16Slice(rowData[:propertyCount*2])
	r.data = rowData[propertyCount*2:]
	return nil
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
