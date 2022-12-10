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
	"sort"

	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/types"
)

// PropertyEncoder is used to encode datums into value bytes.
type PropertyEncoder struct {
	rowBytes

	values []*types.Datum
}

// Encode encodes properties into a value bytes.
func (e *PropertyEncoder) Encode(buf []byte, propertyIDs []uint16, values []types.Datum) ([]byte, error) {
	e.reform(propertyIDs, values)
	for i, value := range e.values {
		err := e.encodeDatum(value)
		if err != nil {
			return nil, err
		}
		e.offsets[i] = uint32(len(e.data))
	}
	return e.toBytes(buf[:0]), nil
}

func (e *PropertyEncoder) encodeDatum(value *types.Datum) error {
	// Put the kind information first.
	e.data = append(e.data, byte(value.Kind()))
	switch value.Kind() {
	case types.KindInt64:
		e.data = encodeInt(e.data, value.GetInt64())
	case types.KindUint64:
		e.data = encodeUint(e.data, value.GetUint64())
	case types.KindString, types.KindBytes:
		e.data = append(e.data, value.GetBytes()...)
	case types.KindFloat64:
		e.data = EncodeFloat(e.data, value.GetFloat64())
	default:
		// TODO: support more types.
		return errors.Errorf("unsupported encode type %d", value.Kind())
	}
	return nil
}

func encodeInt(buf []byte, iVal int64) []byte {
	var tmp [8]byte
	if int64(int8(iVal)) == iVal {
		buf = append(buf, byte(iVal))
	} else if int64(int16(iVal)) == iVal {
		binary.LittleEndian.PutUint16(tmp[:], uint16(iVal))
		buf = append(buf, tmp[:2]...)
	} else if int64(int32(iVal)) == iVal {
		binary.LittleEndian.PutUint32(tmp[:], uint32(iVal))
		buf = append(buf, tmp[:4]...)
	} else {
		binary.LittleEndian.PutUint64(tmp[:], uint64(iVal))
		buf = append(buf, tmp[:8]...)
	}
	return buf
}

func encodeUint(buf []byte, uVal uint64) []byte {
	var tmp [8]byte
	if uint64(uint8(uVal)) == uVal {
		buf = append(buf, byte(uVal))
	} else if uint64(uint16(uVal)) == uVal {
		binary.LittleEndian.PutUint16(tmp[:], uint16(uVal))
		buf = append(buf, tmp[:2]...)
	} else if uint64(uint32(uVal)) == uVal {
		binary.LittleEndian.PutUint32(tmp[:], uint32(uVal))
		buf = append(buf, tmp[:4]...)
	} else {
		binary.LittleEndian.PutUint64(tmp[:], uVal)
		buf = append(buf, tmp[:8]...)
	}
	return buf
}
func (e *PropertyEncoder) reform(propertyIDs []uint16, values []types.Datum) {
	// reset
	e.propertyIDs = e.propertyIDs[:0]
	e.offsets = e.offsets[:0]
	e.data = e.data[:0]
	e.values = e.values[:0]

	for i, propertyID := range propertyIDs {
		e.propertyIDs = append(e.propertyIDs, propertyID)
		e.values = append(e.values, &values[i])
	}
	e.offsets = make([]uint32, len(e.propertyIDs))

	// Sort the property ID.
	sort.Sort(e)
}

// Less implements the Sorter interface.
func (e *PropertyEncoder) Less(i, j int) bool {
	return e.propertyIDs[i] < e.propertyIDs[j]
}

// Len implements the Sorter interface.
func (e *PropertyEncoder) Len() int {
	return len(e.propertyIDs)
}

// Swap implements the Sorter interface.
func (e *PropertyEncoder) Swap(i, j int) {
	e.propertyIDs[i], e.propertyIDs[j] = e.propertyIDs[j], e.propertyIDs[i]
	e.values[i], e.values[j] = e.values[j], e.values[i]
}
