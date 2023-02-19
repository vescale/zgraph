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
	"fmt"
	"sort"

	"github.com/vescale/zgraph/datum"
)

// PropertyEncoder is used to encode datums into value bytes.
type PropertyEncoder struct {
	rowBytes

	values []datum.Datum
}

// Encode encodes properties into a value bytes.
func (e *PropertyEncoder) Encode(buf []byte, labelIDs, propertyIDs []uint16, values []datum.Datum) ([]byte, error) {
	e.reform(labelIDs, propertyIDs, values)
	for i, value := range e.values {
		err := e.encodeDatum(value)
		if err != nil {
			return nil, err
		}
		e.offsets[i] = uint16(len(e.data))
	}
	return e.toBytes(buf[:0]), nil
}

func (e *PropertyEncoder) encodeDatum(value datum.Datum) error {
	// Put the type information first.
	e.data = append(e.data, byte(value.Type()))
	switch v := value.(type) {
	case *datum.Int:
		e.data = encodeInt(e.data, int64(*v))
	case *datum.Float:
		e.data = EncodeFloat(e.data, float64(*v))
	case *datum.String:
		e.data = append(e.data, []byte((*v))...)
	case *datum.Date:
		e.data = encodeDate(e.data, *v)
	default:
		return fmt.Errorf("unsupported encode type %T", value)
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

func encodeDate(buf []byte, date datum.Date) []byte {
	return encodeInt(buf, int64(date.UnixEpochDays()))
}

func (e *PropertyEncoder) reform(labelIDs, propertyIDs []uint16, values []datum.Datum) {
	e.labelIDs = append(e.labelIDs[:0], labelIDs...)
	e.propertyIDs = append(e.propertyIDs[:0], propertyIDs...)
	e.offsets = make([]uint16, len(e.propertyIDs))
	e.data = e.data[:0]
	e.values = e.values[:0]
	for i := range values {
		e.values = append(e.values, values[i])
	}

	sort.Slice(e.labelIDs, func(i, j int) bool {
		return e.labelIDs[i] < e.labelIDs[j]
	})
	sort.Sort(&propertySorter{
		propertyIDs: e.propertyIDs,
		values:      e.values,
	})
}

type propertySorter struct {
	propertyIDs []uint16
	values      []datum.Datum
}

// Less implements the Sorter interface.
func (ps *propertySorter) Less(i, j int) bool {
	return ps.propertyIDs[i] < ps.propertyIDs[j]
}

// Len implements the Sorter interface.
func (ps *propertySorter) Len() int {
	return len(ps.propertyIDs)
}

// Swap implements the Sorter interface.
func (ps *propertySorter) Swap(i, j int) {
	ps.propertyIDs[i], ps.propertyIDs[j] = ps.propertyIDs[j], ps.propertyIDs[i]
	ps.values[i], ps.values[j] = ps.values[j], ps.values[i]
}
