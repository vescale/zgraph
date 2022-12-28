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

	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/types"
)

// PropertyDecoder is used to decode value bytes into datum
type PropertyDecoder struct {
	rowBytes

	labels     []*model.LabelInfo
	properties []*model.PropertyInfo
}

func NewPropertyDecoder(labels []*model.LabelInfo, properties []*model.PropertyInfo) *PropertyDecoder {
	return &PropertyDecoder{
		labels:     labels,
		properties: properties,
	}
}

func (d *PropertyDecoder) Decode(rowData []byte) (map[uint16]struct{}, map[uint16]types.Datum, error) {
	err := d.fromBytes(rowData)
	if err != nil {
		return nil, nil, err
	}

	labelIDs := make(map[uint16]struct{})
	for _, label := range d.labels {
		if d.hasLabel(uint16(label.ID)) {
			labelIDs[uint16(label.ID)] = struct{}{}
		}
	}

	row := make(map[uint16]types.Datum)
	for _, property := range d.properties {
		idx := d.findProperty(property.ID)
		if idx >= 0 {
			propData := d.getData(idx)
			d, err := d.decodeColDatum(propData)
			if err != nil {
				return nil, nil, err
			}
			row[property.ID] = d
		}
	}

	return labelIDs, row, nil
}

func (d *PropertyDecoder) decodeColDatum(propData []byte) (types.Datum, error) {
	var value types.Datum
	kind := types.Kind(propData[0])
	switch kind {
	case types.KindInt64:
		value.SetInt64(decodeInt(propData[1:]))
	case types.KindUint64:
		value.SetUint64(decodeUint(propData[1:]))
	case types.KindString:
		value.SetString(string(propData[1:]))
	case types.KindBytes:
		value.SetBytes(propData[1:])
	case types.KindFloat64:
		_, fVal, err := DecodeFloat(propData[1:])
		if err != nil {
			return value, err
		}
		value.SetFloat64(fVal)
	case types.KindDate:
		value.SetDate(decodeDate(propData[1:]))
	default:
		// TODO: support more types
		return value, errors.Errorf("unknown type %d", kind)
	}
	return value, nil
}

func decodeInt(val []byte) int64 {
	switch len(val) {
	case 1:
		return int64(int8(val[0]))
	case 2:
		return int64(int16(binary.LittleEndian.Uint16(val)))
	case 4:
		return int64(int32(binary.LittleEndian.Uint32(val)))
	default:
		return int64(binary.LittleEndian.Uint64(val))
	}
}

func decodeUint(val []byte) uint64 {
	switch len(val) {
	case 1:
		return uint64(val[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(val))
	case 4:
		return uint64(binary.LittleEndian.Uint32(val))
	default:
		return binary.LittleEndian.Uint64(val)
	}
}

func decodeDate(val []byte) types.Date {
	ct := types.CoreTime(decodeInt(val))
	return types.NewDate(ct)
}
