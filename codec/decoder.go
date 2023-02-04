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

	"github.com/vescale/zgraph/datum"
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

func (d *PropertyDecoder) Decode(rowData []byte) (map[uint16]struct{}, map[uint16]datum.Datum, error) {
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

	row := make(map[uint16]datum.Datum)
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

func (d *PropertyDecoder) decodeColDatum(propData []byte) (datum.Datum, error) {
	var value datum.Datum
	typ := types.T(propData[0])
	switch typ {
	case types.Int:
		value = datum.NewInt(decodeInt(propData[1:]))
	case types.Float:
		_, v, err := DecodeFloat(propData[1:])
		if err != nil {
			return nil, err
		}
		value = datum.NewFloat(v)
	case types.String:
		value = datum.NewString(string(propData[1:]))
	case types.Date:
		value = decodeDate(propData[1:])
	default:
		// TODO: support more types
		return value, fmt.Errorf("unknown type %s", typ)
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

func decodeDate(val []byte) *datum.Date {
	return datum.NewDateFromUnixEpochDays(int32(decodeInt(val)))
}
