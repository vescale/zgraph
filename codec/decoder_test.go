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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/parser/types"
)

func TestPropertyDecoder_Decode(t *testing.T) {
	cases := []struct {
		propertyIDs []uint16
		values      []types.Datum
	}{
		{
			propertyIDs: []uint16{1, 2, 3},
			values: []types.Datum{
				types.NewStringDatum("hello"),
				types.NewDatum(1),
				types.NewDatum(1.1),
			},
		},
	}

	for _, c := range cases {
		encoder := &PropertyEncoder{}
		bytes, err := encoder.Encode(nil, c.propertyIDs, c.values)
		assert.Nil(t, err)

		var properties []*model.PropertyInfo
		for _, id := range c.propertyIDs {
			properties = append(properties, &model.PropertyInfo{
				ID:   id,
				Name: model.NewCIStr(fmt.Sprintf("property%d", id)),
			})
		}

		decoder := &PropertyDecoder{}
		row, err := decoder.Decode(properties, bytes)
		assert.Nil(t, err)
		assert.Equal(t, c.values, row)
	}
}
