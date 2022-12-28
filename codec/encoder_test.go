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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph/types"
)

func TestPropertyEncoder_Encode(t *testing.T) {
	cases := []struct {
		labelIDs    []uint16
		propertyIDs []uint16
		values      []types.Datum
	}{
		{
			labelIDs:    []uint16{1, 2, 3},
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
		// FIXME: validate the values.
		_, err := encoder.Encode(nil, c.labelIDs, c.propertyIDs, c.values)
		assert.NoError(t, err)
	}
}
