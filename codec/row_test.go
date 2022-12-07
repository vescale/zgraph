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
)

func TestRowBytes(t *testing.T) {
	rb := &rowBytes{
		propertyIDs: []uint16{1, 2, 3, 4},
		offsets:     []uint32{1, 2, 3, 4},
		data:        []byte("abcd"),
	}
	bytes := rb.toBytes(nil)
	rb2 := &rowBytes{}
	err := rb2.fromBytes(bytes)
	assert.Nil(t, err)
	assert.Equal(t, rb, rb2)

	a := rb2.getData(1)
	assert.Equal(t, "b", string(a))

	idx := rb2.findProperty(3)
	assert.Equal(t, 2, idx)
	idx = rb2.findProperty(5)
	assert.Equal(t, -1, idx)
}
