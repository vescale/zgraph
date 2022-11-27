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

package mvcc

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	t1 := Encode([]byte("test"), 1)
	t2 := Encode([]byte("test"), 2)
	r := bytes.Compare(t1, t2)
	assert.True(t, r > 0)
}

func TestDecode(t *testing.T) {
	r, v, err := Decode([]byte("test"))
	assert.ErrorContains(t, err, "insufficient bytes to decode value")
	assert.True(t, v == 0)
	assert.Nil(t, r)

	t1 := Encode([]byte("test"), 1)
	r, v, err = Decode(t1)
	assert.Nil(t, err)
	assert.True(t, v == 1)
	assert.True(t, bytes.Equal([]byte("test"), r))
}
