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

func TestNewKey(t *testing.T) {
	x := []byte("test")
	ex := NewKey(x)
	k, v, err := Decode(ex)
	assert.Nil(t, err)
	assert.True(t, bytes.Equal(x, k))
	assert.Zero(t, v)
}
