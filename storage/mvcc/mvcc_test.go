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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLock_MarshalBinary(t *testing.T) {
	lock := &Lock{
		StartVer: 100,
		Primary:  []byte("primary"),
		Value:    []byte("value"),
		Op:       Op_Lock,
		TTL:      1000,
	}
	bytes, err := lock.MarshalBinary()
	assert.Nil(t, err)

	lock2 := &Lock{}
	err = lock2.UnmarshalBinary(bytes)
	assert.Nil(t, err)

	assert.True(t, reflect.DeepEqual(lock, lock2))
}

func TestValue_MarshalBinary(t *testing.T) {
	value := &Value{
		Type:      ValueTypeLock,
		StartVer:  100,
		CommitVer: 1001,
		Value:     []byte("value"),
	}
	bytes, err := value.MarshalBinary()
	assert.Nil(t, err)

	value2 := &Value{}
	err = value2.UnmarshalBinary(bytes)
	assert.Nil(t, err)

	assert.True(t, reflect.DeepEqual(value, value2))
}
