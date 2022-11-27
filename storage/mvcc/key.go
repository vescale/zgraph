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

import "github.com/vescale/zgraph/codec"

// Key is the encoded key type with timestamp.
type Key []byte

// NewKey encodes a key into MvccKey.
func NewKey(key []byte) Key {
	if len(key) == 0 {
		return nil
	}
	return codec.EncodeBytes(nil, key)
}

// Raw decodes a MvccKey to original key.
func (key Key) Raw() []byte {
	if len(key) == 0 {
		return nil
	}
	_, k, err := codec.DecodeBytes(key, nil)
	if err != nil {
		panic(err)
	}
	return k
}
