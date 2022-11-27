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

	"github.com/cockroachdb/pebble"
)

// LockDecoder is used to decode Lock entry of low-level key-value store.
type LockDecoder struct {
	Lock      Lock
	ExpectKey []byte
}

// Decode decodes the Lock value if current iterator is at expectKey::Lock.
func (dec *LockDecoder) Decode(iter *pebble.Iterator) (bool, error) {
	if iter.Error() != nil || !iter.Valid() {
		return false, iter.Error()
	}

	iterKey := iter.Key()
	key, ver, err := Decode(iterKey)
	if err != nil {
		return false, err
	}
	if !bytes.Equal(key, dec.ExpectKey) {
		return false, nil
	}
	if ver != LockVer {
		return false, nil
	}

	var lock Lock
	val, err := iter.ValueAndErr()
	if err != nil {
		return false, err
	}
	err = lock.UnmarshalBinary(val)
	if err != nil {
		return false, err
	}
	dec.Lock = lock
	iter.Next()
	return true, nil
}

// ValueDecoder is used to decode the value entries. There will be multiple
// versions of specified key.
type ValueDecoder struct {
	Value     Value
	ExpectKey []byte
}

// Decode decodes a mvcc value if iter key is expectKey.
func (dec *ValueDecoder) Decode(iter *pebble.Iterator) (bool, error) {
	if iter.Error() != nil || !iter.Valid() {
		return false, iter.Error()
	}

	key, ver, err := Decode(iter.Key())
	if err != nil {
		return false, err
	}
	if !bytes.Equal(key, dec.ExpectKey) {
		return false, nil
	}
	if ver == LockVer {
		return false, nil
	}

	var value Value
	val, err := iter.ValueAndErr()
	if err != nil {
		return false, err
	}
	err = value.UnmarshalBinary(val)
	if err != nil {
		return false, err
	}
	dec.Value = value
	iter.Next()
	return true, nil
}

// SkipDecoder is used to decode useless versions of value entry.
type SkipDecoder struct {
	CurrKey []byte
}

// Decode skips the iterator as long as its key is currKey, the new key would be stored.
func (dec *SkipDecoder) Decode(iter *pebble.Iterator) (bool, error) {
	if iter.Error() != nil {
		return false, iter.Error()
	}
	for iter.Valid() {
		key, _, err := Decode(iter.Key())
		if err != nil {
			return false, err
		}
		if !bytes.Equal(key, dec.CurrKey) {
			dec.CurrKey = key
			return true, nil
		}
		iter.Next()
	}
	return false, nil
}
