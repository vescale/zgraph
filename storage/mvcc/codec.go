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
	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/codec"
	"github.com/vescale/zgraph/storage/kv"
)

var (
	// ErrInvalidEncodedKey describes parsing an invalid format of EncodedKey.
	ErrInvalidEncodedKey = errors.New("invalid encoded key")
)

// LockKey returns the encoded lock key of specified raw key.
func LockKey(key kv.Key) Key {
	return Encode(key, LockVer)
}

// Encode encodes a user defined key with timestamp.
func Encode(key kv.Key, ver kv.Version) Key {
	return codec.EncodeUintDesc(codec.EncodeBytes(nil, key), uint64(ver))
}

// Decode parses the origin key and version of an encoded key.
// Will return the original key if the encoded key is a meta key.
func Decode(encodedKey []byte) (kv.Key, kv.Version, error) {
	// Skip DataPrefix
	remainBytes, key, err := codec.DecodeBytes(encodedKey, nil)
	if err != nil {
		// should never happen
		return nil, 0, err
	}
	// if it's meta key
	if len(remainBytes) == 0 {
		return key, 0, nil
	}
	var ver uint64
	remainBytes, ver, err = codec.DecodeUintDesc(remainBytes)
	if err != nil {
		// should never happen
		return nil, 0, err
	}
	if len(remainBytes) != 0 {
		return nil, 0, errors.Trace(ErrInvalidEncodedKey)
	}
	return key, kv.Version(ver), nil
}
