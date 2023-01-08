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

import "errors"

const VertexKeyLen = 1 /*prefix*/ + 8 /*graphID*/ + 8 /*vertexID*/

// VertexKey encodes the vertex key.
// The key format is: ${Prefix}${GraphID}${VertexID}.
func VertexKey(graphID, vertexID int64) []byte {
	result := make([]byte, 0, VertexKeyLen)
	result = append(result, prefix...)
	result = EncodeInt(result, graphID)
	result = EncodeInt(result, vertexID)
	return result
}

// ParseVertexKey parses the vertex key.
func ParseVertexKey(key []byte) (graphID, vertexID int64, err error) {
	if len(key) < VertexKeyLen {
		return 0, 0, errors.New("insufficient key length")
	}
	_, graphID, err = DecodeInt(key[len(prefix):])
	if err != nil {
		return
	}
	_, vertexID, err = DecodeInt(key[len(prefix)+8:])
	return
}
