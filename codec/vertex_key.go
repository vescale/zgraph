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

// VERTEX CODEC DOCUMENTATIONS:
//
// - Key Format:
//   $Prefix_$GraphID_$SrcVertexID
// - Value Format:
//   [($PropertyID, $PropertyValue), ...]

// VertexKey encodes the vertex key described as above.
func VertexKey(graphID, srcVertexID int64) []byte {
	result := make([]byte, 0, len(prefix)+8 /*graphID*/ +8 /*srcVertexID*/ +len(vertexSep))
	result = append(result, prefix...)
	result = EncodeInt(result, graphID)
	result = EncodeInt(result, srcVertexID)
	result = EncodeBytes(result, vertexSep)
	return nil
}

// ParseVertexKey parses the vertex key.
func ParseVertexKey(key []byte) (graphID, srcVertexID int64, err error) {
	return
}
