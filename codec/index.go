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

// INDEX CODEC DOCUMENTATIONS:

// NOTE: Label is a special kind of index ($LabelID equals $IndexID).
// The following SQL specifies an edge label `known` and the LabelID will be
// the identifier of edge `known`.
//      INSERT EDGE e BETWEEN x AND y LABELS ( knows )
//      FROM MATCH (x:Person)
//         , MATCH (y:Person)
//      WHERE id(x) = 1 AND id(y) = 2

//
// - Key Format:
//   Unique Key:     $Prefix_$GraphID_$LabelID_$Type
//   Non-Unique Key: $Prefix_$GraphID_$LabelID_$Type_$Unique
//   Unique Key:     $Prefix_$GraphID_$IndexID_$Type
//   Non-Unique Key: $Prefix_$GraphID_$IndexID_$Type_$Unique
// - Value Format:
//   [($PropertyID, $PropertyValue), ...]
//
// $Type Explanation: We need to distinguish the index key type because a label
// can be both attach to vertex and edge. There will be two types index key.
// 1. Edge
// 2. Vertex
//
// $Unique Explanation:
// 1. For vertex label index: it will be the vertex identifier.
// 2. For edge label index: it will be $SrcVertexID_$DstVertexID

// UniqueIndexKey encodes the unique index key described as above.
func UniqueIndexKey(graphID, indexID int64, typ byte) []byte {
	return nil
}

// VertexNonUniqueIndexKey encodes the non-unique index key described as above.
func VertexNonUniqueIndexKey(graphID, indexID, vertexID int64) []byte {
	return nil
}

// EdgeNonUniqueIndexKey encodes the non-unique index key described as above.
func EdgeNonUniqueIndexKey(graphID, indexID, srcVertexID, dstVertexID int64) []byte {
	return nil
}

// ParseUniqueIndexKey parse the unique key.
func ParseUniqueIndexKey(key []byte) (graphID, indexID int64, typ byte, err error) {
	return
}

// ParseVertexNonUniqueIndexKey parses the vertex non-unique key.
func ParseVertexNonUniqueIndexKey(key []byte) (graphID, indexID, vertexID int64, err error) {
	return
}

// ParseEdgeNonUniqueIndexKey parses the edge non-unique key.
func ParseEdgeNonUniqueIndexKey(key []byte) (graphID, indexID, srcVertexID, dstVertexID int64, err error) {
	return
}
