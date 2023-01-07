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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEdgeKey(t *testing.T) {
	cases := []struct {
		graphID     int64
		srcVertexID int64
		dstVertexID int64
	}{
		{
			graphID:     100,
			srcVertexID: 200,
			dstVertexID: 300,
		},
		{
			graphID:     math.MaxInt64,
			srcVertexID: math.MaxInt64,
			dstVertexID: math.MaxInt64,
		},
	}
	for _, c := range cases {
		incomingEdgeKey := IncomingEdgeKey(c.graphID, c.srcVertexID, c.dstVertexID)
		graphID, srcVertexID, dstVertexID, err := ParseIncomingEdgeKey(incomingEdgeKey)
		require.NoError(t, err)
		require.Equal(t, c.graphID, graphID)
		require.Equal(t, c.srcVertexID, srcVertexID)
		require.Equal(t, c.dstVertexID, dstVertexID)

		outgoingEdgeKey := OutgoingEdgeKey(c.graphID, c.srcVertexID, c.dstVertexID)
		graphID, srcVertexID, dstVertexID, err = ParseOutgoingEdgeKey(outgoingEdgeKey)
		require.NoError(t, err)
		require.Equal(t, c.graphID, graphID)
		require.Equal(t, c.srcVertexID, srcVertexID)
		require.Equal(t, c.dstVertexID, dstVertexID)

		require.NotEqual(t, incomingEdgeKey, outgoingEdgeKey)
	}
}
