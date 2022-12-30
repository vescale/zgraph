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

func TestVertexKey(t *testing.T) {
	cases := []struct {
		graphID  int64
		vertexID int64
	}{
		{
			graphID:  100,
			vertexID: 200,
		}, {
			graphID:  math.MaxInt64,
			vertexID: math.MaxInt64,
		},
	}
	for _, c := range cases {
		key := VertexKey(c.graphID, c.vertexID)
		graphID, vertexID, err := ParseVertexKey(key)
		require.NoError(t, err)
		require.Equal(t, c.graphID, graphID)
		require.Equal(t, c.vertexID, vertexID)
	}
}
