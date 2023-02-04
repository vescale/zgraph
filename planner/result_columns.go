// Copyright 2023 zGraph Authors. All rights reserved.
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

package planner

import (
	"github.com/vescale/zgraph/parser/model"
	"github.com/vescale/zgraph/types"
)

type ResultColumn struct {
	Name   model.CIStr
	Type   types.T
	Hidden bool
}

type ResultColumns []ResultColumn

func (r ResultColumns) FindColumnIndex(name model.CIStr) int {
	for i, col := range r {
		if col.Name.L == name.L {
			return i
		}
	}
	return -1
}

func ResultColumnsFromSubgraph(sg *Subgraph) ResultColumns {
	var cols ResultColumns
	for _, v := range sg.SingletonVars {
		var colType types.T
		if _, isVertex := sg.Vertices[v.Name.L]; isVertex {
			colType = types.Vertex
		} else {
			colType = types.Edge
		}
		cols = append(cols, ResultColumn{
			Name: v.Name,
			Type: colType,
		})
	}
	return cols
}
