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

package model

// GraphInfo provides meta data describing a graph.
type GraphInfo struct {
	ID         int64           `json:"id"`
	Name       CIStr           `json:"name"`
	Indexes    []*IndexInfo    `json:"indexes"`
	NextPropID int64           `json:"next_prop_id"`
	Query      string          `json:"query"`
	Labels     []*LabelInfo    `json:"-"`
	Properties []*PropertyInfo `json:"-"`
}

// IndexInfo provides meta data describing a index.
type IndexInfo struct {
	ID         int64   `json:"id"`
	Name       CIStr   `json:"name"`
	Properties []CIStr `json:"properties"`
	Query      string  `json:"query"`
}

// LabelInfo provides meta data describing a label.
type LabelInfo struct {
	ID    int64  `json:"id"`
	Name  CIStr  `json:"name"`
	Query string `json:"query"`
}

type PropertyInfo struct {
	ID   int64 `json:"id"`
	Name CIStr `json:"name"`
}
