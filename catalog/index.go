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

package catalog

import "github.com/vescale/zgraph/parser/model"

// Index represents a runtime index object.
type Index struct {
	meta *model.IndexInfo
}

// NewIndex returns a new index object.
func NewIndex(meta *model.IndexInfo) *Index {
	return &Index{
		meta: meta,
	}
}

// Meta returns the meta information object of this index.
func (i *Index) Meta() *model.IndexInfo {
	return i.meta
}