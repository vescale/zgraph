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

import (
	"strings"
	"sync"

	"github.com/vescale/zgraph/parser/model"
)

// Label represents a runtime label object.
type Label struct {
	mu sync.RWMutex

	meta   *model.LabelInfo
	byName map[string]*Index
	byID   map[int64]*Index
}

// NewLabel returns a label instance.
func NewLabel(meta *model.LabelInfo) *Label {
	l := &Label{
		meta:   meta,
		byName: map[string]*Index{},
		byID:   map[int64]*Index{},
	}
	for _, i := range meta.Indexes {
		index := NewIndex(i)
		l.byName[i.Name.L] = index
		l.byID[i.ID] = index
	}
	return l
}

// Meta returns the meta information object of this label.
func (l *Label) Meta() *model.LabelInfo {
	return l.meta
}

// Index returns the label of specified name.
func (l *Label) Index(name string) *Index {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.byName[strings.ToLower(name)]
}

// IndexByID returns the label of specified ID.
func (l *Label) IndexByID(id int64) *Index {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.byID[id]
}
