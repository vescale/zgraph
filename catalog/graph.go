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

// Graph represents a runtime graph object.
type Graph struct {
	mu sync.RWMutex

	// NOTE: DON'T CHANGE THE CONTENT OF THIS POINTER.
	// The information object will be used by multiple package, and we need to clone a
	// new object if we want to modify it and keep the original one immutable.
	meta   *model.GraphInfo
	byName map[string]*Label
	byID   map[int64]*Label
}

// NewGraph returns a graph instance.
func NewGraph(meta *model.GraphInfo) *Graph {
	g := &Graph{
		meta:   meta,
		byName: map[string]*Label{},
		byID:   map[int64]*Label{},
	}
	for _, l := range meta.Labels {
		label := NewLabel(l)
		g.byName[l.Name.L] = label
		g.byID[l.ID] = label
	}
	return g
}

// Meta returns the meta information object of this graph.
func (g *Graph) Meta() *model.GraphInfo {
	return g.meta
}

// Label returns the label of specified name.
func (g *Graph) Label(name string) *Label {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.byName[strings.ToLower(name)]
}

// LabelByID returns the label of specified ID.
func (g *Graph) LabelByID(id int64) *Label {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.byID[id]
}

// Labels returns the labels.
func (g *Graph) Labels() []*Label {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if len(g.byID) < 1 {
		return nil
	}
	labels := make([]*Label, 0, len(g.byID))
	for _, label := range g.byID {
		labels = append(labels, label)
	}
	return labels
}
