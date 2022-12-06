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
	labels struct {
		byName map[string]*Label
		byID   map[int64]*Label
	}
	properties struct {
		byName map[string]*model.PropertyInfo
		byID   map[uint16]*model.PropertyInfo
	}
	indexes struct {
		byName map[string]*Index
		byID   map[int64]*Index
	}
}

// NewGraph returns a graph instance.
func NewGraph(meta *model.GraphInfo) *Graph {
	g := &Graph{meta: meta}

	g.labels.byName = map[string]*Label{}
	g.labels.byID = map[int64]*Label{}
	for _, l := range meta.Labels {
		label := NewLabel(l)
		g.labels.byName[l.Name.L] = label
		g.labels.byID[l.ID] = label
	}

	g.properties.byName = map[string]*model.PropertyInfo{}
	g.properties.byID = map[uint16]*model.PropertyInfo{}
	for _, p := range meta.Properties {
		g.properties.byName[p.Name.L] = p
		g.properties.byID[p.ID] = p
	}

	g.indexes.byName = map[string]*Index{}
	g.indexes.byID = map[int64]*Index{}
	for _, idx := range meta.Indexes {
		index := NewIndex(idx)
		g.indexes.byName[idx.Name.L] = index
		g.indexes.byID[idx.ID] = index
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

	return g.labels.byName[strings.ToLower(name)]
}

// LabelByID returns the label of specified ID.
func (g *Graph) LabelByID(id int64) *Label {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.labels.byID[id]
}

// Property returns the property of specified name.
func (g *Graph) Property(name string) *model.PropertyInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.properties.byName[strings.ToLower(name)]
}

// PropertyByID returns the property of specified ID.
func (g *Graph) PropertyByID(id uint16) *model.PropertyInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.properties.byID[id]
}

// Labels returns the labels.
func (g *Graph) Labels() []*Label {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if len(g.labels.byID) < 1 {
		return nil
	}
	labels := make([]*Label, 0, len(g.labels.byID))
	for _, label := range g.labels.byID {
		labels = append(labels, label)
	}
	return labels
}

// Properties returns the Properties.
func (g *Graph) Properties() []*model.PropertyInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.meta.Properties
}

// CreateLabel create a new label and append to the graph labels list.
func (g *Graph) CreateLabel(labelInfo *model.LabelInfo) {
	g.mu.Lock()
	defer g.mu.Unlock()

	label := NewLabel(labelInfo)
	g.labels.byName[labelInfo.Name.L] = label
	g.labels.byID[labelInfo.ID] = label
}

// DropLabel removes specified label from graph.
func (g *Graph) DropLabel(labelInfo *model.LabelInfo) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.labels.byName, labelInfo.Name.L)
	delete(g.labels.byID, labelInfo.ID)
}

// CreateProperty create a new property and append to the graph properties list.
func (g *Graph) CreateProperty(propertyInfo *model.PropertyInfo) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.properties.byName[propertyInfo.Name.L] = propertyInfo
	g.properties.byID[propertyInfo.ID] = propertyInfo
}

// DropProperty removes specified property from graph.
func (g *Graph) DropProperty(propertyInfo *model.PropertyInfo) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.properties.byName, propertyInfo.Name.L)
	delete(g.properties.byID, propertyInfo.ID)
}

// Index returns the label of specified name.
func (g *Graph) Index(name string) *Index {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.indexes.byName[strings.ToLower(name)]
}

// IndexByID returns the label of specified ID.
func (g *Graph) IndexByID(id int64) *Index {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.indexes.byID[id]
}

// Indexes returns the indexes.
func (g *Graph) Indexes() []*Index {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if len(g.indexes.byID) < 1 {
		return nil
	}
	indexes := make([]*Index, 0, len(g.indexes.byID))
	for _, index := range g.indexes.byID {
		indexes = append(indexes, index)
	}
	return indexes
}

func (g *Graph) SetNextPropID(propID uint16) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.meta.NextPropID = propID
}
