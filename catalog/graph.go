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
	"sync/atomic"

	"github.com/vescale/zgraph/parser/model"
)

// Graph represents a runtime graph object.
type Graph struct {
	// mdl preventing multiple threads change the graph metadata concurrently.
	mdl sync.Mutex

	// NOTE: DON'T CHANGE THE CONTENT OF THIS POINTER.
	// The information object will be used by multiple package, and we need to clone a
	// new object if we want to modify it and keep the original one immutable.
	meta atomic.Pointer[model.GraphInfo]

	labels struct {
		sync.RWMutex

		byName map[string]*Label
		byID   map[int64]*Label
	}
	properties struct {
		sync.RWMutex

		byName map[string]*model.PropertyInfo
		byID   map[uint16]*model.PropertyInfo
	}
	indexes struct {
		sync.RWMutex

		byName map[string]*Index
		byID   map[int64]*Index
	}
}

// NewGraph returns a graph instance.
func NewGraph(meta *model.GraphInfo) *Graph {
	g := &Graph{}
	g.meta.Store(meta)

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
	return g.meta.Load()
}

// Label returns the label of specified name.
func (g *Graph) Label(name string) *Label {
	g.labels.RLock()
	defer g.labels.RUnlock()

	return g.labels.byName[strings.ToLower(name)]
}

// LabelByID returns the label of specified ID.
func (g *Graph) LabelByID(id int64) *Label {
	g.labels.RLock()
	defer g.labels.RUnlock()

	return g.labels.byID[id]
}

// Property returns the property of specified name.
func (g *Graph) Property(name string) *model.PropertyInfo {
	g.properties.RLock()
	defer g.properties.RUnlock()

	return g.properties.byName[strings.ToLower(name)]
}

// PropertyByID returns the property of specified ID.
func (g *Graph) PropertyByID(id uint16) *model.PropertyInfo {
	g.properties.RLock()
	defer g.properties.RUnlock()

	return g.properties.byID[id]
}

// Labels returns the labels.
func (g *Graph) Labels() []*Label {
	g.labels.RLock()
	defer g.labels.RUnlock()

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
	return g.meta.Load().Properties
}

// CreateLabel create a new label and append to the graph labels list.
func (g *Graph) CreateLabel(labelInfo *model.LabelInfo) {
	g.labels.Lock()
	defer g.labels.Unlock()

	label := NewLabel(labelInfo)
	g.labels.byName[labelInfo.Name.L] = label
	g.labels.byID[labelInfo.ID] = label

	meta := *g.meta.Load()
	meta.Labels = append(meta.Labels, labelInfo)
	g.meta.Store(&meta)
}

// DropLabel removes specified label from graph.
func (g *Graph) DropLabel(labelInfo *model.LabelInfo) {
	g.labels.Lock()
	defer g.labels.Unlock()

	delete(g.labels.byName, labelInfo.Name.L)
	delete(g.labels.byID, labelInfo.ID)

	meta := *g.meta.Load()
	for i, l := range meta.Labels {
		if l.ID == labelInfo.ID {
			meta.Labels = append(meta.Labels[:i], meta.Labels[i+1:]...)
			break
		}
	}
	g.meta.Store(&meta)
}

// CreateProperty create a new property and append to the graph properties list.
func (g *Graph) CreateProperty(propertyInfo *model.PropertyInfo) {
	g.properties.Lock()
	defer g.properties.Unlock()

	g.properties.byName[propertyInfo.Name.L] = propertyInfo
	g.properties.byID[propertyInfo.ID] = propertyInfo

	meta := *g.meta.Load()
	meta.Properties = append(meta.Properties, propertyInfo)
	g.meta.Store(&meta)
}

// DropProperty removes specified property from graph.
func (g *Graph) DropProperty(propertyInfo *model.PropertyInfo) {
	g.properties.Lock()
	defer g.properties.Unlock()

	delete(g.properties.byName, propertyInfo.Name.L)
	delete(g.properties.byID, propertyInfo.ID)

	meta := *g.meta.Load()
	for i, p := range meta.Properties {
		if p.ID == propertyInfo.ID {
			meta.Properties = append(meta.Properties[:i], meta.Properties[i+1:]...)
			break
		}
	}
	g.meta.Store(&meta)
}

// Index returns the label of specified name.
func (g *Graph) Index(name string) *Index {
	g.indexes.RLock()
	defer g.indexes.RUnlock()

	return g.indexes.byName[strings.ToLower(name)]
}

// IndexByID returns the label of specified ID.
func (g *Graph) IndexByID(id int64) *Index {
	g.indexes.RLock()
	defer g.indexes.RUnlock()

	return g.indexes.byID[id]
}

// Indexes returns the indexes.
func (g *Graph) Indexes() []*Index {
	g.indexes.RLock()
	defer g.indexes.RUnlock()

	if len(g.indexes.byID) < 1 {
		return nil
	}
	indexes := make([]*Index, 0, len(g.indexes.byID))
	for _, index := range g.indexes.byID {
		indexes = append(indexes, index)
	}
	return indexes
}

// SetNextPropID sets the next property id.
func (g *Graph) SetNextPropID(propID uint16) {
	meta := *g.meta.Load()
	meta.NextPropID = propID
	g.meta.Store(&meta)
}

// MDLock locks the metadata of the current graph.
func (g *Graph) MDLock() {
	g.mdl.Lock()
}

// MDUnlock unlocks the metadata of the current graph.
func (g *Graph) MDUnlock() {
	g.mdl.Unlock()
}
