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

	"github.com/vescale/zgraph/meta"
	"github.com/vescale/zgraph/storage/kv"
)

// Catalog maintains the catalog of graphs and label information.
type Catalog struct {
	// mdl prevent executing DDL concurrently.
	mdl sync.Mutex

	// mu protect the catalog fields.
	mu     sync.RWMutex
	byName map[string]*Graph
	byID   map[int64]*Graph
}

// Load loads the catalog from a kv snapshot.
func Load(snapshot kv.Snapshot) (*Catalog, error) {
	c := &Catalog{
		byName: map[string]*Graph{},
		byID:   map[int64]*Graph{},
	}

	meta := meta.NewSnapshot(snapshot)
	graphs, err := meta.ListGraphs()
	if err != nil {
		return nil, err
	}
	for _, g := range graphs {
		// Load labels
		labels, err := meta.ListLabels(g.ID)
		if err != nil {
			return nil, err
		}
		g.Labels = labels

		// Load properties
		properties, err := meta.ListProperties(g.ID)
		if err != nil {
			return nil, err
		}
		g.Properties = properties

		// Build graph instance.
		graph := NewGraph(g)
		c.byName[g.Name.L] = graph
		c.byID[g.ID] = graph
	}

	return c, nil
}

// Graph returns the graph of specified name.
func (c *Catalog) Graph(name string) *Graph {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.byName[strings.ToLower(name)]
}

// GraphByID returns the graph of specified ID.
func (c *Catalog) GraphByID(id int64) *Graph {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.byID[id]
}

// Graphs returns all graphs.
func (c *Catalog) Graphs() []*Graph {
	c.mu.RLock()
	defer c.mu.RUnlock()

	graphs := make([]*Graph, 0, len(c.byName))
	for _, g := range c.byName {
		graphs = append(graphs, g)
	}

	return graphs
}

// Label returns the label of specified graph.
func (c *Catalog) Label(graphName, labelName string) *Label {
	g := c.Graph(graphName)
	if g == nil {
		return nil
	}

	return g.Label(labelName)
}

// LabelByID returns the label of specified graph.
func (c *Catalog) LabelByID(graphID, labelID int64) *Label {
	g := c.GraphByID(graphID)
	if g == nil {
		return nil
	}

	return g.LabelByID(labelID)
}

// Labels returns all labels of specified graph.
func (c *Catalog) Labels(graphName string) []*Label {
	g := c.Graph(graphName)
	if g == nil {
		return nil
	}

	return g.Labels()
}

// MDLock locks the catalog to prevent executing DDL concurrently.
func (c *Catalog) MDLock() {
	c.mdl.Lock()
}

// MDUnlock unlocks the catalog.
func (c *Catalog) MDUnlock() {
	c.mdl.Unlock()
}
