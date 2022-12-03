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
	"sync"

	"github.com/vescale/zgraph/meta"
	"github.com/vescale/zgraph/storage/kv"
)

// Catalog maintains the catalog of graphs and label information.
type Catalog struct {
	mu sync.RWMutex

	byName map[string]*Graph
	byID   map[int64]*Graph
}

// New returns a catalog instance.
func New() *Catalog {
	return &Catalog{
		byName: map[string]*Graph{},
		byID:   map[int64]*Graph{},
	}
}

// Load loads the catalog from a kv snapshot.
func (c *Catalog) Load(snapshot kv.Snapshot) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	meta := meta.NewSnapshot(snapshot)
	graphs, err := meta.ListGraphs()
	if err != nil {
		return err
	}
	for _, g := range graphs {
		graph := NewGraph(g)
		c.byName[g.Name.L] = graph
		c.byID[g.ID] = graph
	}

	return nil
}
