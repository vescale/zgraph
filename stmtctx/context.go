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

package stmtctx

import (
	"strings"
	"sync"

	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/storage/kv"
)

// Context represent the intermediate state of a query execution and will be
// reset after a query finished.
type Context struct {
	store   kv.Storage
	catalog *catalog.Catalog

	mu struct {
		sync.RWMutex

		currentGraph string
		txn          *LazyTxn

		affectedRows uint64
		foundRows    uint64
		records      uint64
		deleted      uint64
		updated      uint64
		copied       uint64
		touched      uint64

		warnings   []SQLWarn
		errorCount uint16
	}
}

// New returns a session statement context instance.
func New(store kv.Storage, catalog *catalog.Catalog) *Context {
	return &Context{
		store:   store,
		catalog: catalog,
	}
}

// Reset resets all variables associated to execute a query.
func (sc *Context) Reset() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.mu.affectedRows = 0
	sc.mu.foundRows = 0
	sc.mu.records = 0
	sc.mu.deleted = 0
	sc.mu.updated = 0
	sc.mu.copied = 0
	sc.mu.touched = 0
	sc.mu.warnings = sc.mu.warnings[:0]
	sc.mu.errorCount = 0
}

// Store returns the storage instance.
func (sc *Context) Store() kv.Storage {
	return sc.store
}

// Catalog returns the catalog object.
func (sc *Context) Catalog() *catalog.Catalog {
	return sc.catalog
}

// CurrentGraph returns the current chosen graph name.
func (sc *Context) CurrentGraph() string {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return sc.mu.currentGraph
}

// SetCurrentGraph changes the current graph name.
func (sc *Context) SetCurrentGraph(graphName string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.mu.currentGraph = strings.ToLower(graphName)
}
