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
	"sync/atomic"

	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/meta"
	"github.com/vescale/zgraph/storage/kv"
)

// IDRange represents an ID range. The ID range will be (base, max]
type IDRange struct {
	base atomic.Int64
	max  int64
}

// NewIDRange returns a new ID range.
func NewIDRange(base, max int64) *IDRange {
	idr := &IDRange{
		max: max,
	}
	idr.base.Store(base)
	return idr
}

// Next retrieves the next available ID.
func (r *IDRange) Next() (int64, error) {
	next := r.base.Add(1)
	if next > r.max {
		return 0, ErrIDExhaust
	}
	return next, nil
}

// AllocID allocates n IDs.
func (sc *Context) AllocID(graph *catalog.Graph, n int) (*IDRange, error) {
	graph.MDLock()
	defer graph.MDUnlock()

	var idRange *IDRange
	err := kv.Txn(sc.store, func(txn kv.Transaction) error {
		meta := meta.New(txn)
		base, err := meta.AdvanceID(graph.Meta().ID, n)
		if err != nil {
			return err
		}
		idRange = NewIDRange(base, base+int64(n))
		return nil
	})

	return idRange, err
}
