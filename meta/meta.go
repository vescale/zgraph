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

package meta

import (
	"sync"

	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/internal/structure"
	"github.com/vescale/zgraph/storage/kv"
)

// The globalIDMutex is used to reduce the global id key conflicts.
var globalIDMutex sync.Mutex

var (
	mMetaKeyPrefix   = []byte("m")
	mNextGlobalIDKey = []byte("next_global_id")
	mNextIDKey       = []byte("next_id")
	mGraphs          = []byte("graphs")
	mGraphPrefix     = "graph"
	mLabelPrefix     = "label"
	mPropertyPrefix  = "property"
)

const (
	// CurrentMagicByteVer is the current magic byte version, used for future meta compatibility.
	CurrentMagicByteVer byte = 0x00
	// PolicyMagicByte handler
	// 0x00 - 0x3F: Json Handler
	// 0x40 - 0x7F: Reserved
	// 0x80 - 0xBF: Reserved
	// 0xC0 - 0xFF: Reserved

	// MaxInt48 is the max value of int48.
	MaxInt48 = 0x0000FFFFFFFFFFFF
	// MaxGlobalID reserves 1000 IDs. Use MaxInt48 to reserves the high 2 bytes to compatible with Multi-tenancy.
	MaxGlobalID = MaxInt48 - 1000
)

// Meta is for handling meta information in a transaction.
type Meta struct {
	txn      *structure.TxStructure
	StartVer kv.Version
}

// New returns a new instance of meta API object.
func New(txn kv.Transaction) *Meta {
	t := structure.NewStructure(txn, txn, mMetaKeyPrefix)
	return &Meta{
		txn:      t,
		StartVer: txn.StartVer(),
	}
}

// NewSnapshot returns a read-only new instance of meta API object.
func NewSnapshot(snap kv.Snapshot) *Meta {
	t := structure.NewStructure(snap, nil, mMetaKeyPrefix)
	return &Meta{
		txn:      t,
		StartVer: snap.StartVer(),
	}
}

// NextGlobalID generates next id globally.
func (m *Meta) NextGlobalID() (int64, error) {
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()

	newID, err := m.txn.Inc(mNextGlobalIDKey, 1)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if newID > MaxGlobalID {
		return 0, errors.Errorf("global id:%d exceeds the limit:%d", newID, MaxGlobalID)
	}
	return newID, err
}

// AdvanceGlobalID advances the global ID by n.
// return the old global ID.
func (m *Meta) AdvanceGlobalID(n int) (int64, error) {
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()

	newID, err := m.txn.Inc(mNextGlobalIDKey, int64(n))
	if err != nil {
		return 0, err
	}
	if newID > MaxGlobalID {
		return 0, errors.Errorf("global id:%d exceeds the limit:%d", newID, MaxGlobalID)
	}
	origID := newID - int64(n)
	return origID, nil
}

// GenGlobalIDs generates the next n global IDs.
func (m *Meta) GenGlobalIDs(n int) ([]int64, error) {
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()

	newID, err := m.txn.Inc(mNextGlobalIDKey, int64(n))
	if err != nil {
		return nil, err
	}
	if newID > MaxGlobalID {
		return nil, errors.Errorf("global id:%d exceeds the limit:%d", newID, MaxGlobalID)
	}
	origID := newID - int64(n)
	ids := make([]int64, 0, n)
	for i := origID + 1; i <= newID; i++ {
		ids = append(ids, i)
	}
	return ids, nil
}

// GlobalID gets current global id.
func (m *Meta) GlobalID() (int64, error) {
	return m.txn.GetInt64(mNextGlobalIDKey)
}
