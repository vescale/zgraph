// Copyright 2022 zGraph Authors. All rights reserved.
//
// Copyright 2020 PingCAP, Inc.
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

package storage

import (
	"bytes"

	"github.com/vescale/zgraph/storage/kv"
)

// MemDBIter is an Iterator with KeyFlags related functions.
type MemDBIter struct {
	db           *MemDB
	curr         memdbNodeAddr
	start        []byte
	end          []byte
	reverse      bool
	includeFlags bool
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (db *MemDB) Iter(lowerBound, upperBound kv.Key) (kv.Iterator, error) {
	i := &MemDBIter{
		db:    db,
		start: lowerBound,
		end:   upperBound,
	}
	i.init()
	return i, nil
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
// The returned SnapshotIter will iterate from greater key to smaller key.
// If k is nil, the returned SnapshotIter will be positioned at the last key.
func (db *MemDB) IterReverse(lowerBound, upperBound kv.Key) (kv.Iterator, error) {
	i := &MemDBIter{
		db:      db,
		start:   lowerBound,
		end:     upperBound,
		reverse: true,
	}
	i.init()
	return i, nil
}

// IterWithFlags returns a MemDBIter.
func (db *MemDB) IterWithFlags(lowerBound []byte, upperBound []byte) *MemDBIter {
	i := &MemDBIter{
		db:           db,
		start:        lowerBound,
		end:          upperBound,
		includeFlags: true,
	}
	i.init()
	return i
}

// IterReverseWithFlags returns a reversed MemDBIter.
func (db *MemDB) IterReverseWithFlags(k []byte) *MemDBIter {
	i := &MemDBIter{
		db:           db,
		end:          k,
		reverse:      true,
		includeFlags: true,
	}
	i.init()
	return i
}

func (i *MemDBIter) init() {
	if i.reverse {
		if len(i.end) == 0 {
			i.seekToLast()
		} else {
			i.seek(i.end)
		}
	} else {
		if len(i.start) == 0 {
			i.seekToFirst()
		} else {
			i.seek(i.start)
		}
	}

	if i.isFlagsOnly() && !i.includeFlags {
		err := i.Next()
		_ = err // memdbIterator will never fail
	}
}

// Valid returns true if the current SnapshotIter is valid.
func (i *MemDBIter) Valid() bool {
	if !i.reverse {
		return !i.curr.isNull() && (i.end == nil || bytes.Compare(i.Key(), i.end) < 0)
	}
	return !i.curr.isNull()
}

// Flags returns flags belong to current SnapshotIter.
func (i *MemDBIter) Flags() kv.KeyFlags {
	return i.curr.getKeyFlags()
}

// UpdateFlags updates and apply with flagsOp.
func (i *MemDBIter) UpdateFlags(ops ...kv.FlagsOp) {
	origin := i.curr.getKeyFlags()
	n := kv.ApplyFlagsOps(origin, ops...)
	i.curr.setKeyFlags(n)
}

// HasValue returns false if it is flags only.
func (i *MemDBIter) HasValue() bool {
	return !i.isFlagsOnly()
}

// Key returns current key.
func (i *MemDBIter) Key() kv.Key {
	return i.curr.getKey()
}

// Handle returns MemKeyHandle with the current position.
func (i *MemDBIter) Handle() MemKeyHandle {
	return MemKeyHandle{
		idx: uint16(i.curr.addr.idx),
		off: i.curr.addr.off,
	}
}

// Value returns the value.
func (i *MemDBIter) Value() []byte {
	return i.db.vlog.getValue(i.curr.vptr)
}

// Next goes the next position.
func (i *MemDBIter) Next() error {
	for {
		if i.reverse {
			i.curr = i.db.predecessor(i.curr)
		} else {
			i.curr = i.db.successor(i.curr)
		}

		// We need to skip persistent flags only nodes.
		if i.includeFlags || !i.isFlagsOnly() {
			break
		}
	}
	return nil
}

// Close closes the current SnapshotIter.
func (i *MemDBIter) Close() {}

func (i *MemDBIter) seekToFirst() {
	y := memdbNodeAddr{nil, nullAddr}
	x := i.db.getNode(i.db.root)

	for !x.isNull() {
		y = x
		x = y.getLeft(i.db)
	}

	i.curr = y
}

func (i *MemDBIter) seekToLast() {
	y := memdbNodeAddr{nil, nullAddr}
	x := i.db.getNode(i.db.root)

	for !x.isNull() {
		y = x
		x = y.getRight(i.db)
	}

	i.curr = y
}

func (i *MemDBIter) seek(key []byte) {
	y := memdbNodeAddr{nil, nullAddr}
	x := i.db.getNode(i.db.root)

	var cmp int
	for !x.isNull() {
		y = x
		cmp = bytes.Compare(key, y.getKey())

		if cmp < 0 {
			x = y.getLeft(i.db)
		} else if cmp > 0 {
			x = y.getRight(i.db)
		} else {
			break
		}
	}

	if !i.reverse {
		if cmp > 0 {
			// Move to next
			i.curr = i.db.successor(y)
			return
		}
		i.curr = y
		return
	}

	if cmp <= 0 && !y.isNull() {
		i.curr = i.db.predecessor(y)
		return
	}
	i.curr = y
}

func (i *MemDBIter) isFlagsOnly() bool {
	return !i.curr.isNull() && i.curr.vptr.isNull()
}
