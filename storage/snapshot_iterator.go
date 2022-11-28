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

package storage

import (
	"bytes"

	"github.com/cockroachdb/pebble"
	"github.com/vescale/zgraph/storage/kv"
	"github.com/vescale/zgraph/storage/mvcc"
)

type SnapshotIter struct {
	inner   *pebble.Iterator
	ver     mvcc.Version
	key     kv.Key
	val     []byte
	nextKey kv.Key
	valid   bool
}

// Valid implements the Iterator interface.
func (i *SnapshotIter) Valid() bool {
	return i.valid
}

// Key implements the Iterator interface.
func (i *SnapshotIter) Key() kv.Key {
	return i.key
}

// Value implements the Iterator interface.
func (i *SnapshotIter) Value() []byte {
	return i.val
}

// Next implements the Iterator interface.
func (i *SnapshotIter) Next() error {
	i.valid = i.inner.Valid()
	if !i.valid {
		return nil
	}

	for hasNext := true; hasNext; {
		// TODO: handle LockedError and reset the resolvedLocks.
		val, err := getValue(i.inner, i.nextKey, i.ver, nil)
		if err != nil {
			return err
		}

		// We cannot early return here because we must skip the remained
		// versions and set the next key properly.
		if val != nil {
			i.key = i.nextKey
			i.val = val
		}

		// Skip the remained multiple versions if we found a valid value.
		// Or seek to the next valid key.
		skip := mvcc.SkipDecoder{CurrKey: i.nextKey}
		hasNext, err = skip.Decode(i.inner)
		if err != nil {
			return err
		}
		i.nextKey = skip.CurrKey

		// Early return if we found a valid value.
		if val != nil {
			return nil
		}
	}

	// This position means we didn't find a valid value in the above loop.
	// We should set the valid flag into false to avoid the caller read
	// previous key/value.
	i.valid = false

	return nil
}

// Close implements the Iterator interface.
func (i *SnapshotIter) Close() {
	_ = i.inner.Close()
}

type SnapshotReverseIter struct {
	inner   *pebble.Iterator
	ver     mvcc.Version
	key     kv.Key
	val     []byte
	nextKey kv.Key
	entry   mvcc.Entry
	valid   bool
}

// Valid implements the Iterator interface.
func (r *SnapshotReverseIter) Valid() bool {
	return r.valid
}

// Key implements the Iterator interface.
func (r *SnapshotReverseIter) Key() kv.Key {
	return r.key
}

// Value implements the Iterator interface.
func (r *SnapshotReverseIter) Value() []byte {
	return r.val
}

// Next implements the Iterator interface.
func (r *SnapshotReverseIter) Next() error {
	r.valid = r.inner.Valid()
	if !r.valid {
		return nil
	}

	for hasPrev := true; hasPrev; {
		key, ver, err := mvcc.Decode(r.inner.Key())
		if err != nil {
			return err
		}
		if !bytes.Equal(key, r.nextKey) {
			err := r.finishEntry()
			if err != nil {
				return err
			}
			r.key = r.nextKey
			r.nextKey = key

			// Early return if we found a valid value. The SnapshotReverseIter will
			// continue to decode the next different key because the inner iterator
			// had pointed to next different key.
			if len(r.val) > 0 {
				return nil
			}
		}
		val, err := r.inner.ValueAndErr()
		if err != nil {
			return err
		}
		if ver == mvcc.LockVer {
			var lock mvcc.Lock
			err = lock.UnmarshalBinary(val)
		} else {
			var value mvcc.Value
			err = value.UnmarshalBinary(val)
			r.entry.Values = append(r.entry.Values, value)
		}
		if err != nil {
			return err
		}
		hasPrev = r.inner.Prev()

		// Set the key/value to properly value if there is no previous key/value.
		if !hasPrev {
			err := r.finishEntry()
			if err != nil {
				return err
			}
			r.key = r.nextKey
		}
	}

	// This position means there is no previous key in the specified range of iterator.
	// The `finishEntry` method always was called even there is no previous key remained
	// because there maybe some old versions are stored in `entry.values`.
	// The r.val nil means we didn't find any valid data for the current key. So, we
	// should set the valid flag into false to avoid the caller read previous key/value.
	if len(r.val) == 0 {
		r.valid = false
	}

	return nil
}

func reverse(values []mvcc.Value) {
	i, j := 0, len(values)-1
	for i < j {
		values[i], values[j] = values[j], values[i]
		i++
		j--
	}
}

func (r *SnapshotReverseIter) finishEntry() error {
	reverse(r.entry.Values)
	r.entry.Key = mvcc.NewKey(r.nextKey)
	// TODO: resolvedLocks
	val, err := r.entry.Get(r.ver, nil)
	if err != nil {
		return err
	}
	r.val = val
	r.entry = mvcc.Entry{}
	return nil
}

// Close implements the Iterator interface.
func (r *SnapshotReverseIter) Close() {
	_ = r.inner.Close()
}
