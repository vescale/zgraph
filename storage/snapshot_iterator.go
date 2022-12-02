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
	"sync"

	"github.com/cenkalti/backoff"
	"github.com/cockroachdb/pebble"
	"github.com/vescale/zgraph/storage/kv"
	"github.com/vescale/zgraph/storage/mvcc"
	"github.com/vescale/zgraph/storage/resolver"
)

// SnapshotIter represents a iterator which provides a consistent view of key/value store.
type SnapshotIter struct {
	db       *pebble.DB
	vp       kv.VersionProvider
	ver      kv.Version
	inner    *pebble.Iterator
	resolver *resolver.Scheduler
	mu       struct {
		sync.RWMutex
		resolved []kv.Version
	}

	// Iterator states
	valid   bool
	reverse bool
	key     kv.Key
	val     []byte
	nextKey kv.Key

	// Only for reverse iterator
	entry mvcc.Entry
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
	err := backoff.RetryNotify(func() error {
		i.valid = i.inner.Valid()
		if !i.valid {
			return nil
		}

		var err error
		if i.reverse {
			err = i.reverseNext()
		} else {
			err = i.next()
		}

		if err != nil {
			lockedErr, ok := err.(*mvcc.LockedError)
			if !ok {
				return err
			}
			// Try to resolve lock
			status, err := resolver.CheckTxnStatus(i.db, i.vp, lockedErr.Primary, lockedErr.StartVer)
			if err != nil {
				return &backoff.PermanentError{Err: err}
			}
			switch status.Action {
			case resolver.TxnActionNone:
				// Transaction is still alive and try it letter.
				err = resolver.ErrRetryable("txn still alive")

			case resolver.TxnActionTTLExpireRollback,
				resolver.TxnActionLockNotExistRollback:
				// Resolve the current key.
				i.resolver.Resolve([]kv.Key{lockedErr.Key}, lockedErr.StartVer, 0, nil)
				// Put the resolve transaction into the resolved list to make the
				// subsequent request bypass them.
				i.mu.Lock()
				i.mu.resolved = append(i.mu.resolved, lockedErr.StartVer)
				i.mu.Unlock()
				err = resolver.ErrRetryable("bypass rollback transaction")

			default:
				// TxnActionLockNotExistDoNothing
				// Transaction committed: we try to resolve the current key and backoff.
				i.resolver.Resolve([]kv.Key{lockedErr.Key}, lockedErr.StartVer, status.CommitVer, nil)
				err = resolver.ErrRetryable("resolving committed transaction")
			}

			// We must make the iterator point to the correct key.
			i.resetIter()
			return err
		}

		return nil
	}, expoBackoff(), BackoffErrReporter("SnapshotIter.Next"))

	return err
}

func (i *SnapshotIter) resetIter() {
	lowerBound, upperBound := i.inner.RangeBounds()
	if i.reverse {
		iter := i.db.NewIter(&pebble.IterOptions{
			LowerBound: lowerBound,
			UpperBound: i.nextKey.PrefixNext(),
		})
		iter.Last()
		i.inner = iter
	} else {
		iter := i.db.NewIter(&pebble.IterOptions{
			LowerBound: mvcc.LockKey(i.nextKey),
			UpperBound: upperBound,
		})
		iter.First()
		i.inner = iter
	}
}

// Close implements the Iterator interface.
func (i *SnapshotIter) Close() {
	_ = i.inner.Close()
}

func (i *SnapshotIter) next() error {
	for hasNext := true; hasNext; {
		i.mu.RLock()
		resolved := i.mu.resolved
		i.mu.RUnlock()
		val, err := getValue(i.inner, i.nextKey, i.ver, resolved)
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

func (i *SnapshotIter) reverseNext() error {
	for hasPrev := true; hasPrev; {
		key, ver, err := mvcc.Decode(i.inner.Key())
		if err != nil {
			return err
		}
		if !bytes.Equal(key, i.nextKey) {
			err := i.finishEntry()
			if err != nil {
				return err
			}
			i.key = i.nextKey
			i.nextKey = key

			// Early return if we found a valid value. The SnapshotReverseIter will
			// continue to decode the next different key because the inner iterator
			// had pointed to next different key.
			if len(i.val) > 0 {
				return nil
			}
		}
		val, err := i.inner.ValueAndErr()
		if err != nil {
			return err
		}
		if ver == mvcc.LockVer {
			var lock mvcc.Lock
			err = lock.UnmarshalBinary(val)
		} else {
			var value mvcc.Value
			err = value.UnmarshalBinary(val)
			i.entry.Values = append(i.entry.Values, value)
		}
		if err != nil {
			return err
		}
		hasPrev = i.inner.Prev()

		// Set the key/value to properly value if there is no previous key/value.
		if !hasPrev {
			err := i.finishEntry()
			if err != nil {
				return err
			}
			i.key = i.nextKey
		}
	}

	// This position means there is no previous key in the specified range of iterator.
	// The `finishEntry` method always was called even there is no previous key remained
	// because there maybe some old versions are stored in `entry.values`.
	// The i.val nil means we didn't find any valid data for the current key. So, we
	// should set the valid flag into false to avoid the caller read previous key/value.
	if len(i.val) == 0 {
		i.valid = false
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

func (i *SnapshotIter) finishEntry() error {
	if i.entry.Lock != nil {
		return i.entry.Lock.LockErr(i.nextKey)
	}

	reverse(i.entry.Values)
	i.entry.Key = mvcc.NewKey(i.nextKey)
	val, err := i.entry.Get(i.ver, nil)
	if err != nil {
		return err
	}
	i.val = val
	i.entry = mvcc.Entry{}
	return nil
}
