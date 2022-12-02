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
	"context"
	"sync"

	"github.com/cenkalti/backoff"
	"github.com/cockroachdb/pebble"
	"github.com/vescale/zgraph/storage/kv"
	"github.com/vescale/zgraph/storage/mvcc"
	"github.com/vescale/zgraph/storage/resolver"
)

// KVSnapshot represent the MVCC snapshot of the low-level key/value store.
// All values read from the KVSnapshot will be checked via mvcc.Version.
// And only the committed key/values can be retrieved or iterated.
type KVSnapshot struct {
	db       *pebble.DB
	vp       mvcc.VersionProvider
	ver      mvcc.Version
	resolver *resolver.Scheduler

	// The KVSnapshot instance may be accessed concurrently.
	mu struct {
		sync.RWMutex
		resolved []mvcc.Version
	}
}

// Get implements the Snapshot interface.
func (s *KVSnapshot) Get(_ context.Context, key kv.Key) ([]byte, error) {
	var val []byte
	err := backoff.RetryNotify(func() error {
		v, err := s.get(key)
		if err != nil {
			lockedErr, ok := err.(*mvcc.LockedError)
			if !ok {
				return &backoff.PermanentError{Err: err}
			}

			// Try to resolve lock
			status, err := resolver.CheckTxnStatus(s.db, s.vp, lockedErr.Primary, lockedErr.StartVer)
			if err != nil {
				return &backoff.PermanentError{Err: err}
			}
			switch status.Action {
			case resolver.TxnActionNone:
				// Transaction is still alive and try it letter.
				return resolver.ErrRetryable("txn still alive")

			case resolver.TxnActionTTLExpireRollback,
				resolver.TxnActionLockNotExistRollback:
				// Resolve the current key.
				s.resolver.Resolve([]kv.Key{key}, lockedErr.StartVer, 0, nil)
				// Put the resolve transaction into the resolved list to make the
				// subsequent request bypass them.
				s.mu.Lock()
				s.mu.resolved = append(s.mu.resolved, lockedErr.StartVer)
				s.mu.Unlock()
				return resolver.ErrRetryable("bypass rollback transaction")

			default:
				// TxnActionLockNotExistDoNothing
				// Transaction committed: we try to resolve the current key and backoff.
				s.resolver.Resolve([]kv.Key{key}, lockedErr.StartVer, status.CommitVer, nil)
				return resolver.ErrRetryable("resolving committed transaction")
			}
		}
		val = v
		return nil
	}, expoBackoff(), BackoffErrReporter("KVSnapshot.Get"))

	return val, err
}

// Iter implements the Snapshot interface.
func (s *KVSnapshot) Iter(lowerBound kv.Key, upperBound kv.Key) (Iterator, error) {
	// The lower-level database stored key-value with versions. We need
	// to append the startVer to the raw keys.
	var start, end mvcc.Key
	if len(lowerBound) > 0 {
		start = mvcc.Encode(lowerBound, mvcc.LockVer)
	}
	if len(upperBound) > 0 {
		end = mvcc.Encode(upperBound, mvcc.LockVer)
	}

	inner := s.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})

	// Ignore the return boolean value of positioning the cursor of the iterator
	// to the first key/value. The inner iterator status of the field `valid` will
	// be same as the returned value of `inner.First()`. So it will be checked
	// while the `Next` method calling.
	_ = inner.First()

	iter := &SnapshotIter{
		db:       s.db,
		inner:    inner,
		resolver: s.resolver,
		ver:      s.ver,
	}

	// Handle startKey is nil, in this case, the real startKey
	// should be changed the first key of the lower-level database.
	if inner.Valid() {
		key, _, err := mvcc.Decode(inner.Key())
		if err != nil {
			// Close the inner SnapshotIter if error encountered.
			_ = inner.Close()
			return nil, err
		}
		iter.nextKey = key
	}

	return iter, iter.Next()
}

// IterReverse implements the Snapshot interface.
func (s *KVSnapshot) IterReverse(lowerBound kv.Key, upperBound kv.Key) (Iterator, error) {
	var start, end mvcc.Key
	if len(lowerBound) > 0 {
		start = mvcc.Encode(lowerBound, mvcc.LockVer)
	}
	if len(upperBound) > 0 {
		end = mvcc.Encode(upperBound, mvcc.LockVer)
	}

	inner := s.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})

	// Ignore the return boolean value of positioning the cursor of the iterator
	// to the last key/value. The inner iterator status of the field `valid` will
	// be same as the returned value of `inner.Last()`. So it will be checked
	// while the `Next` method calling.
	_ = inner.Last()

	iter := &SnapshotIter{
		db:       s.db,
		reverse:  true,
		inner:    inner,
		resolver: s.resolver,
		ver:      s.ver,
	}

	// Set the next key to the last valid key between lowerBound and upperBound.
	if inner.Valid() {
		key, _, err := mvcc.Decode(inner.Key())
		if err != nil {
			_ = inner.Close()
			return nil, err
		}
		iter.nextKey = key
	}

	return iter, iter.Next()
}

func (s *KVSnapshot) BatchGet(_ context.Context, keys []kv.Key) (map[string][]byte, error) {
	results := map[string][]byte{}
	err := backoff.RetryNotify(func() error {
		rollbacks := map[mvcc.Version][]kv.Key{}
		committed := map[mvcc.VersionPair][]kv.Key{}
		for _, key := range keys {
			_, found := results[string(key)]
			if found {
				continue
			}

			value, err := s.get(key)
			if err != nil {
				lockedErr, ok := err.(*mvcc.LockedError)
				if !ok {
					return &backoff.PermanentError{Err: err}
				}

				// Try to resolve lock
				status, err := resolver.CheckTxnStatus(s.db, s.vp, lockedErr.Primary, lockedErr.StartVer)
				if err != nil {
					return &backoff.PermanentError{Err: err}
				}
				switch status.Action {
				case resolver.TxnActionNone:
					// Transaction is still alive and try it letter.
					continue

				case resolver.TxnActionTTLExpireRollback,
					resolver.TxnActionLockNotExistRollback:
					// Resolve the current key.
					rollbacks[lockedErr.StartVer] = append(rollbacks[lockedErr.StartVer], key)
					continue

				default:
					// TxnActionLockNotExistDoNothing
					// Transaction committed: we try to resolve the current key and backoff.
					pair := mvcc.VersionPair{StartVer: lockedErr.StartVer, CommitVer: status.CommitVer}
					committed[pair] = append(committed[pair], key)
					continue
				}
			}
			results[string(key)] = value
		}

		if len(rollbacks) > 0 {
			for startVer, keys := range rollbacks {
				s.resolver.Resolve(keys, startVer, 0, nil)
				// Put the resolve transaction into the resolved list to make the
				// subsequent request bypass them.
				s.mu.Lock()
				s.mu.resolved = append(s.mu.resolved, startVer)
				s.mu.Unlock()
			}
		}
		if len(committed) > 0 {
			for pair, keys := range committed {
				s.resolver.Resolve(keys, pair.StartVer, pair.CommitVer, nil)
			}
		}

		if len(results) != len(keys) {
			return resolver.ErrRetryable("some keys still resolving")
		}
		return nil
	}, expoBackoff(), BackoffErrReporter("KVSnapshot.BatchGet"))

	return results, err
}

func (s *KVSnapshot) get(key kv.Key) ([]byte, error) {
	opt := pebble.IterOptions{LowerBound: mvcc.Encode(key, mvcc.LockVer)}
	iter := s.db.NewIter(&opt)
	iter.First()
	defer iter.Close()

	s.mu.RLock()
	resolved := s.mu.resolved
	s.mu.RUnlock()

	return getValue(iter, key, s.ver, resolved)
}

func getValue(iter *pebble.Iterator, key kv.Key, startVer mvcc.Version, resolvedLocks []mvcc.Version) ([]byte, error) {
	dec1 := mvcc.LockDecoder{ExpectKey: key}
	ok, err := dec1.Decode(iter)
	if ok {
		startVer, err = dec1.Lock.Check(startVer, key, resolvedLocks)
	}
	if err != nil {
		return nil, err
	}
	dec2 := mvcc.ValueDecoder{ExpectKey: key}
	for iter.Valid() {
		ok, err := dec2.Decode(iter)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		value := &dec2.Value
		if value.Type == mvcc.ValueTypeRollback ||
			value.Type == mvcc.ValueTypeLock {
			continue
		}
		// Read the first committed value that can be seen at startVer.
		if value.CommitVer <= startVer {
			if value.Type == mvcc.ValueTypeDelete {
				return nil, nil
			}
			return value.Value, nil
		}
	}
	return nil, nil
}
