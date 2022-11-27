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

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/storage/mvcc"
)

type snapshot struct {
	db  *pebble.DB
	ver mvcc.Version
}

// Get implements the Snapshot interface.
func (s *snapshot) Get(_ context.Context, key Key) ([]byte, error) {
	return s.get(key, nil)
}

// Iter implements the Snapshot interface.
func (s *snapshot) Iter(lowerBound Key, upperBound Key) (Iterator, error) {
	// The lower-level database stored key-value with versions. We need
	// to append the version to the raw keys.
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
	if ok := inner.First(); !ok {
		_ = inner.Close()
		return nil, errors.New("invalid bound")
	}

	// Handle startKey is nil, in this case, the real startKey
	// should be changed the first key of the lower-level database.
	if inner.Valid() {
		key, _, err := mvcc.Decode(inner.Key())
		if err != nil {
			// Close the inner iterator if error encountered.
			_ = inner.Close()
			return nil, err
		}
		lowerBound = key
	}

	iter := &iterator{
		inner:   inner,
		ver:     s.ver,
		nextKey: lowerBound,
	}

	return iter, iter.Next()
}

// IterReverse implements the Snapshot interface.
func (s *snapshot) IterReverse(lowerBound Key, upperBound Key) (Iterator, error) {
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
	ok := inner.Last()
	if !ok {
		_ = inner.Close()
		return nil, errors.New("invalid upper bound")
	}

	// Set the next key to the last valid key between lowerBound and upperBound.
	key, _, err := mvcc.Decode(inner.Key())
	if err != nil {
		_ = inner.Close()
		return nil, err
	}
	iter := &reverseIterator{
		inner:   inner,
		ver:     s.ver,
		nextKey: key,
	}
	return iter, iter.Next()
}

func (s *snapshot) BatchGet(_ context.Context, keys []Key) (map[string][]byte, error) {
	results := map[string][]byte{}
	for _, key := range keys {
		value, err := s.get(key, nil)
		if err != nil {
			// TODO: backoff if locked keys encountered.
			//locked, ok := err.(*LockedError)
			//if !ok {
			//	return nil, err
			//}
			return nil, err
		}
		results[string(key)] = value
	}

	// TODO: resolve locks and backoff

	return results, nil
}

func (s *snapshot) get(key Key, resolvedLocks []uint64) ([]byte, error) {
	iter := s.db.NewIter(&pebble.IterOptions{LowerBound: mvcc.Encode(key, mvcc.LockVer)})
	defer iter.Close()

	// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
	// return false). We must to call First or Last to position the iterator.
	if ok := iter.First(); !ok {
		return nil, errors.New("invalid key")
	}
	return getValue(iter, key, s.ver.Ver, resolvedLocks)
}

func getValue(iter *pebble.Iterator, key Key, startTS uint64, resolvedLocks []uint64) ([]byte, error) {
	dec1 := mvcc.LockDecoder{ExpectKey: key}
	ok, err := dec1.Decode(iter)
	if ok {
		startTS, err = dec1.Lock.Check(startTS, key, resolvedLocks)
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
		// Read the first committed value that can be seen at startTS.
		if value.CommitTS <= startTS {
			if value.Type == mvcc.ValueTypeDelete {
				return nil, nil
			}
			return value.Value, nil
		}
	}
	return nil, nil
}
