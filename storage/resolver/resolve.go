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

package resolver

import (
	"github.com/cockroachdb/pebble"
	"github.com/vescale/zgraph/storage/kv"
	"github.com/vescale/zgraph/storage/mvcc"
)

// Resolve resolves the specified key.
func Resolve(db *pebble.DB, batch *pebble.Batch, key kv.Key, startVer, commitVer mvcc.Version) error {
	opt := pebble.IterOptions{LowerBound: mvcc.Encode(key, mvcc.LockVer)}
	iter := db.NewIter(&opt)
	lock := mvcc.LockDecoder{ExpectKey: key}
	defer iter.Close()

	iter.First()
	foundLock, err := lock.Decode(iter)
	if err != nil {
		return err
	}
	if !foundLock || lock.Lock.StartVer != startVer {
		// If the lock of this transaction is not found, or the lock is replaced by
		// another transaction, check commit information of this transaction.
		c, ok, err1 := getTxnCommitInfo(iter, key, startVer)
		if err1 != nil {
			return err
		}

		// c.Type != mvcc.ValueTypeRollback means the transaction is already committed, do nothing.
		if ok && c.Type != mvcc.ValueTypeRollback {
			return nil
		}
		return ErrRetryable("txn not found")
	}

	// Delete lock and construct the value entry
	var valueType mvcc.ValueType
	switch lock.Lock.Op {
	case mvcc.Op_Put:
		valueType = mvcc.ValueTypePut
	case mvcc.Op_Lock:
		valueType = mvcc.ValueTypeLock
	default:
		valueType = mvcc.ValueTypeDelete
	}

	value := mvcc.Value{
		Type:      valueType,
		StartVer:  startVer,
		CommitVer: commitVer,
		Value:     lock.Lock.Value,
	}
	writeKey := mvcc.Encode(key, commitVer)
	writeValue, err := value.MarshalBinary()
	if err != nil {
		return err
	}
	err = batch.Set(writeKey, writeValue, nil)
	if err != nil {
		return err
	}
	err = batch.Delete(mvcc.Encode(key, mvcc.LockVer), nil)
	if err != nil {
		return err
	}

	return nil
}

func getTxnCommitInfo(iter *pebble.Iterator, expectKey []byte, startVer mvcc.Version) (mvcc.Value, bool, error) {
	for iter.Valid() {
		dec := mvcc.ValueDecoder{
			ExpectKey: expectKey,
		}
		ok, err := dec.Decode(iter)
		if err != nil || !ok {
			return mvcc.Value{}, ok, err
		}

		if dec.Value.StartVer == startVer {
			return dec.Value, true, nil
		}
	}
	return mvcc.Value{}, false, nil
}
