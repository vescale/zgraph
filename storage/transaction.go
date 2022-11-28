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
	"fmt"
	"sync"
	"time"

	"github.com/vescale/zgraph/storage/kv"
	"github.com/vescale/zgraph/storage/latch"
	"github.com/vescale/zgraph/storage/mvcc"
)

// Txn represents a transaction implemented beyond the low-level key/value storage.
type Txn struct {
	mu        sync.Mutex // For thread-safe LockKeys function.
	valid     bool
	snapshot  Snapshot
	us        *UnionStore
	startTime time.Time
	startTS   mvcc.Version
	commitTS  mvcc.Version
	setCnt    int64
	lockedCnt int
	latches   *latch.LatchesScheduler
}

// Get implements the Transaction interface.
func (txn *Txn) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	return txn.us.Get(ctx, k)
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (txn *Txn) Iter(lowerBound, upperBound kv.Key) (Iterator, error) {
	return txn.us.Iter(lowerBound, upperBound)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (txn *Txn) IterReverse(lowerBound, upperBound kv.Key) (Iterator, error) {
	return txn.us.IterReverse(lowerBound, upperBound)
}

// Set implements the Transaction interface.
// It sets the value for key k as v into kv store.
// v must NOT be nil or empty, otherwise it returns ErrCannotSetNilValue.
func (txn *Txn) Set(k kv.Key, v []byte) error {
	txn.setCnt++
	return txn.us.MemBuffer().Set(k, v)
}

// Delete implements the Transaction interface. It removes the entry for key k from kv store.
func (txn *Txn) Delete(k kv.Key) error {
	return txn.us.MemBuffer().Delete(k)
}

// Snapshot implements the Transaction interface.
func (txn *Txn) Snapshot() Snapshot {
	return txn.snapshot
}

// BatchGet implements the Transaction interface.
// It gets kv from the memory buffer of statement and transaction, and the kv storage.
// Do not use len(value) == 0 or value == nil to represent non-exist.
// If a key doesn't exist, there shouldn't be any corresponding entry in the result map.
func (txn *Txn) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	return NewBufferBatchGetter(txn.us.MemBuffer(), txn.Snapshot()).BatchGet(ctx, keys)
}

// Size implements the Transaction interface. It returns sum of keys and values length.
func (txn *Txn) Size() int {
	return txn.us.MemBuffer().Size()
}

// Len implements the Transaction interface. It returns the number of entries in the DB.
func (txn *Txn) Len() int {
	return txn.us.MemBuffer().Len()
}

// Reset implements the Transaction interface. It resets the Transaction to initial states.
func (txn *Txn) Reset() {
	txn.us.MemBuffer().Reset()
}

func (txn *Txn) Commit(ctx context.Context) error {
	if !txn.valid {
		return ErrInvalidTxn
	}
	defer txn.close()

	// Sanity check for start timestamp of the current transaction.
	if txn.startTS == mvcc.LockVer {
		return ErrInvalidStartTS
	}

	committer := &committer{memDB: txn.us.MemBuffer()}
	err := committer.init(txn.startTime)
	if err != nil {
		return err
	}
	if committer.length() == 0 {
		return nil
	}
	lock := txn.latches.Lock(txn.startTS, committer.keys())
	defer txn.latches.UnLock(lock)

	err = committer.execute()
	if err == nil {
		lock.SetCommitTS(committer.commitTS)
	}
	return err
}

// Rollback implements the Transaction interface. It undoes the transaction operations to KV store.
func (txn *Txn) Rollback() error {
	if !txn.valid {
		return ErrInvalidTxn
	}
	txn.close()
	return nil
}

// String implements fmt.Stringer interface.
func (txn *Txn) String() string {
	return fmt.Sprintf("%d", txn.startTS)
}

func (txn *Txn) close() {
	txn.valid = false
}
