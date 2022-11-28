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

	"github.com/vescale/zgraph/storage/kv"
	"github.com/vescale/zgraph/storage/mvcc"
)

type transaction struct {
	mu        sync.Mutex // For thread-safe LockKeys function.
	valid     bool
	snapshot  Snapshot
	us        *UnionStore
	startTS   mvcc.Version
	commitTS  mvcc.Version
	setCnt    int64
	lockedCnt int
	committer *committer
}

// Get implements the Transaction interface.
func (txn *transaction) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	return txn.us.Get(ctx, k)
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (txn *transaction) Iter(lowerBound, upperBound kv.Key) (Iterator, error) {
	return txn.us.Iter(lowerBound, upperBound)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (txn *transaction) IterReverse(lowerBound, upperBound kv.Key) (Iterator, error) {
	return txn.us.IterReverse(lowerBound, upperBound)
}

// Set implements the Transaction interface.
// It sets the value for key k as v into kv store.
// v must NOT be nil or empty, otherwise it returns ErrCannotSetNilValue.
func (txn *transaction) Set(k kv.Key, v []byte) error {
	txn.setCnt++
	return txn.us.MemBuffer().Set(k, v)
}

// Delete implements the Transaction interface. It removes the entry for key k from kv store.
func (txn *transaction) Delete(k kv.Key) error {
	return txn.us.MemBuffer().Delete(k)
}

// Snapshot implements the Transaction interface.
func (txn *transaction) Snapshot() Snapshot {
	return txn.snapshot
}

// BatchGet implements the Transaction interface.
// It gets kv from the memory buffer of statement and transaction, and the kv storage.
// Do not use len(value) == 0 or value == nil to represent non-exist.
// If a key doesn't exist, there shouldn't be any corresponding entry in the result map.
func (txn *transaction) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	return NewBufferBatchGetter(txn.us.MemBuffer(), txn.Snapshot()).BatchGet(ctx, keys)
}

// Size implements the Transaction interface. It returns sum of keys and values length.
func (txn *transaction) Size() int {
	return txn.us.MemBuffer().Size()
}

// Len implements the Transaction interface. It returns the number of entries in the DB.
func (txn *transaction) Len() int {
	return txn.us.MemBuffer().Len()
}

// Reset implements the Transaction interface. It resets the Transaction to initial states.
func (txn *transaction) Reset() {
	txn.us.MemBuffer().Reset()
}

func (txn *transaction) Commit(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

// Rollback implements the Transaction interface. It undoes the transaction operations to KV store.
func (txn *transaction) Rollback() error {
	if !txn.valid {
		return ErrInvalidTxn
	}
	txn.valid = false
	return nil
}

// String implements fmt.Stringer interface.
func (txn *transaction) String() string {
	return fmt.Sprintf("%d", txn.startTS)
}
