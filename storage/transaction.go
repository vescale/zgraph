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

	"github.com/cenkalti/backoff"
	"github.com/cockroachdb/pebble"
	"github.com/vescale/zgraph/storage/kv"
	"github.com/vescale/zgraph/storage/latch"
	"github.com/vescale/zgraph/storage/mvcc"
	"github.com/vescale/zgraph/storage/resolver"
)

// Txn represents a transaction implemented beyond the low-level key/value storage.
type Txn struct {
	mu        sync.Mutex
	vp        mvcc.VersionProvider
	db        *pebble.DB
	us        *UnionStore
	latches   *latch.LatchesScheduler
	resolver  *resolver.Scheduler
	valid     bool
	snapshot  Snapshot
	startTime time.Time
	startVer  mvcc.Version
	commitVer mvcc.Version
	setCnt    int64
	lockedCnt int
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

func (txn *Txn) Commit(_ context.Context) error {
	if !txn.valid {
		return ErrInvalidTxn
	}
	defer txn.close()

	// Sanity check for start timestamp of the current transaction.
	if txn.startVer == mvcc.LockVer {
		return ErrInvalidStartVer
	}

	committer := &committer{
		db:       txn.db,
		memDB:    txn.us.MemBuffer(),
		resolver: txn.resolver,
		startVer: txn.startVer,
	}
	err := committer.init(txn.startTime)
	if err != nil {
		return err
	}
	if committer.length() == 0 {
		return nil
	}
	keys := committer.keys()

	err = backoff.RetryNotify(func() error {
		// Note: don't use `defer txn.latches.UnLock(lock)` here. we need to keep the
		// lock fine-grain.
		// Because the subsequent routine may time-consumed:
		//   - CheckTxnStatus: will be slow if the IO usage is high.
		//   - Resolve: will block if the worker queue full.
		lock := txn.latches.Lock(txn.startVer, keys)
		err := committer.prepare()
		errg, ok := err.(*ErrGroup)
		if !ok {
			txn.latches.UnLock(lock)
			return err
		}
		// Prepare transaction successfully means all lock are written into the low-level
		// storage.
		if len(errg.Errors) == 0 {
			commitVer := txn.vp.CurrentVersion()
			txn.commitVer = commitVer
			committer.commitVer = commitVer
			lock.SetCommitVer(commitVer)
			txn.latches.UnLock(lock)
			return nil
		}
		txn.latches.UnLock(lock)

		rollbacks := map[mvcc.Version][]kv.Key{}
		committed := map[mvcc.VersionPair][]kv.Key{}
		for _, err := range errg.Errors {
			// Try to resolve keys locked error.
			lockedErr, ok := err.(*mvcc.LockedError)
			if !ok {
				return &backoff.PermanentError{Err: err}
			}

			status, err := resolver.CheckTxnStatus(txn.db, txn.vp, lockedErr.Primary, lockedErr.StartVer)
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
				rollbacks[lockedErr.StartVer] = append(rollbacks[lockedErr.StartVer], lockedErr.Key)
				continue

			default:
				// TxnActionLockNotExistDoNothing
				// Transaction committed: we try to resolve the current key and backoff.
				pair := mvcc.VersionPair{StartVer: lockedErr.StartVer, CommitVer: status.CommitVer}
				committed[pair] = append(committed[pair], lockedErr.Key)
				continue
			}
		}

		if len(rollbacks) > 0 {
			for startVer, keys := range rollbacks {
				txn.resolver.Resolve(keys, startVer, 0, nil)
				committer.resolved = append(committer.resolved, startVer)
			}
		}
		if len(committed) > 0 {
			for pair, keys := range committed {
				txn.resolver.Resolve(keys, pair.StartVer, pair.CommitVer, nil)
			}
		}

		return resolver.ErrRetryable("resolving locks in transaction prepare staging")
	}, expoBackoff(), BackoffErrReporter("committer.execute"))
	if err != nil {
		return err
	}

	return committer.commit()
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
	return fmt.Sprintf("%d", txn.startVer)
}

func (txn *Txn) close() {
	txn.valid = false
}
