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
	"math"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/cockroachdb/pebble"
	"github.com/vescale/zgraph/storage/kv"
	"github.com/vescale/zgraph/storage/latch"
	"github.com/vescale/zgraph/storage/mvcc"
	"github.com/vescale/zgraph/storage/resolver"
	"go.uber.org/atomic"
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

// committer represents the transaction 2 phase committer. It will calculate the
// mutations and apply to the low-level storage.
type committer struct {
	db         *pebble.DB
	memDB      *MemDB
	resolver   *resolver.Scheduler
	startVer   mvcc.Version
	commitVer  mvcc.Version
	resolved   []mvcc.Version
	primaryIdx int
	primaryKey kv.Key
	lockTTL    uint64
	handles    []MemKeyHandle

	// counter of mutations
	size, putCnt, delCnt, lockCnt, checkCnt int

	// The commit status
	mu struct {
		sync.RWMutex
		undeterminedErr error // undeterminedErr saves the rpc error we encounter when commit primary key.
		committed       bool
	}
}

// init initializes the keys and mutations.
func (c *committer) init(startTime time.Time) error {
	// Foreach all the changes cached in the memory buffer and build the mutations.
	var err error
	for it := c.memDB.IterWithFlags(nil, nil); it.Valid(); err = it.Next() {
		// TODO: handle error properly
		_ = err

		var (
			key   = it.Key()
			flags = it.Flags()
			value []byte
			op    mvcc.Op
		)

		if !it.HasValue() {
			if !flags.HasLocked() {
				continue
			}
			op = mvcc.Op_Lock
			c.lockCnt++
		} else {
			value = it.Value()
			if len(value) > 0 {
				op = mvcc.Op_Put
				if flags.HasPresumeKeyNotExists() {
					op = mvcc.Op_Insert
				}
				c.putCnt++
			} else if flags.HasPresumeKeyNotExists() {
				// delete-your-writes keys in optimistic txn need check not exists in prewrite-phase
				// due to `Op_CheckNotExists` doesn't prewrite lock, so mark those keys should not be used in commit-phase.
				op = mvcc.Op_CheckNotExists
				c.checkCnt++
				c.memDB.UpdateFlags(key, kv.SetPrewriteOnly)
			} else if flags.HasNewlyInserted() {
				// The delete-your-write keys in pessimistic transactions, only lock needed keys and skip
				// other deletes for example the secondary index delete.
				// Here if `tidb_constraint_check_in_place` is enabled and the transaction is in optimistic mode,
				// the logic is same as the pessimistic mode.
				if flags.HasLocked() {
					op = mvcc.Op_Lock
					c.lockCnt++
				} else {
					continue
				}
			} else {
				op = mvcc.Op_Del
				c.delCnt++
			}

			handle := it.Handle()
			handle.op = op
			handle.flags = flags
			c.handles = append(c.handles, handle)
			c.size += len(key) + len(value)
		}

		// Choose the first valid key as the primary key of the current transaction.
		if len(c.primaryKey) == 0 && op != mvcc.Op_CheckNotExists {
			c.primaryIdx = len(c.handles) - 1
			c.primaryKey = key
		}
	}

	if len(c.handles) == 0 {
		return nil
	}
	c.lockTTL = txnLockTTL(startTime, c.size)

	return nil
}

func (c *committer) length() int {
	return len(c.handles)
}

// keys returns keys of all mutations in the current transaction.
func (c *committer) keys() []kv.Key {
	keys := make([]kv.Key, len(c.handles))
	for i, h := range c.handles {
		keys[i] = c.memDB.GetKeyByHandle(h)
	}
	return keys
}

// prepare implements the first stage of 2PC transaction model.
func (c *committer) prepare() error {
	var (
		errs       []error
		batch      = c.db.NewBatch()
		primaryKey = c.primaryKey
		startVer   = c.startVer
		resolved   = c.resolved
	)
	defer batch.Close()

	for _, h := range c.handles {
		op := h.op
		key := c.memDB.GetKeyByHandle(h)
		enc := mvcc.Encode(key, mvcc.LockVer)
		opt := pebble.IterOptions{
			LowerBound: enc,
		}
		if op == mvcc.Op_Insert || op == mvcc.Op_CheckNotExists {
			iter := c.db.NewIter(&opt)
			iter.First()
			val, err := getValue(iter, key, startVer, resolved)
			_ = iter.Close()
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if val != nil {
				err = &ErrKeyAlreadyExist{
					Key: key,
				}
				errs = append(errs, err)
				continue
			}
		}
		if op == mvcc.Op_CheckNotExists {
			continue
		}

		err := func() error {
			iter := c.db.NewIter(&opt)
			iter.First()
			defer iter.Close()

			decoder := mvcc.LockDecoder{ExpectKey: key}
			exists, err := decoder.Decode(iter)
			if err != nil {
				return err
			}

			// There is a lock exists.
			if exists && decoder.Lock.StartVer != startVer {
				return decoder.Lock.LockErr(key)
			}

			// Check conflicts
			vdecoder := mvcc.ValueDecoder{ExpectKey: key}
			exists, err = vdecoder.Decode(iter)
			if err != nil {
				return err
			}
			if exists && vdecoder.Value.CommitVer > startVer {
				return &ErrConflict{
					StartVer:          startVer,
					ConflictStartVer:  vdecoder.Value.StartVer,
					ConflictCommitVer: vdecoder.Value.CommitVer,
					Key:               key,
				}
			}
			return nil
		}()
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// Append the current row key into the write batch.
		if op == mvcc.Op_Insert {
			op = mvcc.Op_Put
		}
		val, _ := c.memDB.GetValueByHandle(h)
		l := mvcc.Lock{
			StartVer: startVer,
			Primary:  primaryKey,
			Value:    val,
			Op:       op,
			TTL:      c.lockTTL,
		}
		writeVal, err := l.MarshalBinary()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		err = batch.Set(enc, writeVal, nil)
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}

	// Commit the current write batch into the low-level storage engine.
	if err := batch.Commit(nil); err != nil {
		return err
	}

	return &ErrGroup{Errors: errs}
}

// commit implements the second stage of 2PC transaction model.
func (c *committer) commit() error {
	batch := c.db.NewBatch()
	defer batch.Close()

	// Commit primary key first.
	err := resolver.Resolve(c.db, batch, c.primaryKey, c.startVer, c.commitVer)
	if err != nil {
		return err
	}
	err = batch.Commit(nil)
	if err != nil {
		return err
	}

	// The remained keys submit to resolver to resolve them asynchronously.
	var remainedKeys []kv.Key
	for i, h := range c.handles {
		// The primary key had been committed.
		if i == c.primaryIdx {
			continue
		}
		if h.op == mvcc.Op_CheckNotExists {
			continue
		}

		// Note: the keys stored in MemDB are reference to MemDB and its lifetime
		// bound to the MemDB. We will release MemDB instance after the transaction
		// committed. So we need to copy the keys, then submit them to the resolver.
		key := c.memDB.GetKeyByHandle(h)
		cpy := make(kv.Key, len(key))
		copy(cpy, key)
		remainedKeys = append(remainedKeys, cpy)
	}
	c.resolver.Resolve(remainedKeys, c.startVer, c.commitVer, nil)

	return nil
}

const bytesPerMiB = 1024 * 1024

// ttl = ttlFactor * sqrt(writeSizeInMiB)
var ttlFactor = 6000

// By default, locks after 3000ms is considered unusual (the client created the
// lock might be dead). Other client may clean up this kind of lock.
// For locks created recently, we will do backoff and retry.
var defaultLockTTL uint64 = 3000

// Global variable set by config file.
var (
	ManagedLockTTL uint64 = 20000 // 20s
)

var (
	// PrewriteMaxBackoff is max sleep time of the `pre-write` command.
	PrewriteMaxBackoff = atomic.NewUint64(40000)
	// CommitMaxBackoff is max sleep time of the 'commit' command
	CommitMaxBackoff = uint64(40000)
)

func txnLockTTL(startTime time.Time, txnSize int) uint64 {
	// Increase lockTTL for large transactions.
	// The formula is `ttl = ttlFactor * sqrt(sizeInMiB)`.
	// When writeSize is less than 256KB, the base ttl is defaultTTL (3s);
	// When writeSize is 1MiB, 4MiB, or 10MiB, ttl is 6s, 12s, 20s correspondingly;
	lockTTL := defaultLockTTL
	if txnSize >= int(kv.TxnCommitBatchSize.Load()) {
		sizeMiB := float64(txnSize) / bytesPerMiB
		lockTTL = uint64(float64(ttlFactor) * math.Sqrt(sizeMiB))
		if lockTTL < defaultLockTTL {
			lockTTL = defaultLockTTL
		}
		if lockTTL > ManagedLockTTL {
			lockTTL = ManagedLockTTL
		}
	}

	// Increase lockTTL by the transaction's read time.
	// When resolving a lock, we compare current ver and startVer+lockTTL to decide whether to clean up. If a txn
	// takes a long time to read, increasing its TTL will help to prevent it from been aborted soon after prewrite.
	elapsed := time.Since(startTime) / time.Millisecond
	return lockTTL + uint64(elapsed)
}
