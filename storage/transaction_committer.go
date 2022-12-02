package storage

import (
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/vescale/zgraph/storage/kv"
	"github.com/vescale/zgraph/storage/mvcc"
	"github.com/vescale/zgraph/storage/resolver"
	"go.uber.org/atomic"
)

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
