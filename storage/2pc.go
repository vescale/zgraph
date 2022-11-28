package storage

import (
	"math"
	"sync"
	"time"

	"github.com/vescale/zgraph/storage/kv"
	"github.com/vescale/zgraph/storage/mvcc"
	"go.uber.org/atomic"
)

// committer represents the transaction 2 phase committer. It will calculate the
// mutations and apply to the low-level storage.
type committer struct {
	memDB      *MemDB
	primaryKey kv.Key
	lockTTL    uint64
	commitTS   mvcc.Version

	// The format to put to the UserData of the handles:
	// MSB									                                                                  LSB
	// [12 bits: Op]
	// [1 bit: NeedConstraintCheckInPrewrite][1 bit: assertNotExist][1 bit: assertExist][1 bit: isPessimisticLock]
	handles []MemKeyHandle

	// counter of mutations
	size, putCnt, delCnt, lockCnt, checkCnt int

	// The commit status
	mu struct {
		sync.RWMutex
		undeterminedErr error // undeterminedErr saves the rpc error we encounter when commit primary key.
		committed       bool
	}
}

// Prepare implements the first stage of 2PC transaction model.
func (c *committer) Prepare() error {
	return nil
}

// Commit implements the second stage of 2PC transaction model.
func (*committer) Commit() error {
	return nil
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

			// Set the mutation flags.
			aux := uint16(op) << 4
			if flags.HasAssertExist() {
				aux |= 1 << 1
			}
			if flags.HasAssertNotExist() {
				aux |= 1 << 2
			}
			if flags.HasNeedConstraintCheckInPrewrite() {
				aux |= 1 << 3
			}

			handle := it.Handle()
			handle.UserData = aux
			c.handles = append(c.handles, handle)
			c.size += len(key) + len(value)
		}

		// Choose the first valid key as the primary key of the current transaction.
		if len(c.primaryKey) == 0 && op != mvcc.Op_CheckNotExists {
			c.primaryKey = key
		}
	}

	if len(c.handles) == 0 {
		return nil
	}
	c.lockTTL = txnLockTTL(startTime, c.size)

	return nil
}

func (c *committer) primary() kv.Key {
	if len(c.primaryKey) > 0 {
		return c.primaryKey
	}
	return c.memDB.GetKeyByHandle(c.handles[0])
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

// execute commits the mutations to the low-level key/value storage engine.
func (c *committer) execute() error {
	panic("implement me!")
}

const bytesPerMiB = 1024 * 1024

// ttl = ttlFactor * sqrt(writeSizeInMiB)
var ttlFactor = 6000

// By default, locks after 3000ms is considered unusual (the client created the
// lock might be dead). Other client may cleanup this kind of lock.
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
	// When resolving a lock, we compare current ts and startTS+lockTTL to decide whether to clean up. If a txn
	// takes a long time to read, increasing its TTL will help to prevent it from been aborted soon after prewrite.
	elapsed := time.Since(startTime) / time.Millisecond
	return lockTTL + uint64(elapsed)
}
