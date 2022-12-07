package stmtctx

import "github.com/vescale/zgraph/storage/kv"

type (
	txnStatus byte

	LazyTxn struct {
		sc         *Context
		status     txnStatus
		autocommit bool
		txn        kv.Transaction
	}
)

const (
	txnStatusPending txnStatus = iota
)

// Txn returns the transaction object.
func (sc *Context) Txn() *LazyTxn {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.mu.txn == nil {
		sc.mu.txn = &LazyTxn{
			sc: sc,
		}
	}
	return sc.mu.txn
}

func (txn *LazyTxn) validOrPending() bool {
	return false
}
