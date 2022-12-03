// Copyright 2022 zGraph Authors. All rights reserved.
//
// Copyright 2015 PingCAP, Inc.
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

package kv

import (
	"context"

	"github.com/pingcap/errors"
)

const retry = 5

// RunNewTxnContext creates a new transaction and call the user-define transaction callback.
// The transaction will be committed automatically.
func RunNewTxnContext(ctx context.Context, store Storage, fn func(ctx context.Context, txn Transaction) error) error {
	txn, err := store.Begin()
	if err != nil {
		return err
	}

	for i := 0; i < retry; i++ {
		err := fn(ctx, txn)
		if err != nil {
			if IsRetryable(err) {
				continue
			}
			return err
		}
		err = txn.Commit(ctx)
		if err == nil {
			return nil
		}
		if !IsRetryable(err) {
			return err
		}
	}

	return nil
}

// RunNewTxn creates a new transaction and call the user-define transaction callback.
func RunNewTxn(store Storage, fn func(txn Transaction) error) error {
	return RunNewTxnContext(context.Background(), store, func(ctx context.Context, txn Transaction) error {
		return fn(txn)
	})
}

// IsRetryable reports whether an error retryable.
func IsRetryable(err error) bool {
	err = errors.Cause(err)
	if err == nil {
		return false
	}
	return err == ErrTxnConflicts
}
