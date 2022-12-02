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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/storage/kv"
)

var (
	// ErrTxnConflicts indicates the current transaction contains some vertex/edge/index
	// conflicts with others.
	ErrTxnConflicts = errors.New("transaction conflicts")

	// ErrNotExist means the related data not exist.
	ErrNotExist = errors.New("not exist")

	// ErrCannotSetNilValue is the error when sets an empty value.
	ErrCannotSetNilValue = errors.New("can not set nil value")

	// ErrInvalidTxn is the error when commits or rollbacks in an invalid transaction.
	ErrInvalidTxn = errors.New("invalid transaction")

	ErrInvalidStartVer = errors.New("invalid start timestamp for transaction")
)

// ErrEntryTooLarge is the error when a key value entry is too large.
type ErrEntryTooLarge struct {
	Limit uint64
	Size  uint64
}

func (e *ErrEntryTooLarge) Error() string {
	return fmt.Sprintf("entry size too large, size: %v,limit: %v.", e.Size, e.Limit)
}

// ErrTxnTooLarge is the error when transaction is too large, lock time reached the maximum value.
type ErrTxnTooLarge struct {
	Size int
}

func (e *ErrTxnTooLarge) Error() string {
	return fmt.Sprintf("txn too large, size: %v.", e.Size)
}

func IsErrNotFound(err error) bool {
	return errors.Cause(err) == ErrNotExist
}

// ErrKeyAlreadyExist is returned when key exists but this key has a constraint that
// it should not exist. Client should return duplicated entry error.
type ErrKeyAlreadyExist struct {
	Key []byte
}

func (e *ErrKeyAlreadyExist) Error() string {
	return fmt.Sprintf("key already exist, key: %q", e.Key)
}

// ErrGroup is used to collect multiple errors.
type ErrGroup struct {
	Errors []error
}

func (e *ErrGroup) Error() string {
	return fmt.Sprintf("Errors: %v", e.Errors)
}

// ErrConflict is returned when the commitTS of key in the DB is greater than startTS.
type ErrConflict struct {
	StartVer          kv.Version
	ConflictStartVer  kv.Version
	ConflictCommitVer kv.Version
	Key               kv.Key
}

func (e *ErrConflict) Error() string {
	return "write conflict"
}
