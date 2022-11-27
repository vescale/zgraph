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

	"github.com/vescale/zgraph/storage/mvcc"
)

type Storage interface {
	mvcc.VersionProvider

	Open(dirname string, options ...Option) error
	Begin() (Transaction, error)
	Snapshot(ver mvcc.Version) (Snapshot, error)
	Close() error
}

// Iterator is the interface for a iterator on KV db.
type Iterator interface {
	Valid() bool
	Key() Key
	Value() []byte
	Next() error
	Close()
}

// Getter is the interface for the Get method.
type Getter interface {
	// Get gets the value for key k from kv db.
	// If corresponding kv pair does not exist, it returns nil and ErrNotExist.
	Get(ctx context.Context, k Key) ([]byte, error)
}

// BatchGetter is the interface for BatchGet.
type BatchGetter interface {
	// BatchGet gets a batch of values.
	BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error)
}

// Retriever is the interface wraps the basic Get and Seek methods.
type Retriever interface {
	Getter
	// Iter creates an Iterator positioned on the first entry that k <= entry's key.
	// If such entry is not found, it returns an invalid Iterator with no error.
	// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
	// The Iterator must be Closed after use.
	Iter(lowerBound Key, upperBound Key) (Iterator, error)

	// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
	// The returned iterator will iterate from greater key to smaller key.
	// If k is nil, the returned iterator will be positioned at the last key.
	// TODO: Add lower bound limit
	IterReverse(lowerBound Key, upperBound Key) (Iterator, error)
}

// Mutator is the interface wraps the basic Set and Delete methods.
type Mutator interface {
	// Set sets the value for key k as v into kv db.
	// v must NOT be nil or empty, otherwise it returns ErrCannotSetNilValue.
	Set(k Key, v []byte) error
	// Delete removes the entry for key k from kv db.
	Delete(k Key) error
}

type Transaction interface {
	Retriever
	Mutator

	// Snapshot returns the Snapshot binding to this transaction.
	Snapshot() Snapshot
	// BatchGet gets kv from the memory buffer of statement and transaction, and the kv storage.
	// Do not use len(value) == 0 or value == nil to represent non-exist.
	// If a key doesn't exist, there shouldn't be any corresponding entry in the result map.
	BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error)
	// Size returns sum of keys and values length.
	Size() int
	// Len returns the number of entries in the DB.
	Len() int
	// Reset reset the Transaction to initial states.
	Reset()
	// Commit commits the transaction operations to KV db.
	Commit(context.Context) error
	// Rollback undoes the transaction operations to KV db.
	Rollback() error
	// String implements fmt.Stringer interface.
	String() string
}

// Snapshot defines the interface for the snapshot fetched from KV db.
type Snapshot interface {
	Retriever
	BatchGetter
}
