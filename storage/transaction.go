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

type transaction struct {
	version    mvcc.Version
	memManager MemManager
	snapshot   Snapshot
}

// Get implements the Transaction interface.
func (t *transaction) Get(ctx context.Context, k Key) ([]byte, error) {
	return t.memManager.UnionGet(ctx, t.snapshot, k)
}

func (t *transaction) Iter(lowerBound Key, upperBound Key) (Iterator, error) {
	//TODO implement me
	panic("implement me")
}

func (t *transaction) IterReverse(lowerBound Key, upperBound Key) (Iterator, error) {
	//TODO implement me
	panic("implement me")
}

func (t *transaction) Set(k Key, v []byte) error {
	//TODO implement me
	panic("implement me")
}

func (t *transaction) Delete(k Key) error {
	//TODO implement me
	panic("implement me")
}

// Snapshot implements the Transaction interface.
func (t *transaction) Snapshot() Snapshot {
	return t.snapshot
}

// BatchGet implements the Transaction interface.
func (t *transaction) BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error) {
	return t.memManager.UnionBatchGet(ctx, t.snapshot, keys)
}

func (t *transaction) Size() int {
	//TODO implement me
	panic("implement me")
}

func (t *transaction) Len() int {
	//TODO implement me
	panic("implement me")
}

func (t *transaction) Reset() {
	//TODO implement me
	panic("implement me")
}

func (t *transaction) Commit(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (t *transaction) Rollback() error {
	//TODO implement me
	panic("implement me")
}

func (t *transaction) String() string {
	//TODO implement me
	panic("implement me")
}
