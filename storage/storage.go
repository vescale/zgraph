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
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/vescale/zgraph/storage/latch"
	"github.com/vescale/zgraph/storage/mvcc"
	"github.com/vescale/zgraph/storage/resolver"
)

type mvccStorage struct {
	db       *pebble.DB
	latches  *latch.LatchesScheduler
	resolver *resolver.Scheduler
}

// New returns a new storage instance.
func New() Storage {
	return &mvccStorage{
		latches:  latch.NewScheduler(8),
		resolver: resolver.NewScheduler(4),
	}
}

// Open implements the Storage interface.
func (s *mvccStorage) Open(dirname string, options ...Option) error {
	opt := &pebble.Options{}
	for _, op := range options {
		op(opt)
	}
	db, err := pebble.Open(dirname, opt)
	if err != nil {
		return err
	}
	s.db = db

	return nil
}

// Begin implements the Storage interface
func (s *mvccStorage) Begin() (Transaction, error) {
	curVer, err := s.CurrentVersion()
	if err != nil {
		return nil, err
	}
	snap, err := s.Snapshot(curVer)
	if err != nil {
		return nil, err
	}
	txn := &Txn{
		vp:        s,
		db:        s.db,
		us:        NewUnionStore(snap),
		resolver:  s.resolver,
		startTime: time.Now(),
		startVer:  curVer,
		snapshot:  snap,
		latches:   s.latches,
	}
	return txn, nil
}

// Snapshot implements the Storage interface.
func (s *mvccStorage) Snapshot(ver mvcc.Version) (Snapshot, error) {
	snap := &KVSnapshot{
		db:  s.db,
		ver: ver,
	}
	return snap, nil
}

// CurrentVersion implements the VersionProvider interface.
// Currently, we use the system time as our startVer, and we cannot tolerant
// the system time rewind.
func (s *mvccStorage) CurrentVersion() (mvcc.Version, error) {
	return mvcc.Version(time.Now().UnixNano()), nil
}

// Close implements the Storage interface.
func (s *mvccStorage) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}
