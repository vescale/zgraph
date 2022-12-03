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

package zgraph

import (
	"context"
	"sync"

	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/storage"
	"github.com/vescale/zgraph/storage/kv"
)

// DB represents the zGraph database instance.
type DB struct {
	// All fields are not been protected by Mutex will be read-only.
	options *Options
	store   kv.Storage
	catalog *catalog.Catalog

	mu struct {
		sync.RWMutex
		sessions map[int64]*Session
	}
}

// Open opens a zGraph database instance with specified directory name.
func Open(dirname string, opt *Options) (*DB, error) {
	if opt == nil {
		opt = &Options{}
	}
	opt.SetDefaults()

	store, err := storage.Open(dirname)
	if err != nil {
		return nil, err
	}

	// Load the catalog from storage.
	snapshot, err := store.Snapshot(store.CurrentVersion())
	if err != nil {
		return nil, err
	}
	catalog, err := catalog.Load(snapshot)
	if err != nil {
		return nil, err
	}

	db := &DB{
		options: opt,
		store:   store,
		catalog: catalog,
	}
	db.mu.sessions = map[int64]*Session{}

	return db, nil
}

// NewSession returns a new session.
func (db *DB) NewSession() *Session {
	// TODO: concurrency limitation
	db.mu.Lock()
	defer db.mu.Unlock()

	s := newSession(db)
	db.mu.sessions[s.ID()] = s
	return s
}

// Execute executes a query and reports whether the query executed successfully or not.
func (db *DB) Execute(ctx context.Context, query string) (ResultSet, error) {
	return db.NewSession().Execute(ctx, query)
}

// Close destroys the zGraph database instances and all sessions will be terminated.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, s := range db.mu.sessions {
		s.setCloseCallback(db.onSessionClosedLocked)
		s.Close()
	}

	return nil
}

func (db *DB) onSessionClosed(s *Session) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.onSessionClosedLocked(s)
}

func (db *DB) onSessionClosedLocked(s *Session) {
	delete(db.mu.sessions, s.ID())
}
