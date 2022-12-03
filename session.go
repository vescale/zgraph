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
	"sync/atomic"

	"github.com/vescale/zgraph/stmtctx"
)

var sessionIDGenerator atomic.Int64

// Session represents the session to interact with zGraph database instance.
// Typically, the number of session will be same as the concurrent thread
// count of the application.
// All execution intermediate variables should be placed in the Context.
type Session struct {
	// Protect the current session will not be used concurrently.
	mu       sync.Mutex
	id       int64
	db       *DB
	sc       *stmtctx.Context
	wg       sync.WaitGroup
	closed   atomic.Bool
	cancelFn context.CancelFunc

	// Callback function while session closing.
	closeCallback func(s *Session)
}

// newSession returns a new session instance.
func newSession(db *DB) *Session {
	return &Session{
		id: sessionIDGenerator.Add(1),
		db: db,
		sc: stmtctx.New(),
	}
}

// ID returns a integer identifier of the current session.
func (s *Session) ID() int64 {
	return s.id
}

// Execute executes a query.
func (s *Session) Execute(ctx context.Context, query string) (ResultSet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, cancelFn := context.WithCancel(ctx)
	s.cancelFn = cancelFn
	s.wg.Add(1)
	defer s.wg.Done()

	return s.execute(ctx, query)
}

func (s *Session) execute(ctx context.Context, query string) (ResultSet, error) {
	return nil, nil
}

// Close terminates the current session.
func (s *Session) Close() {
	if s.closed.Swap(true) {
		return
	}
	s.cancelFn()
	s.wg.Wait()
}

func (s *Session) setCloseCallback(cb func(session *Session)) {
	s.mu.Lock()
	s.mu.Unlock()
	s.closeCallback = cb
}
