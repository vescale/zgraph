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

package session

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/parser"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/storage/kv"
)

var (
	idGenerator atomic.Int64
	parserPool  = &sync.Pool{New: func() interface{} { return parser.New() }}
)

// Session represents the session to interact with zGraph database instance.
// Typically, the number of session will be same as the concurrent thread
// count of the application.
// All execution intermediate variables should be placed in the Context.
type Session struct {
	// Protect the current session will not be used concurrently.
	mu       sync.Mutex
	id       int64
	sc       *stmtctx.Context
	wg       sync.WaitGroup
	store    kv.Storage
	catalog  *catalog.Catalog
	closed   atomic.Bool
	cancelFn context.CancelFunc

	// Callback function while session closing.
	closeCallback func(s *Session)
}

// New returns a new session instance.
func New(store kv.Storage, catalog *catalog.Catalog) *Session {
	return &Session{
		id:      idGenerator.Add(1),
		sc:      stmtctx.New(),
		store:   store,
		catalog: catalog,
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

	p := parserPool.Get().(*parser.Parser)
	defer parserPool.Put(p)

	stmts, warns, err := p.Parse(query)
	if err != nil {
		return nil, err
	}
	for _, warn := range warns {
		s.sc.AppendWarning(errors.Annotate(warn, "parse warning"))
	}
	if len(stmts) == 0 {
		return emptyResultSet{}, nil
	}
	if len(stmts) > 1 {
		return nil, ErrMultipleStatementsNotSuported
	}

	return s.executeStmt(ctx, stmts[0])
}

func (s *Session) executeStmt(ctx context.Context, stmt ast.StmtNode) (ResultSet, error) {
	return nil, nil
}

// Close terminates the current session.
func (s *Session) Close() {
	if s.closed.Swap(true) {
		return
	}

	// Wait the current execution finished.
	if s.cancelFn != nil {
		s.cancelFn()
	}
	s.wg.Wait()

	if s.closeCallback != nil {
		s.closeCallback(s)
	}
}

// OnClosed sets the closed callback which will invoke after session closed.
func (s *Session) OnClosed(cb func(session *Session)) {
	s.mu.Lock()
	s.mu.Unlock()
	s.closeCallback = cb
}
