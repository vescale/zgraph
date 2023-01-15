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

package gc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/sourcegraph/conc"
	"github.com/vescale/zgraph/storage/resolver"
)

// Manager represents the GC manager which is used to scheduler GC tasks to GC worker.
type Manager struct {
	running  atomic.Bool
	size     int
	mu       sync.RWMutex
	db       *pebble.DB
	resolver *resolver.Scheduler
	workers  []*worker
	wg       conc.WaitGroup
	cancelFn context.CancelFunc
	pending  chan Task
}

func NewManager(size int) *Manager {
	return &Manager{
		size:    size,
		pending: make(chan Task, 32),
	}
}

func (m *Manager) SetDB(db *pebble.DB) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.db = db
}

func (m *Manager) SetResolver(resolver *resolver.Scheduler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resolver = resolver
}

func (m *Manager) Run() {
	if m.running.Swap(true) {
		return
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	for i := 0; i < m.size; i++ {
		worker := newWorker(m.db, m.resolver)
		m.workers = append(m.workers, worker)
		m.wg.Go(func() { worker.run(ctx, m.pending) })
	}
	m.cancelFn = cancelFn

	// Schedule tasks
	m.wg.Go(func() { m.scheduler(ctx) })
}

func (m *Manager) scheduler(ctx context.Context) {
	const interval = time.Second * 5
	timer := time.NewTimer(interval)
	for {
		select {
		case <-timer.C:
			if len(m.pending) < cap(m.pending) {
				// TODO: schedule some new tasks.
			}

		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) Close() {
	m.cancelFn()
	m.wg.Wait()
}
