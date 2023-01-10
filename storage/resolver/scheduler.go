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

package resolver

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/twmb/murmur3"
	"github.com/vescale/zgraph/storage/kv"
)

// Scheduler is used to schedule Resolve tasks.
type Scheduler struct {
	running   atomic.Bool
	mu        sync.Mutex
	db        *pebble.DB
	size      int
	resolvers []*resolver
	wg        https://github.com/sourcegraph/conc
	cancelFn  context.CancelFunc
}

func NewScheduler(size int) *Scheduler {
	s := &Scheduler{
		size: size,
	}
	return s
}

func (s *Scheduler) SetDB(db *pebble.DB) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db = db
}

// Run initializes the resolvers and start to accept resolve tasks.
func (s *Scheduler) Run() {
	if s.running.Swap(true) {
		return
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	for i := 0; i < s.size; i++ {
		r := newResolver(s.db)
		s.resolvers = append(s.resolvers, r)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			r.run(ctx)
		}()
	}
	s.cancelFn = cancelFn
}

// Resolve submits a bundle of keys to resolve
func (s *Scheduler) Resolve(keys []kv.Key, startVer, commitVer kv.Version, notifier Notifier) {
	if len(keys) == 0 {
		return
	}
	for _, key := range keys {
		idx := int(murmur3.Sum32(key)) % s.size
		s.resolvers[idx].push(Task{
			Key:       key,
			StartVer:  startVer,
			CommitVer: commitVer,
			Notifier:  notifier,
		})
	}
}

func (s *Scheduler) Close() {
	s.cancelFn()
	s.wg.Wait()
	for _, r := range s.resolvers {
		r.close()
	}
}
