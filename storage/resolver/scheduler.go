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
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/vescale/zgraph/storage/kv"
)

// Scheduler is used to schedule ResolveKey tasks.
type Scheduler struct {
	mu        sync.Mutex
	db        *pebble.DB
	size      int
	resolvers []*resolver
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
	for i := 0; i < s.size; i++ {
		r := newResolver(s.db)
		go r.run()
		s.resolvers = append(s.resolvers, r)
	}
}

// SubmitKey submits a key to resolve
func (s *Scheduler) SubmitKey(key kv.Key) {

}

// SubmitKeys submits a bundle of keys to resolve
func (s *Scheduler) SubmitKeys(keys []kv.Key) {

}
