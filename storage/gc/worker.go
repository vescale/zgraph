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
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/vescale/zgraph/storage/resolver"
)

// worker represents a GC worker which is used to clean staled versions.
type worker struct {
	db       *pebble.DB
	resolver *resolver.Scheduler
	closed   atomic.Bool
}

func newWorker(db *pebble.DB, resolver *resolver.Scheduler) *worker {
	return &worker{
		db:       db,
		resolver: resolver,
	}
}

func (w *worker) run(ctx context.Context, queue <-chan Task) {
	for {
		select {
		case task, ok := <-queue:
			if !ok {
				return
			}
			w.execute(task)

		case <-ctx.Done():
			return
		}
	}
}

func (w *worker) execute(task Task) {

}
