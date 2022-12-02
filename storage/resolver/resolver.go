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
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/vescale/zgraph/internal/logutil"
)

type resolver struct {
	db     *pebble.DB
	ch     chan Task
	closed atomic.Bool
}

func newResolver(db *pebble.DB) *resolver {
	return &resolver{
		db: db,
		ch: make(chan Task, 512),
	}
}

func (r *resolver) run(ctx context.Context) {
	for {
		select {
		case task, ok := <-r.ch:
			if !ok {
				return
			}
			c := len(r.ch)
			tasks := make([]Task, 0, c+1)
			tasks = append(tasks, task)
			if c > 0 {
				for i := 0; i < c; i++ {
					tasks = append(tasks, <-r.ch)
				}
			}
			r.resolve(tasks)

		case <-ctx.Done():
			return
		}
	}
}

func (r *resolver) resolve(tasks []Task) {
	batch := r.db.NewBatch()
	for _, task := range tasks {
		var err error
		if task.CommitVer > 0 {
			err = Resolve(r.db, batch, task.Key, task.StartVer, task.CommitVer)
		} else {
			err = Rollback(r.db, batch, task.Key, task.StartVer)
		}
		if err != nil {
			logutil.Errorf("Resolve key failed, key:%v, startVer:%d, commitVer:%d, caused by:%+v",
				task.Key, task.StartVer, task.CommitVer, err)
		}
		if task.Notifier != nil {
			task.Notifier.Notify(err)
		}
	}
	err := batch.Commit(nil)
	if err != nil {
		// TODO: handle error
	}
}

func (r *resolver) push(tasks ...Task) {
	for _, t := range tasks {
		r.ch <- t
	}
}

func (r *resolver) close() {
	if r.closed.Swap(true) {
		return
	}
	close(r.ch)
}
