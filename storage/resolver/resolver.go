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
	"github.com/cockroachdb/pebble"
)

type resolver struct {
	db *pebble.DB
	ch chan Task
}

func newResolver(db *pebble.DB) *resolver {
	return &resolver{
		db: db,
	}
}

func (r *resolver) run() {
	for task := range r.ch {
		c := len(r.ch)
		tasks := make([]Task, 0, c+1)
		tasks = append(tasks, task)
		if c > 0 {
			for i := 0; i < c; i++ {
				tasks = append(tasks, <-r.ch)
			}
		}
		r.resolve(tasks)
	}
}

func (r *resolver) resolve(tasks []Task) {
	batch := r.db.NewBatch()
	for _, task := range tasks {
		err := ResolveKey(r.db, batch, task.Key, task.StartVer, task.CommitVer)
		if err != nil {
			// TODO: handle error
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
