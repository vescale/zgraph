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

import "sync"

// Notifier is used to notify the task finished.
type Notifier interface {
	Notify(err error)
	Wait() []error
}

type MultiKeysNotifier struct {
	wg   sync.WaitGroup
	mu   sync.RWMutex
	errs []error
}

func NewMultiKeysNotifier(size int) Notifier {
	n := &MultiKeysNotifier{}
	n.wg.Add(size)
	return n
}

func (s *MultiKeysNotifier) Notify(err error) {
	s.wg.Done()
	if err != nil {
		s.errs = append(s.errs, err)
	}
}

func (s *MultiKeysNotifier) Wait() []error {
	s.wg.Wait()
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.errs
}
