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

const defaultConcurrency = 512

// Options contains some options which is used to customize the zGraph database
// instance while instantiating zGraph.
type Options struct {
	// Concurrency is used to limit the max concurrent sessions count. The NewSession
	// method will block if the current alive sessions count reach this limitation.
	Concurrency int64
}

// SetDefaults sets the missing options into default value.
func (opt *Options) SetDefaults() {
	if opt.Concurrency <= 0 {
		opt.Concurrency = defaultConcurrency
	}
}
