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

package storage

import (
	"context"

	"github.com/cockroachdb/pebble"
)

type snapshot struct {
	ver Version
	db  *pebble.DB
}

func (s *snapshot) Get(ctx context.Context, k Key) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (s *snapshot) Iter(k Key, upperBound Key) (Iterator, error) {
	//TODO implement me
	panic("implement me")
}

func (s *snapshot) IterReverse(k Key) (Iterator, error) {
	//TODO implement me
	panic("implement me")
}

func (s *snapshot) BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error) {
	//TODO implement me
	panic("implement me")
}
