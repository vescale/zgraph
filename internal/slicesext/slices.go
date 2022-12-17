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

package slicesext

import (
	"golang.org/x/exp/slices"
)

func ContainsFunc[S ~[]E, E any](s S, f func(E) bool) bool {
	return slices.IndexFunc(s, f) >= 0
}

func FilterFunc[S ~[]E, E any](s S, f func(E) bool) S {
	n := 0
	for i, v := range s {
		if f(v) {
			s[n] = s[i]
			n++
		}
	}
	return s[:n]
}

func FindFunc[S ~[]E, E any](s S, f func(E) bool) (e E, ok bool) {
	for _, v := range s {
		if f(v) {
			return v, true
		}
	}
	return
}
