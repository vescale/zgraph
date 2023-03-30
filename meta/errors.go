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

package meta

import "github.com/pingcap/errors"

var (
	// ErrGraphExists is the error for db exists.
	ErrGraphExists = errors.New("graph exists")
	// ErrGraphNotExists is the error for db not exists.
	ErrGraphNotExists    = errors.New("graph not exists")
	ErrInvalidString     = errors.New("invalid string")
	ErrLabelExists       = errors.New("label exists")
	ErrLabelNotExists    = errors.New("label not exists")
	ErrIndexExists       = errors.New("index exists")
	ErrIndexNotExists    = errors.New("index not exists")
	ErrPropertyExists    = errors.New("property exists")
	ErrPropertyNotExists = errors.New("property not exists")
	ErrNoGraphSelected   = errors.New("no graph selected")
)
