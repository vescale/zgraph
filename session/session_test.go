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

package session_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph"
	"github.com/vescale/zgraph/session"
)

func TestNew(t *testing.T) {
	assert := assert.New(t)
	db, err := zgraph.Open(t.TempDir(), nil)
	assert.Nil(err)
	assert.NotNil(db)

	s := session.New(db.Store(), db.Catalog())
	assert.NotNil(s)
}

func TestSession_OnClosed(t *testing.T) {
	assert := assert.New(t)
	db, err := zgraph.Open(t.TempDir(), nil)
	assert.Nil(err)
	assert.NotNil(db)

	s := session.New(db.Store(), db.Catalog())
	assert.NotNil(s)

	var closed bool
	s.OnClosed(func(session *session.Session) {
		closed = true
	})

	s.Close()
	assert.True(closed)
}
