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

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	assert := assert.New(t)
	db, err := Open(t.TempDir(), nil)
	assert.Nil(err)
	assert.NotNil(db)
}

func TestDB_NewSession(t *testing.T) {
	assert := assert.New(t)
	db, err := Open(t.TempDir(), nil)
	assert.Nil(err)
	assert.NotNil(db)
	defer db.Close()

	session := db.NewSession()
	assert.NotNil(session)

	ctx := context.Background()
	rs, err := session.Execute(ctx, "create graph graph1000")
	assert.Nil(err)

	err = rs.Next(ctx)
	assert.Nil(err)

	// Check the catalog.
	catalog := db.Catalog()
	graph := catalog.Graph("graph1000")
	assert.NotNil(graph)
}
