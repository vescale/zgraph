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

package catalog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph/parser/model"
)

func TestCatalog_Apply(t *testing.T) {
	assert := assert.New(t)

	catalog := &Catalog{
		byID:   map[int64]*Graph{},
		byName: map[string]*Graph{},
	}

	cases := []struct {
		patch   *Patch
		checker func()
	}{
		{
			patch: &Patch{
				Type: PatchTypeCreateGraph,
				Data: &model.GraphInfo{
					ID:   1,
					Name: model.NewCIStr("graph1"),
				},
			},
			checker: func() {
				assert.NotNil(catalog.Graph("graph1"))
			},
		},
		{
			patch: &Patch{
				Type: PatchTypeCreateLabel,
				Data: &PatchLabel{
					GraphID: 1,
					LabelInfo: &model.LabelInfo{
						ID:   2,
						Name: model.NewCIStr("label1"),
					},
				},
			},
			checker: func() {
				graph := catalog.Graph("graph1")
				assert.NotNil(graph.Label("label1"))
			},
		},
		{
			patch: &Patch{
				Type: PatchTypeCreateLabel,
				Data: &PatchLabel{
					GraphID: 1,
					LabelInfo: &model.LabelInfo{
						ID:   3,
						Name: model.NewCIStr("label2"),
					},
				},
			},
			checker: func() {
				graph := catalog.Graph("graph1")
				assert.NotNil(graph.Label("label2"))
			},
		},
		{
			patch: &Patch{
				Type: PatchTypeDropLabel,
				Data: &PatchLabel{
					GraphID: 1,
					LabelInfo: &model.LabelInfo{
						ID:   2,
						Name: model.NewCIStr("label1"),
					},
				},
			},
			checker: func() {
				graph := catalog.Graph("graph1")
				assert.Nil(graph.Label("label1"))
				assert.NotNil(graph.Label("label2"))
			},
		},
		{
			patch: &Patch{
				Type: PatchTypeDropGraph,
				Data: &model.GraphInfo{
					ID:   1,
					Name: model.NewCIStr("graph1"),
				},
			},
			checker: func() {
				assert.Nil(catalog.Graph("graph1"))
			},
		},
	}

	for _, c := range cases {
		catalog.Apply(c.patch)
		if c.checker != nil {
			c.checker()
		}
	}
}
