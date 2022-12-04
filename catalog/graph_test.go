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

func TestNewGraph(t *testing.T) {
	meta := &model.GraphInfo{
		ID:   1,
		Name: model.NewCIStr("test-graph"),
	}

	graph := NewGraph(meta)
	assert := assert.New(t)
	assert.Equal(graph.Meta(), meta)
}

func TestGraph_Label(t *testing.T) {
	meta := &model.GraphInfo{
		ID:   1,
		Name: model.NewCIStr("test-graph"),
		Labels: []*model.LabelInfo{
			{
				ID:   2,
				Name: model.NewCIStr("label1"),
			},
		},
	}

	graph := NewGraph(meta)
	assert := assert.New(t)
	assert.Equal(graph.Label("label1").Meta(), meta.Labels[0])
}

func TestGraph_LabelByID(t *testing.T) {
	meta := &model.GraphInfo{
		ID:   1,
		Name: model.NewCIStr("test-graph"),
		Labels: []*model.LabelInfo{
			{
				ID:   2,
				Name: model.NewCIStr("label1"),
			},
		},
	}

	graph := NewGraph(meta)
	assert := assert.New(t)
	assert.Equal(graph.LabelByID(2).Meta(), meta.Labels[0])
}
