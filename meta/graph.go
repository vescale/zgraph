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

import (
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/parser/model"
)

func (*Meta) graphKey(id int64) []byte {
	return GraphKey(id)
}

// GraphKey encodes the graph identifier into graph key.
func GraphKey(id int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mGraphPrefix, id))
}

func (m *Meta) checkGraphExists(graphKey []byte) error {
	v, err := m.txn.HGet(mGraphs, graphKey)
	if err == nil && v == nil {
		err = ErrGraphNotExists
	}
	return errors.Trace(err)
}

func (m *Meta) checkGraphNotExists(graphKey []byte) error {
	v, err := m.txn.HGet(mGraphs, graphKey)
	if err == nil && v != nil {
		err = ErrGraphExists
	}
	return errors.Trace(err)
}

// CreateGraph creates a graph.
func (m *Meta) CreateGraph(info *model.GraphInfo) error {
	graphKey := m.graphKey(info.ID)

	if err := m.checkGraphNotExists(graphKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(info)
	if err != nil {
		return errors.Trace(err)
	}

	return m.txn.HSet(mGraphs, graphKey, data)
}

// UpdateGraph updates a graph.
func (m *Meta) UpdateGraph(info *model.GraphInfo) error {
	graphKey := m.graphKey(info.ID)

	if err := m.checkGraphExists(graphKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(info)
	if err != nil {
		return errors.Trace(err)
	}

	return m.txn.HSet(mGraphs, graphKey, data)
}

// DropGraph drops whole graph.
func (m *Meta) DropGraph(graphID int64) error {
	// Check if graph exists.
	graphKey := m.graphKey(graphID)
	if err := m.txn.HClear(graphKey); err != nil {
		return errors.Trace(err)
	}

	if err := m.txn.HDel(mGraphs, graphKey); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// ListGraphs shows all graphs.
func (m *Meta) ListGraphs() ([]*model.GraphInfo, error) {
	res, err := m.txn.HGetAll(mGraphs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	graphs := make([]*model.GraphInfo, 0, len(res))
	for _, r := range res {
		graphInfo := &model.GraphInfo{}
		err = json.Unmarshal(r.Value, graphInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		graphs = append(graphs, graphInfo)
	}
	return graphs, nil
}

// GetGraph gets the database value with ID.
func (m *Meta) GetGraph(graphID int64) (*model.GraphInfo, error) {
	graphKey := m.graphKey(graphID)
	value, err := m.txn.HGet(mGraphs, graphKey)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	graphInfo := &model.GraphInfo{}
	err = json.Unmarshal(value, graphInfo)
	return graphInfo, errors.Trace(err)
}

// AdvanceID advances the local ID allocator by n, and return the old global ID.
// NOTE: It's better to call graph.MDLock() to reduce transaction conflicts.
func (m *Meta) AdvanceID(graphID int64, n int) (int64, error) {
	// Check if graph exists.
	graphKey := m.graphKey(graphID)
	if err := m.checkGraphExists(graphKey); err != nil {
		return 0, errors.Trace(err)
	}

	newID, err := m.txn.HInc(graphKey, mNextIDKey, int64(n))
	if err != nil {
		return 0, err
	}
	if newID > MaxGlobalID {
		return 0, errors.Errorf("global id:%d exceeds the limit:%d", newID, MaxGlobalID)
	}
	origID := newID - int64(n)
	return origID, nil
}
