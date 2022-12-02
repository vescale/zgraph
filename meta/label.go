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
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/parser/model"
)

func (*Meta) labelKey(labelID int64) []byte {
	return LabelKey(labelID)
}

// LabelKey encodes the labelID into label key.
func LabelKey(labelID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mLabelPrefix, labelID))
}

// IsLabelKey checks whether the label key comes from LabelKey().
func IsLabelKey(labelKey []byte) bool {
	return strings.HasPrefix(string(labelKey), mLabelPrefix+":")
}

// ParseLabelKey decodes the label key to get label id.
func ParseLabelKey(labelKey []byte) (int64, error) {
	if !strings.HasPrefix(string(labelKey), mLabelPrefix) {
		return 0, ErrInvalidString
	}

	labelID := strings.TrimPrefix(string(labelKey), mLabelPrefix+":")
	id, err := strconv.Atoi(labelID)
	return int64(id), errors.Trace(err)
}

func (m *Meta) checkLabelExists(graphKey []byte, labelKey []byte) error {
	v, err := m.txn.HGet(graphKey, labelKey)
	if err == nil && v == nil {
		err = ErrLabelNotExists
	}
	return errors.Trace(err)
}

func (m *Meta) checkLabelNotExists(graphKey []byte, labelKey []byte) error {
	v, err := m.txn.HGet(graphKey, labelKey)
	if err == nil && v != nil {
		err = ErrLabelExists
	}
	return errors.Trace(err)
}

// CreateLabel creates a label.
func (m *Meta) CreateLabel(graphID int64, labelInfo *model.LabelInfo) error {
	// Check if graph exists.
	graphKey := m.graphKey(graphID)
	if err := m.checkGraphExists(graphKey); err != nil {
		return errors.Trace(err)
	}

	// Check if label exists.
	lableKey := m.labelKey(labelInfo.ID)
	if err := m.checkLabelNotExists(graphKey, lableKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(labelInfo)
	if err != nil {
		return errors.Trace(err)
	}

	return m.txn.HSet(graphKey, lableKey, data)
}

// UpdateLabel updates the label.
func (m *Meta) UpdateLabel(graphID int64, labelInfo *model.LabelInfo) error {
	// Check if graph exists.
	graphKey := m.graphKey(graphID)
	if err := m.checkGraphExists(graphKey); err != nil {
		return errors.Trace(err)
	}

	// Check if label exists.
	labelKey := m.labelKey(labelInfo.ID)
	if err := m.checkLabelExists(graphKey, labelKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(labelInfo)
	if err != nil {
		return errors.Trace(err)
	}

	err = m.txn.HSet(graphKey, labelKey, data)
	return errors.Trace(err)
}

// DropLabel drops label in graph.
func (m *Meta) DropLabel(graphID int64, labelID int64) error {
	// Check if graph exists.
	graphKey := m.graphKey(graphID)
	if err := m.checkGraphExists(graphKey); err != nil {
		return errors.Trace(err)
	}

	// Check if label exists.
	labelKey := m.labelKey(labelID)
	if err := m.checkLabelExists(graphKey, labelKey); err != nil {
		return errors.Trace(err)
	}

	if err := m.txn.HDel(graphKey, labelKey); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ListLabels shows all labels in a graph.
func (m *Meta) ListLabels(graphID int64) ([]*model.LabelInfo, error) {
	graphKey := m.graphKey(graphID)
	if err := m.checkGraphExists(graphKey); err != nil {
		return nil, errors.Trace(err)
	}

	res, err := m.txn.HGetAll(graphKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	labels := make([]*model.LabelInfo, 0, len(res)/2)
	for _, r := range res {
		// only handle label meta
		labelKey := string(r.Field)
		if !strings.HasPrefix(labelKey, mLabelPrefix) {
			continue
		}

		tbInfo := &model.LabelInfo{}
		err = json.Unmarshal(r.Value, tbInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}

		labels = append(labels, tbInfo)
	}

	return labels, nil
}

// GetLabel gets the label value in a graph.
func (m *Meta) GetLabel(graphID int64, labelID int64) (*model.LabelInfo, error) {
	// Check if graph exists.
	graphKey := m.graphKey(graphID)
	if err := m.checkGraphExists(graphKey); err != nil {
		return nil, errors.Trace(err)
	}

	labelKey := m.labelKey(labelID)
	value, err := m.txn.HGet(graphKey, labelKey)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	labelInfo := &model.LabelInfo{}
	err = json.Unmarshal(value, labelInfo)
	return labelInfo, errors.Trace(err)
}
