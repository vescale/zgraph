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

func (*Meta) propertyKey(propertyID uint16) []byte {
	return PropertyKey(propertyID)
}

// PropertyKey encodes the propertyID into property key.
func PropertyKey(propertyID uint16) []byte {
	return []byte(fmt.Sprintf("%s:%d", mPropertyPrefix, propertyID))
}

// IsPropertyKey checks whether the property key comes from PropertyKey().
func IsPropertyKey(propertyKey []byte) bool {
	return strings.HasPrefix(string(propertyKey), mPropertyPrefix+":")
}

// ParsePropertyKey decodes the property key to get property id.
func ParsePropertyKey(propertyKey []byte) (int64, error) {
	if !strings.HasPrefix(string(propertyKey), mPropertyPrefix) {
		return 0, ErrInvalidString
	}

	propertyID := strings.TrimPrefix(string(propertyKey), mPropertyPrefix+":")
	id, err := strconv.Atoi(propertyID)
	return int64(id), errors.Trace(err)
}

func (m *Meta) checkPropertyExists(graphKey []byte, propertyKey []byte) error {
	v, err := m.txn.HGet(graphKey, propertyKey)
	if err == nil && v == nil {
		err = ErrPropertyNotExists
	}
	return errors.Trace(err)
}

func (m *Meta) checkPropertyNotExists(graphKey []byte, propertyKey []byte) error {
	v, err := m.txn.HGet(graphKey, propertyKey)
	if err == nil && v != nil {
		err = ErrPropertyExists
	}
	return errors.Trace(err)
}

// CreateProperty creates a property.
func (m *Meta) CreateProperty(graphID int64, propertyInfo *model.PropertyInfo) error {
	// Check if graph exists.
	graphKey := m.graphKey(graphID)
	if err := m.checkGraphExists(graphKey); err != nil {
		return errors.Trace(err)
	}

	// Check if property exists.
	lableKey := m.propertyKey(propertyInfo.ID)
	if err := m.checkPropertyNotExists(graphKey, lableKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(propertyInfo)
	if err != nil {
		return errors.Trace(err)
	}

	return m.txn.HSet(graphKey, lableKey, data)
}

// UpdateProperty updates the property.
func (m *Meta) UpdateProperty(graphID int64, propertyInfo *model.PropertyInfo) error {
	// Check if graph exists.
	graphKey := m.graphKey(graphID)
	if err := m.checkGraphExists(graphKey); err != nil {
		return errors.Trace(err)
	}

	// Check if property exists.
	propertyKey := m.propertyKey(propertyInfo.ID)
	if err := m.checkPropertyExists(graphKey, propertyKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(propertyInfo)
	if err != nil {
		return errors.Trace(err)
	}

	err = m.txn.HSet(graphKey, propertyKey, data)
	return errors.Trace(err)
}

// DropProperty drops property in graph.
func (m *Meta) DropProperty(graphID int64, propertyID uint16) error {
	// Check if graph exists.
	graphKey := m.graphKey(graphID)
	if err := m.checkGraphExists(graphKey); err != nil {
		return errors.Trace(err)
	}

	// Check if property exists.
	propertyKey := m.propertyKey(propertyID)
	if err := m.checkPropertyExists(graphKey, propertyKey); err != nil {
		return errors.Trace(err)
	}

	if err := m.txn.HDel(graphKey, propertyKey); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ListProperties shows all properties in a graph.
func (m *Meta) ListProperties(graphID int64) ([]*model.PropertyInfo, error) {
	graphKey := m.graphKey(graphID)
	if err := m.checkGraphExists(graphKey); err != nil {
		return nil, errors.Trace(err)
	}

	res, err := m.txn.HGetAll(graphKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	propertys := make([]*model.PropertyInfo, 0, len(res)/2)
	for _, r := range res {
		// only handle property meta
		propertyKey := string(r.Field)
		if !strings.HasPrefix(propertyKey, mPropertyPrefix) {
			continue
		}

		tbInfo := &model.PropertyInfo{}
		err = json.Unmarshal(r.Value, tbInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}

		propertys = append(propertys, tbInfo)
	}

	return propertys, nil
}

// GetProperty gets the property value in a graph.
func (m *Meta) GetProperty(graphID int64, propertyID uint16) (*model.PropertyInfo, error) {
	// Check if graph exists.
	graphKey := m.graphKey(graphID)
	if err := m.checkGraphExists(graphKey); err != nil {
		return nil, errors.Trace(err)
	}

	propertyKey := m.propertyKey(propertyID)
	value, err := m.txn.HGet(graphKey, propertyKey)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	propertyInfo := &model.PropertyInfo{}
	err = json.Unmarshal(value, propertyInfo)
	return propertyInfo, errors.Trace(err)
}
