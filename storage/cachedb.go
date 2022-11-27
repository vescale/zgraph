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
	"sync"

	"github.com/coocood/freecache"
)

type (
	cacheDB struct {
		mu    sync.RWMutex
		cache *freecache.Cache
	}

	// MemManager adds a cache between transaction buffer and the storage to reduce requests to the storage.
	// Beware, it uses table ID for partition tables, because the keys are unique for partition tables.
	// no matter the physical IDs are the same or not.
	MemManager interface {
		// UnionGet gets the value from cacheDB first, if it not exists,
		// it gets the value from the snapshot, then caches the value in cacheDB.
		UnionGet(ctx context.Context, snapshot Snapshot, key Key) ([]byte, error)
		// UnionBatchGet gets the values from cacheDB first, if it not exists,
		// it gets the value from the snapshot, then caches the value in cacheDB.
		UnionBatchGet(ctx context.Context, snapshot Snapshot, key []Key) (map[string][]byte, error)
	}
)

// Set sets the key/value in cacheDB.
func (c *cacheDB) set(key Key, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.Set(key, value, 0)
}

// Get gets the value from cacheDB.
func (c *cacheDB) get(key Key) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if val, err := c.cache.Get(key); err == nil {
		return val
	}
	return nil
}

// UnionGet implements MemManager UnionGet interface.
func (c *cacheDB) UnionGet(ctx context.Context, snapshot Snapshot, key Key) (val []byte, err error) {
	val = c.get(key)
	// key does not exist then get from snapshot and set to cache
	if val == nil {
		val, err = snapshot.Get(ctx, key)
		if err != nil {
			return nil, err
		}

		err = c.set(key, val)
		if err != nil {
			return nil, err
		}
	}
	return val, nil
}

// UnionBatchGet implements MemManager UnionBatchGet interface.
func (c *cacheDB) UnionBatchGet(ctx context.Context, snapshot Snapshot, keys []Key) (values map[string][]byte, err error) {
	values = map[string][]byte{}
	// Retrieve from cache first.
	var missing []Key
	for _, key := range keys {
		val := c.get(key)
		if val == nil {
			missing = append(missing, key)
		} else {
			values[string(key)] = val
		}
	}
	// key does not exist then get from snapshot and set to cache.
	if len(missing) > 0 {
		vs, err := snapshot.BatchGet(ctx, missing)
		if err != nil {
			return nil, err
		}
		for k, v := range vs {
			values[k] = v
			err := c.set(Key(k), v)
			if err != nil {
				return nil, err
			}
		}
	}
	return values, nil
}

// NewCacheDB news the cacheDB.
func NewCacheDB() MemManager {
	mm := new(cacheDB)
	mm.cache = freecache.NewCache(100 * 1024 * 1024)
	return mm
}
