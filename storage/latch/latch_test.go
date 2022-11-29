// Copyright 2022 zGraph Authors. All rights reserved.
//
// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package latch

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vescale/zgraph/storage/kv"
	"github.com/vescale/zgraph/storage/mvcc"
)

var baseTso = uint64(time.Now().UnixNano())

func getTso() mvcc.Version {
	return mvcc.Version(atomic.AddUint64(&baseTso, uint64(1)))
}

func TestWakeUp(t *testing.T) {
	assert := assert.New(t)
	latches := NewLatches(256)
	keysA := []kv.Key{
		[]byte("a"), []byte("b"), []byte("c")}
	startVerA := getTso()
	lockA := latches.genLock(startVerA, keysA)

	keysB := []kv.Key{[]byte("d"), []byte("e"), []byte("a"), []byte("c")}
	startVerB := getTso()
	lockB := latches.genLock(startVerB, keysB)

	// A acquire lock success.
	result := latches.acquire(lockA)
	assert.Equal(result, acquireSuccess)

	// B acquire lock failed.
	result = latches.acquire(lockB)
	assert.Equal(result, acquireLocked)

	// A release lock, and get wakeup list.
	commitTSA := getTso()
	wakeupList := make([]*Lock, 0)
	lockA.SetCommitTS(commitTSA)
	wakeupList = latches.release(lockA, wakeupList)
	assert.Equal(wakeupList[0].startVer, startVerB)

	// B acquire failed since startVerB has stale for some keys.
	result = latches.acquire(lockB)
	assert.Equal(result, acquireStale)

	// B release lock since it received a stale.
	wakeupList = latches.release(lockB, wakeupList)
	assert.Equal(0, len(wakeupList))

	// B restart:get a new startVer.
	startVerB = getTso()
	lockB = latches.genLock(startVerB, keysB)
	result = latches.acquire(lockB)
	assert.Equal(result, acquireSuccess)
}

func TestFirstAcquireFailedWithStale(t *testing.T) {
	assert := assert.New(t)
	latches := NewLatches(256)

	keys := []kv.Key{
		[]byte("a"), []byte("b"), []byte("c")}
	startVerA := getTso()
	lockA := latches.genLock(startVerA, keys)
	startVerB := getTso()
	lockB := latches.genLock(startVerA, keys)

	// acquire lockA success
	result := latches.acquire(lockA)
	assert.Equal(result, acquireSuccess)

	// release lockA
	commitVerA := getTso()
	wakeupList := make([]*Lock, 0)
	lockA.SetCommitTS(commitVerA)
	latches.release(lockA, wakeupList)

	assert.Greater(commitVerA, startVerB)
	// acquire lockB first time, should be failed with stale since commitVerA > startVerB
	result = latches.acquire(lockB)
	assert.Equal(result, acquireStale)
	latches.release(lockB, wakeupList)
}

func TestRecycle(t *testing.T) {
	assert := assert.New(t)
	latches := NewLatches(8)
	startVer := getTso()
	lock := latches.genLock(startVer, []kv.Key{
		[]byte("a"), []byte("b"),
	})
	lock1 := latches.genLock(startVer, []kv.Key{
		[]byte("b"), []byte("c"),
	})
	assert.Equal(latches.acquire(lock), acquireSuccess)
	assert.Equal(latches.acquire(lock1), acquireLocked)
	lock.SetCommitTS(startVer + 1)
	var wakeupList []*Lock
	latches.release(lock, wakeupList)
	// Release lock will grant latch to lock1 automatically,
	// so release lock1 is called here.
	latches.release(lock1, wakeupList)

	lock2 := latches.genLock(startVer+3, []kv.Key{
		[]byte("b"), []byte("c"),
	})
	assert.Equal(latches.acquire(lock2), acquireSuccess)
	wakeupList = wakeupList[:0]
	latches.release(lock2, wakeupList)

	allEmpty := true
	for i := 0; i < len(latches.slots); i++ {
		latch := &latches.slots[i]
		if latch.queue != nil {
			allEmpty = false
		}
	}
	assert.False(allEmpty)

	currentTS := mvcc.Version(time.Now().Add(expireDuration).UnixNano()) + 3
	latches.recycle(currentTS)

	for i := 0; i < len(latches.slots); i++ {
		latch := &latches.slots[i]
		assert.Nil(latch.queue)
	}
}
