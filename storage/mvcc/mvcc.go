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

package mvcc

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/google/btree"
	"github.com/pingcap/errors"
)

type ValueType int

const (
	ValueTypePut ValueType = iota
	ValueTypeDelete
	ValueTypeRollback
	ValueTypeLock
)

type Value struct {
	Type      ValueType
	StartVer  Version
	CommitVer Version
	Value     []byte
}

type Lock struct {
	StartVer Version
	Primary  []byte
	Value    []byte
	Op       Op
	TTL      uint64
}

type Entry struct {
	Key    Key
	Values []Value
	Lock   *Lock
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (l *Lock) MarshalBinary() ([]byte, error) {
	var (
		mh  marshalHelper
		buf bytes.Buffer
	)
	mh.WriteNumber(&buf, l.StartVer)
	mh.WriteSlice(&buf, l.Primary)
	mh.WriteSlice(&buf, l.Value)
	mh.WriteNumber(&buf, l.Op)
	mh.WriteNumber(&buf, l.TTL)
	return buf.Bytes(), mh.err
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (l *Lock) UnmarshalBinary(data []byte) error {
	var mh marshalHelper
	buf := bytes.NewBuffer(data)
	mh.ReadNumber(buf, &l.StartVer)
	mh.ReadSlice(buf, &l.Primary)
	mh.ReadSlice(buf, &l.Value)
	mh.ReadNumber(buf, &l.Op)
	mh.ReadNumber(buf, &l.TTL)
	return mh.err
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (v *Value) MarshalBinary() ([]byte, error) {
	var (
		mh  marshalHelper
		buf bytes.Buffer
	)
	mh.WriteNumber(&buf, int64(v.Type))
	mh.WriteNumber(&buf, v.StartVer)
	mh.WriteNumber(&buf, v.CommitVer)
	mh.WriteSlice(&buf, v.Value)
	return buf.Bytes(), mh.err
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (v *Value) UnmarshalBinary(data []byte) error {
	var mh marshalHelper
	buf := bytes.NewBuffer(data)
	var vt int64
	mh.ReadNumber(buf, &vt)
	v.Type = ValueType(vt)
	mh.ReadNumber(buf, &v.StartVer)
	mh.ReadNumber(buf, &v.CommitVer)
	mh.ReadSlice(buf, &v.Value)
	return mh.err
}

type marshalHelper struct {
	err error
}

func (mh *marshalHelper) WriteSlice(buf io.Writer, slice []byte) {
	if mh.err != nil {
		return
	}
	var tmp [binary.MaxVarintLen64]byte
	off := binary.PutUvarint(tmp[:], uint64(len(slice)))
	if err := writeFull(buf, tmp[:off]); err != nil {
		mh.err = err
	}
	if err := writeFull(buf, slice); err != nil {
		mh.err = err
	}
}

func (mh *marshalHelper) WriteNumber(buf io.Writer, n interface{}) {
	if mh.err != nil {
		return
	}
	err := binary.Write(buf, binary.LittleEndian, n)
	if err != nil {
		mh.err = errors.Trace(err)
	}
}

func writeFull(w io.Writer, slice []byte) error {
	written := 0
	for written < len(slice) {
		n, err := w.Write(slice[written:])
		if err != nil {
			return errors.Trace(err)
		}
		written += n
	}
	return nil
}

func (mh *marshalHelper) ReadNumber(r io.Reader, n interface{}) {
	if mh.err != nil {
		return
	}
	err := binary.Read(r, binary.LittleEndian, n)
	if err != nil {
		mh.err = errors.WithStack(err)
	}
}

func (mh *marshalHelper) ReadSlice(r *bytes.Buffer, slice *[]byte) {
	if mh.err != nil {
		return
	}
	sz, err := binary.ReadUvarint(r)
	if err != nil {
		mh.err = errors.WithStack(err)
		return
	}
	const c10M = 10 * 1024 * 1024
	if sz > c10M {
		mh.err = errors.New("too large slice, maybe something wrong")
		return
	}
	data := make([]byte, sz)
	if _, err := io.ReadFull(r, data); err != nil {
		mh.err = errors.WithStack(err)
		return
	}
	*slice = data
}

// LockErr returns LockedError.
// Note that parameter key is raw key, while key in LockedError is mvcc key.
func (l *Lock) LockErr(key []byte) error {
	return &LockedError{
		Key:      key,
		Primary:  l.Primary,
		StartVer: l.StartVer,
		TTL:      l.TTL,
	}
}

func (l *Lock) Check(ver Version, key []byte, resolvedLocks []Version) (Version, error) {
	// ignore when ver is older than lock or lock's type is Lock.
	if l.StartVer > ver || l.Op == Op_Lock {
		return ver, nil
	}
	// for point get the latest version.
	if ver == math.MaxUint64 && bytes.Equal(l.Primary, key) {
		return l.StartVer - 1, nil
	}
	// Skip lock if the lock is resolved.
	for _, resolved := range resolvedLocks {
		if l.StartVer == resolved {
			return ver, nil
		}
	}
	return 0, l.LockErr(key)
}

func (e *Entry) Less(than btree.Item) bool {
	return bytes.Compare(e.Key, than.(*Entry).Key) < 0
}

func (e *Entry) Get(ver Version, resolvedLocks []Version) ([]byte, error) {
	if e.Lock != nil {
		var err error
		ver, err = e.Lock.Check(ver, e.Key.Raw(), resolvedLocks)
		if err != nil {
			return nil, err
		}
	}
	for _, v := range e.Values {
		if v.CommitVer <= ver && v.Type != ValueTypeRollback && v.Type != ValueTypeLock {
			return v.Value, nil
		}
	}
	return nil, nil
}
