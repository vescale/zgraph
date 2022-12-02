// Copyright 2022 zGraph Authors. All rights reserved.
//
// Copyright 2015 PingCAP, Inc.
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

package structure

import (
	"github.com/pingcap/errors"
	"github.com/vescale/zgraph/storage/kv"
)

var (
	ErrWriteOnSnapshot     = errors.New("write on snapshot")
	ErrInvalidListIndex    = errors.New("invalid list index")
	ErrInvalidListMetaData = errors.New("invalid list metadata")
	ErrInvalidHashKeyFlag  = errors.New("invalid hash key flash")
)

// NewStructure creates a TxStructure with Retriever, RetrieverMutator and key prefix.
func NewStructure(reader kv.Retriever, readWriter kv.RetrieverMutator, prefix []byte) *TxStructure {
	return &TxStructure{
		reader:     reader,
		readWriter: readWriter,
		prefix:     prefix,
	}
}

// TxStructure supports some simple data structures like string, hash, list, etc... and
// you can use these in a transaction.
type TxStructure struct {
	reader     kv.Retriever
	readWriter kv.RetrieverMutator
	prefix     []byte
}
