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

package executor

import (
	"context"

	"github.com/vescale/zgraph/expression"
	"github.com/vescale/zgraph/internal/chunk"
	"github.com/vescale/zgraph/stmtctx"
)

// Executor is the physical implementation of a algebra operator.
//
// In TiDB, all algebra operators are implemented as iterators, i.e., they
// support a simple Open-Next-Close protocol. See this paper for more details:
//
// "Volcano-An Extensible and Parallel Query Evaluation System"
//
// Different from Volcano's execution model, a "Next" function call in zGraph will
// return a batch of rows, other than a single row in Volcano.
// NOTE: Executors must call "chk.Reset()" before appending their results to it.
type Executor interface {
	base() *baseExecutor
	Open(context.Context) error
	Next(ctx context.Context, req *chunk.Chunk) error
	Close() error
	Schema() *expression.Schema
}

type baseExecutor struct {
	ctx      *stmtctx.Context
	id       int
	schema   *expression.Schema // output schema
	children []Executor
}

func newBaseExecutor(ctx *stmtctx.Context, schema *expression.Schema, id int, children ...Executor) baseExecutor {
	e := baseExecutor{
		children: children,
		ctx:      ctx,
		id:       id,
		schema:   schema,
	}
	return e
}

// base returns the baseExecutor of an executor, don't override this method!
func (e *baseExecutor) base() *baseExecutor {
	return e
}

// Open initializes children recursively and "childrenResults" according to children's schemas.
func (e *baseExecutor) Open(ctx context.Context) error {
	for _, child := range e.children {
		err := child.Open(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// Next fills multiple rows into a chunk.
func (e *baseExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	return nil
}

// Close closes all executors and release all resources.
func (e *baseExecutor) Close() error {
	var firstErr error
	for _, src := range e.children {
		if err := src.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Schema returns the current baseExecutor's schema. If it is nil, then create and return a new one.
func (e *baseExecutor) Schema() *expression.Schema {
	if e.schema == nil {
		return expression.NewSchema()
	}
	return e.schema
}
