// Copyright 2022 zGraph Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import "github.com/vescale/zgraph/types"

// Constant represents a literal constant.
type Constant struct {
	Value types.Datum
}

func (c *Constant) Clone() Expression {
	cc := *c
	return &cc
}

// String implements the fmt.Stringer interface.
func (c *Constant) String() string {
	return "datum doesn't implement fmt.Stringer)"
}

func (c *Constant) Eval(row Row) (types.Datum, error) {
	return c.Value, nil
}

func (c *Constant) EvalInt(row Row) (Nullable[int64], error) {
	var result Nullable[int64]
	if c.Value.IsNull() {
		return result, nil
	}
	result.Set(c.Value.GetInt64())
	return result, nil
}

func (c *Constant) EvalReal(row Row) (Nullable[float64], error) {
	var result Nullable[float64]
	if c.Value.IsNull() {
		return result, nil
	}
	result.Set(c.Value.GetFloat64())
	return result, nil
}

func (c *Constant) EvalString(row Row) (Nullable[string], error) {
	var result Nullable[string]
	if c.Value.IsNull() {
		return result, nil
	}
	result.Set(c.Value.GetString())
	return result, nil
}

func (c *Constant) EvalDecimal(row Row) (Nullable[*types.Decimal], error) {
	var result Nullable[*types.Decimal]
	if c.Value.IsNull() {
		return result, nil
	}
	result.Set(c.Value.GetDecimal())
	return result, nil
}
