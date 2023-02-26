// Copyright 2023 zGraph Authors. All rights reserved.
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

package expression

import (
	"fmt"

	"github.com/cockroachdb/apd/v3"
	"github.com/vescale/zgraph/datum"
	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/types"
)

var _ Expression = &CastExpr{}

type CastExpr struct {
	Expr Expression
	Type types.T
}

func NewCastExpr(expr Expression, typ types.T) *CastExpr {
	return &CastExpr{
		Expr: expr,
		Type: typ,
	}
}

func (c *CastExpr) String() string {
	return fmt.Sprintf("CAST(%s AS %s)", c.Expr, c.Type)
}

func (c *CastExpr) ReturnType() types.T {
	return c.Type
}

func (c *CastExpr) Eval(stmtCtx *stmtctx.Context, input datum.Row) (datum.Datum, error) {
	d, err := c.Expr.Eval(stmtCtx, input)
	if err != nil || d == datum.Null {
		return d, err
	}
	// Cast the datum to the desired type.
	// See https://pgql-lang.org/spec/1.5/#cast for supported casts.
	switch c.Type {
	case types.Bool:
		v, err := datum.TryAsBool(d)
		if err != nil {
			return nil, err
		}
		return datum.NewBool(v), nil
	case types.Int:
	case types.Float:
	case types.Decimal:
	case types.String:
	case types.Date:
	case types.Time:
	case types.TimeTZ:
	case types.Timestamp:
	case types.TimestampTZ:
	case types.Interval:
	}
	return nil, fmt.Errorf("unsupported cast: %s -> %s", d.Type(), c.Type)
}

type castFunc func(stmtCtx *stmtctx.Context, input datum.Datum) (datum.Datum, error)

func castIntAsFloat(_ *stmtctx.Context, input datum.Datum) (datum.Datum, error) {
	i := datum.AsInt(input)
	return datum.NewFloat(float64(i)), nil
}

func castIntAsDecimal(_ *stmtctx.Context, input datum.Datum) (datum.Datum, error) {
	i := datum.AsInt(input)
	d := &apd.Decimal{}
	d.SetInt64(i)
	return datum.NewDecimal(d), nil
}

func castFloatAsDecimal(_ *stmtctx.Context, input datum.Datum) (datum.Datum, error) {
	f := datum.AsFloat(input)
	d := &apd.Decimal{}
	d.SetInt64(int64(f))
	return datum.NewDecimal(d), nil
}

func castBytesAsString(_ *stmtctx.Context, input datum.Datum) (datum.Datum, error) {
	b := datum.AsBytes(input)
	return datum.NewString(string(b)), nil
}

func castTimeAsTimeTZ(_ *stmtctx.Context, input datum.Datum) (datum.Datum, error) {
	panic("unimplemented")
}

func castTimestampAsTimestampTZ(_ *stmtctx.Context, input datum.Datum) (datum.Datum, error) {
	panic("unimplemented")
}
