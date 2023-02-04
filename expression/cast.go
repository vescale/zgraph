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

	"github.com/vescale/zgraph/datum"
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

func (c *CastExpr) Eval(evalCtx *EvalContext) (datum.Datum, error) {
	d, err := c.Expr.Eval(evalCtx)
	if err != nil || d == datum.Null {
		return d, err
	}
	// Cast the datum to the desired type.
	// See https://pgql-lang.org/spec/1.5/#cast for supported casts.
	switch c.Type {
	case types.Bool:
		switch v := d.(type) {
		case *datum.Bool:
			return d, nil
		case *datum.String:
			res, err := datum.ParseBool(string(*v))
			if err != nil {
				return nil, err
			}
			return res, nil
		}
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
