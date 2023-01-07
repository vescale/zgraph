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

package expression

import (
	"fmt"

	"github.com/vescale/zgraph/stmtctx"
	"github.com/vescale/zgraph/types"
)

// PropertyAccess represents a property access expression.
type PropertyAccess struct {
	// Column is the column that the property is being accessed from.
	// e.g:
	// SELECT n.name, n.age FROM MATCH (n)
	// -----------------^-----------------
	// n.age is being accessed from the second column.
	Column      *Column
	VariableRef *VariableRef
	PropertyRef *PropertyRef
}

func (p *PropertyAccess) String() string {
	return fmt.Sprintf("%s.%s", p.VariableRef.String(), p.PropertyRef.String())
}

func (p *PropertyAccess) Clone() Expression {
	return &PropertyAccess{
		Column:      p.Column.Clone().(*Column),
		VariableRef: p.VariableRef,
		PropertyRef: p.PropertyRef,
	}
}

func (p *PropertyAccess) Eval(ctx *stmtctx.Context, row Row) (types.Datum, error) {
	value, err := p.Column.Eval(ctx, row)
	if err != nil {
		return types.Datum{}, err
	}
	if value.Kind() != types.KindGraphVar {
		return types.Datum{}, fmt.Errorf("cannot access property from non-graph variable")
	}
	graphVar := value.GetGraphVar()
	if property, ok := graphVar.Properties[p.PropertyRef.Property.Name.L]; ok {
		return property, nil
	} else {
		return types.Datum{}, nil
	}
}
