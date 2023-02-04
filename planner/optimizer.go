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

package planner

// Optimize optimizes the plan to the optimal physical plan.
func Optimize(plan LogicalPlan) Plan {
	switch p := plan.(type) {
	case *LogicalMatch:
		return optimizeMatch(p)
	case *LogicalProjection:
		return optimizeProjection(p)
	case *LogicalSelection:
		return optimizeSelection(p)
	}
	return plan
}

func optimizeMatch(plan *LogicalMatch) Plan {
	result := &PhysicalMatch{}
	result.SetColumns(plan.Columns())
	result.Subgraph = plan.Subgraph
	return result
}

func optimizeProjection(plan *LogicalProjection) Plan {
	result := &PhysicalProjection{}
	result.SetColumns(plan.Columns())
	result.Exprs = plan.Exprs
	childPlan := Optimize(plan.Children()[0])
	result.SetChildren(childPlan.(PhysicalPlan))
	return result
}

func optimizeSelection(plan *LogicalSelection) Plan {
	result := &PhysicalSelection{}
	result.SetColumns(plan.Columns())
	result.Condition = plan.Condition
	childPlan := Optimize(plan.Children()[0])
	result.SetChildren(childPlan.(PhysicalPlan))
	return result
}
