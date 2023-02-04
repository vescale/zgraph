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

import (
	"strconv"
)

type Plan interface {
	// ID gets the ID.
	ID() int
	// TP gets the plan type.
	TP() string
	// Columns gets the result columns.
	Columns() ResultColumns
	// SetColumns sets the result columns
	SetColumns(cols ResultColumns)
	// ExplainID gets the ID in explain statement
	ExplainID() string
	// ExplainInfo returns operator information to be explained.
	ExplainInfo() string
}

type LogicalPlan interface {
	Plan

	// Children returns all the children.
	Children() []LogicalPlan
	// SetChildren sets the children for the plan.
	SetChildren(...LogicalPlan)
	// SetChild sets the ith child for the plan.
	SetChild(i int, child LogicalPlan)
}

type PhysicalPlan interface {
	Plan

	// Children returns all the children.
	Children() []PhysicalPlan
	// SetChildren sets the children for the plan.
	SetChildren(...PhysicalPlan)
	// SetChild sets the ith child for the plan.
	SetChild(i int, child PhysicalPlan)
}

type basePlan struct {
	tp      string
	id      int
	columns ResultColumns
}

// ID implements the Plan interface.
func (p *basePlan) ID() int {
	return p.id
}

// TP implements the Plan interface.
func (p *basePlan) TP() string {
	return p.tp
}

// Columns implements the Plan.Columns interface.
func (p *basePlan) Columns() ResultColumns {
	return p.columns
}

func (p *basePlan) SetColumns(cols ResultColumns) {
	p.columns = cols
}

// ExplainID implements the Plan interface.
func (p *basePlan) ExplainID() string {
	return p.tp + "_" + strconv.Itoa(p.id)
}

// ExplainInfo implements Plan interface.
func (*basePlan) ExplainInfo() string {
	return "N/A"
}

type baseLogicalPlan struct {
	basePlan

	self     LogicalPlan
	children []LogicalPlan
}

// Children implements LogicalPlan Children interface.
func (p *baseLogicalPlan) Children() []LogicalPlan {
	return p.children
}

// SetChildren implements LogicalPlan SetChildren interface.
func (p *baseLogicalPlan) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// SetChild implements LogicalPlan SetChild interface.
func (p *baseLogicalPlan) SetChild(i int, child LogicalPlan) {
	p.children[i] = child
}

func (p *baseLogicalPlan) Columns() ResultColumns {
	if p.columns == nil && len(p.Children()) == 1 {
		// default implementation for plans has only one child: proprgate child columns.
		// multi-children plans are likely to have particular implementation.
		p.columns = p.Children()[0].Columns()
	}
	return p.columns
}

type basePhysicalPlan struct {
	basePlan

	self     PhysicalPlan
	children []PhysicalPlan
}

// Children implements PhysicalPlan Children interface.
func (p *basePhysicalPlan) Children() []PhysicalPlan {
	return p.children
}

// SetChildren implements PhysicalPlan SetChildren interface.
func (p *basePhysicalPlan) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// SetChild implements PhysicalPlan SetChild interface.
func (p *basePhysicalPlan) SetChild(i int, child PhysicalPlan) {
	p.children[i] = child
}

func (p *basePhysicalPlan) Columns() ResultColumns {
	if p.columns == nil && len(p.Children()) == 1 {
		// default implementation for plans has only one child: proprgate child columns.
		// multi-children plans are likely to have particular implementation.
		p.columns = p.Children()[0].Columns()
	}
	return p.columns
}

// LogicalDual represents the plan which returns empty result set.
type LogicalDual struct {
	baseLogicalPlan
}
