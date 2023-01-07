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

	"github.com/vescale/zgraph/expression"
	"github.com/vescale/zgraph/parser/model"
)

type Plan interface {
	// ID gets the ID.
	ID() int
	// TP gets the plan type.
	TP() string
	// Schema returns the schema.
	Schema() *expression.Schema
	// OutputNames returns the output names.
	OutputNames() []model.CIStr
	// SetOutputNames sets the output names.
	SetOutputNames(names []model.CIStr)
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
	tp string
	id int
}

// ID implements the Plan interface.
func (p *basePlan) ID() int {
	return p.id
}

// TP implements the Plan interface.
func (p *basePlan) TP() string {
	return p.tp
}

// ExplainID implements the Plan interface.
func (p *basePlan) ExplainID() string {
	return p.tp + "_" + strconv.Itoa(p.id)
}

// ExplainInfo implements Plan interface.
func (*basePlan) ExplainInfo() string {
	return "N/A"
}

// baseSchemaProducer stores the schema for the base plans who can produce schema directly.
type baseSchemaProducer struct {
	basePlan

	schema *expression.Schema
	names  []model.CIStr
}

// Schema implements the Plan interface.
func (sp *baseSchemaProducer) Schema() *expression.Schema {
	return sp.schema
}

func (sp *baseSchemaProducer) OutputNames() []model.CIStr {
	return sp.names
}

func (sp *baseSchemaProducer) SetOutputNames(names []model.CIStr) {
	sp.names = names
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

// physicalSchemaProducer stores the schema for the physical plans who can produce schema directly.
type physicalSchemaProducer struct {
	basePhysicalPlan

	schema *expression.Schema
	names  []model.CIStr
}

// Schema implements the Plan.Schema interface.
func (s *physicalSchemaProducer) Schema() *expression.Schema {
	if s.schema == nil {
		if len(s.Children()) == 1 {
			// default implementation for plans has only one child: proprgate child schema.
			// multi-children plans are likely to have particular implementation.
			s.schema = s.Children()[0].Schema().Clone()
		} else {
			s.schema = expression.NewSchema()
		}
	}
	return s.schema
}

// SetSchema implements the Plan.SetSchema interface.
func (s *physicalSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

func (s *physicalSchemaProducer) OutputNames() []model.CIStr {
	if s.names == nil && len(s.Children()) == 1 {
		// default implementation for plans has only one child: proprgate child `OutputNames`.
		// multi-children plans are likely to have particular implementation.
		s.names = s.Children()[0].OutputNames()
	}
	return s.names
}

func (s *physicalSchemaProducer) SetOutputNames(names []model.CIStr) {
	s.names = names
}

// logicalSchemaProducer stores the schema for the logical plans who can produce schema directly.
type logicalSchemaProducer struct {
	baseLogicalPlan

	schema *expression.Schema
	names  []model.CIStr
}

// Schema implements the Plan.Schema interface.
func (s *logicalSchemaProducer) Schema() *expression.Schema {
	if s.schema == nil {
		if len(s.Children()) == 1 {
			// default implementation for plans has only one child: proprgate child schema.
			// multi-children plans are likely to have particular implementation.
			s.schema = s.Children()[0].Schema().Clone()
		} else {
			s.schema = expression.NewSchema()
		}
	}
	return s.schema
}

// SetSchema implements the Plan.SetSchema interface.
func (s *logicalSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

func (s *logicalSchemaProducer) OutputNames() []model.CIStr {
	if s.names == nil && len(s.Children()) == 1 {
		// default implementation for plans has only one child: proprgate child `OutputNames`.
		// multi-children plans are likely to have particular implementation.
		s.names = s.Children()[0].OutputNames()
	}
	return s.names
}

func (s *logicalSchemaProducer) SetOutputNames(names []model.CIStr) {
	s.names = names
}

// LogicalDual represents the plan which returns empty result set.
type LogicalDual struct {
	logicalSchemaProducer
}
