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
	"fmt"
	"math"
	"sort"

	"github.com/vescale/zgraph/catalog"
	"github.com/vescale/zgraph/internal/slicesext"
	"github.com/vescale/zgraph/parser/ast"
	"github.com/vescale/zgraph/parser/model"
)

type LogicalMatch struct {
	baseLogicalPlan

	Subgraph *Subgraph
}

type PhysicalMatch struct {
	basePhysicalPlan
	Subgraph *Subgraph
}

type Vertex struct {
	Name   model.CIStr
	Labels []*catalog.Label
}

// VertexPairConnection represents a connection between two vertices.
type VertexPairConnection interface {
	Name() model.CIStr
	AnyDirected() bool
	SetAnyDirected(anyDirected bool)
	SrcVarName() model.CIStr
	SetSrcVarName(name model.CIStr)
	DstVarName() model.CIStr
	SetDstVarName(name model.CIStr)
}

var (
	_ VertexPairConnection = &Edge{}
	_ VertexPairConnection = &CommonPathExpression{}
	_ VertexPairConnection = &VariableLengthPath{}
)

type baseVertexPairConnection struct {
	name        model.CIStr
	srcVarName  model.CIStr
	dstVarName  model.CIStr
	anyDirected bool
}

func (b *baseVertexPairConnection) Name() model.CIStr {
	return b.name
}

func (b *baseVertexPairConnection) SetName(name model.CIStr) {
	b.name = name
}

func (b *baseVertexPairConnection) SrcVarName() model.CIStr {
	return b.srcVarName
}

func (b *baseVertexPairConnection) SetSrcVarName(name model.CIStr) {
	b.srcVarName = name
}

func (b *baseVertexPairConnection) DstVarName() model.CIStr {
	return b.dstVarName
}

func (b *baseVertexPairConnection) SetDstVarName(name model.CIStr) {
	b.dstVarName = name
}

func (b *baseVertexPairConnection) AnyDirected() bool {
	return b.anyDirected
}

func (b *baseVertexPairConnection) SetAnyDirected(anyDirected bool) {
	b.anyDirected = anyDirected
}

type Edge struct {
	baseVertexPairConnection

	Labels []*catalog.Label
}

type CommonPathExpression struct {
	baseVertexPairConnection

	Leftmost    *Vertex
	Rightmost   *Vertex
	Vertices    map[string]*Vertex
	Connections map[string]VertexPairConnection
	Constraints ast.ExprNode
}

type PathFindingGoal int

const (
	PathFindingAll PathFindingGoal = iota
	PathFindingReaches
	PathFindingShortest
	PathFindingCheapest
)

type VariableLengthPath struct {
	baseVertexPairConnection

	Conn        VertexPairConnection
	Goal        PathFindingGoal
	MinHops     int64
	MaxHops     int64
	TopK        int64
	WithTies    bool
	Constraints ast.ExprNode
	Cost        ast.ExprNode
	HopSrc      *Vertex
	HopDst      *Vertex
}

type Subgraph struct {
	Vertices      map[string]*Vertex
	Connections   map[string]VertexPairConnection
	SingletonVars []*GraphVar
	GroupVars     []*GraphVar
}

// GraphVar represents a graph variable in a MATCH clause.
type GraphVar struct {
	Name      model.CIStr
	Anonymous bool
}

type SubgraphBuilder struct {
	graph  *catalog.Graph
	macros []*ast.PathPatternMacro
	paths  []*ast.PathPattern

	cpes          map[string]*CommonPathExpression
	vertices      map[string]*Vertex
	connections   map[string]VertexPairConnection
	singletonVars []*GraphVar
	groupVars     []*GraphVar
}

func NewSubgraphBuilder(graph *catalog.Graph) *SubgraphBuilder {
	return &SubgraphBuilder{
		graph:       graph,
		cpes:        make(map[string]*CommonPathExpression),
		vertices:    make(map[string]*Vertex),
		connections: make(map[string]VertexPairConnection),
	}
}

func (s *SubgraphBuilder) AddPathPatterns(paths ...*ast.PathPattern) *SubgraphBuilder {
	s.paths = append(s.paths, paths...)
	return s
}

func (s *SubgraphBuilder) AddPathPatternMacros(macros ...*ast.PathPatternMacro) *SubgraphBuilder {
	s.macros = append(s.macros, macros...)
	return s
}

func (s *SubgraphBuilder) Build() (*Subgraph, error) {
	s.buildVertices()
	if err := s.buildCommonPathExpressions(); err != nil {
		return nil, err
	}
	if err := s.buildConnections(); err != nil {
		return nil, err
	}
	sort.Slice(s.singletonVars, func(i, j int) bool {
		return s.singletonVars[i].Name.L < s.singletonVars[j].Name.L
	})
	sort.Slice(s.groupVars, func(i, j int) bool {
		return s.groupVars[i].Name.L < s.groupVars[j].Name.L
	})
	sg := &Subgraph{
		Vertices:      s.vertices,
		Connections:   s.connections,
		SingletonVars: s.singletonVars,
		GroupVars:     s.groupVars,
	}
	return sg, nil
}

func (s *SubgraphBuilder) buildCommonPathExpressions() error {
	for _, m := range s.macros {
		result, err := s.buildPathPatternMacro(m)
		if err != nil {
			return err
		}
		s.cpes[m.Name.L] = result
	}
	return nil
}

func (s *SubgraphBuilder) buildPathPatternMacro(macro *ast.PathPatternMacro) (*CommonPathExpression, error) {
	if macro.Path.Tp != ast.PathPatternSimple {
		return nil, fmt.Errorf("non-simple path in path pattern macro is not supported")
	}
	sg, err := NewSubgraphBuilder(s.graph).AddPathPatterns(macro.Path).Build()
	if err != nil {
		return nil, err
	}

	leftmostVarName := macro.Path.Vertices[0].Variable.Name.L
	rightmostVarName := macro.Path.Vertices[len(macro.Path.Vertices)-1].Variable.Name.L

	cpe := &CommonPathExpression{
		Leftmost:    sg.Vertices[leftmostVarName],
		Rightmost:   sg.Vertices[rightmostVarName],
		Vertices:    sg.Vertices,
		Connections: sg.Connections,
		Constraints: macro.Where,
	}
	return cpe, nil
}

func (s *SubgraphBuilder) buildVertices() {
	astVars := make(map[string]*ast.VariableSpec)
	for _, path := range s.paths {
		for _, astVertex := range path.Vertices {
			astVar := astVertex.Variable
			if v, ok := s.vertices[astVar.Name.L]; ok {
				if labels := astVar.Labels; len(labels) > 0 {
					if len(v.Labels) > 0 {
						v.Labels = slicesext.FilterFunc(v.Labels, func(l *catalog.Label) bool {
							return slicesext.ContainsFunc(labels, func(label model.CIStr) bool {
								return l.Meta().Name.Equal(label)
							})
						})
					} else {
						for _, l := range astVar.Labels {
							v.Labels = append(v.Labels, s.graph.Label(l.L))
						}
					}
				}
			} else {
				s.vertices[astVar.Name.L] = s.buildVertex(astVar)
				astVars[astVar.Name.L] = astVar
			}
		}
	}
	for name := range s.vertices {
		astVar := astVars[name]
		s.singletonVars = append(s.singletonVars, &GraphVar{
			Name:      astVar.Name,
			Anonymous: astVar.Anonymous,
		})
	}
}

func (s *SubgraphBuilder) buildVertex(astVar *ast.VariableSpec) *Vertex {
	v := &Vertex{
		Name: astVar.Name,
	}
	for _, l := range astVar.Labels {
		v.Labels = append(v.Labels, s.graph.Label(l.L))
	}
	return v
}

func (s *SubgraphBuilder) buildConnections() error {
	allConns := s.connections
	for _, path := range s.paths {
		for i, astConn := range path.Connections {
			var (
				conn VertexPairConnection
				err  error
			)
			switch path.Tp {
			case ast.PathPatternSimple:
				conn, err = s.buildSimplePath(astConn)
			case ast.PathPatternAny, ast.PathPatternAnyShortest, ast.PathPatternAllShortest, ast.PathPatternTopKShortest,
				ast.PathPatternAnyCheapest, ast.PathPatternAllCheapest, ast.PathPatternTopKCheapest, ast.PathPatternAll:
				topK := path.TopK
				conn, err = s.buildVariableLengthPath(path.Tp, topK, astConn)
			default:
				return fmt.Errorf("unsupported path pattern type: %d", path.Tp)
			}

			connName, direction, err := extractConnNameAndDirection(astConn)
			if err != nil {
				return err
			}
			leftVarName := path.Vertices[i].Variable.Name
			rightVarName := path.Vertices[i+1].Variable.Name
			srcVarName, dstVarName, anyDirected, err := resolveAndSrcDstVarName(leftVarName, rightVarName, direction)
			if err != nil {
				return err
			}
			conn.SetAnyDirected(anyDirected)
			conn.SetSrcVarName(srcVarName)
			conn.SetDstVarName(dstVarName)
			allConns[connName.L] = conn
		}
	}
	return nil
}

func (s *SubgraphBuilder) buildSimplePath(astConn ast.VertexPairConnection) (VertexPairConnection, error) {
	switch x := astConn.(type) {
	case *ast.EdgePattern:
		varName := x.Variable.Name
		edge := &Edge{}
		edge.SetName(varName)
		for _, l := range x.Variable.Labels {
			edge.Labels = append(edge.Labels, s.graph.Label(l.L))
		}
		s.singletonVars = append(s.singletonVars, &GraphVar{
			Name:      varName,
			Anonymous: x.Variable.Anonymous,
		})
		return edge, nil
	case *ast.ReachabilityPathExpr:
		conn, err := s.buildConnWithCpe(x.AnonymousName, x.Labels)
		if err != nil {
			return nil, err
		}
		vlp := &VariableLengthPath{Conn: conn}
		vlp.SetName(x.AnonymousName)
		vlp.Goal = PathFindingReaches
		if x.Quantifier != nil {
			vlp.MinHops = x.Quantifier.N
			vlp.MaxHops = x.Quantifier.M & math.MaxInt64
		} else {
			vlp.MinHops = 1
			vlp.MaxHops = 1
		}
		s.groupVars = append(s.groupVars, &GraphVar{
			Name:      x.AnonymousName,
			Anonymous: true,
		})
		return vlp, nil
	default:
		return nil, fmt.Errorf("unsupported ast.VertexPairConnection(%T) in simple path pattern", x)
	}
}

func (s *SubgraphBuilder) buildVariableLengthPath(
	pathTp ast.PathPatternType, topK int64, astConn ast.VertexPairConnection,
) (VertexPairConnection, error) {
	x, ok := astConn.(*ast.QuantifiedPathExpr)
	if !ok {
		return nil, fmt.Errorf("unsupported ast.VertexPairConnection(%T) in variable-length path pattern", astConn)
	}
	varName := x.Edge.Variable.Name
	labels := x.Edge.Variable.Labels

	conn, err := s.buildConnWithCpe(varName, labels)
	if err != nil {
		return nil, err
	}
	vlp := &VariableLengthPath{Conn: conn}

	switch pathTp {
	case ast.PathPatternAny, ast.PathPatternAnyShortest:
		vlp.Goal = PathFindingShortest
		vlp.TopK = 1
	case ast.PathPatternAllShortest:
		vlp.Goal = PathFindingShortest
		vlp.WithTies = true
	case ast.PathPatternTopKShortest:
		vlp.Goal = PathFindingShortest
		vlp.TopK = topK
	case ast.PathPatternAnyCheapest:
		vlp.Goal = PathFindingCheapest
		vlp.TopK = 1
	case ast.PathPatternAllCheapest:
		vlp.Goal = PathFindingCheapest
		vlp.WithTies = true
	case ast.PathPatternTopKCheapest:
		vlp.Goal = PathFindingCheapest
		vlp.TopK = topK
	case ast.PathPatternAll:
		vlp.Goal = PathFindingAll
	}
	if x.Quantifier != nil {
		vlp.MinHops = x.Quantifier.N
		vlp.MaxHops = x.Quantifier.M
	} else {
		vlp.MinHops = 1
		vlp.MaxHops = 1
	}
	if x.Quantifier != nil {
		vlp.MinHops = x.Quantifier.N
		vlp.MaxHops = x.Quantifier.M
	} else {
		vlp.MinHops = 1
		vlp.MaxHops = 1
	}
	vlp.Constraints = x.Where
	vlp.Cost = x.Cost

	var hopSrcVar, hopDstVar *ast.VariableSpec
	if x.Source != nil {
		hopSrcVar = x.Source.Variable
	}
	if x.Destination != nil {
		hopDstVar = x.Destination.Variable
	}
	if x.Edge.Direction == ast.EdgeDirectionIncoming {
		hopSrcVar, hopDstVar = hopDstVar, hopSrcVar
	}
	if hopSrcVar != nil {
		vlp.HopSrc = s.buildVertex(hopSrcVar)
		s.groupVars = append(s.groupVars, &GraphVar{
			Name:      hopSrcVar.Name,
			Anonymous: hopSrcVar.Anonymous,
		})
	}
	if hopDstVar != nil {
		vlp.HopDst = s.buildVertex(hopDstVar)
		s.groupVars = append(s.groupVars, &GraphVar{
			Name:      hopDstVar.Name,
			Anonymous: hopDstVar.Anonymous,
		})
	}
	s.groupVars = append(s.groupVars, &GraphVar{
		Name:      varName,
		Anonymous: x.Edge.Variable.Anonymous,
	})
	return vlp, nil
}

// buildConnWithCpe builds a single connection with a CPE. If label and CPE have the same name, the CPE will be used.
// Currently, reference multiple CPEs or mix CPEs with normal labels is not supported.
func (s *SubgraphBuilder) buildConnWithCpe(varName model.CIStr, labelOrCpeNames []model.CIStr) (VertexPairConnection, error) {
	edge := &Edge{}
	edge.SetName(varName)
	if len(labelOrCpeNames) == 0 {
		edge.Labels = s.graph.Labels()
		return edge, nil
	}

	for _, name := range labelOrCpeNames {
		if cpe, ok := s.cpes[name.L]; ok {
			if len(labelOrCpeNames) != 1 {
				return nil, fmt.Errorf("reference multiple CPEs or mix CPEs with normal labels is not supported")
			}
			// TODO: clone the CPE to avoid modifying the original one.
			return cpe, nil
		}
		edge.Labels = append(edge.Labels, s.graph.Label(name.L))
	}
	return edge, nil
}

func resolveAndSrcDstVarName(
	leftVarName, rightVarName model.CIStr, direction ast.EdgeDirection,
) (srcVarName, dstVarName model.CIStr, anyDirected bool, err error) {
	switch direction {
	case ast.EdgeDirectionOutgoing:
		srcVarName = leftVarName
		dstVarName = rightVarName
	case ast.EdgeDirectionIncoming:
		srcVarName = rightVarName
		dstVarName = leftVarName
	case ast.EdgeDirectionAnyDirected:
		srcVarName = leftVarName
		dstVarName = rightVarName
		anyDirected = true
	default:
		err = fmt.Errorf("unsupported edge direction: %v", direction)
	}
	return
}

func extractConnNameAndDirection(conn ast.VertexPairConnection) (model.CIStr, ast.EdgeDirection, error) {
	switch x := conn.(type) {
	case *ast.EdgePattern:
		return x.Variable.Name, x.Direction, nil
	case *ast.ReachabilityPathExpr:
		return x.AnonymousName, x.Direction, nil
	case *ast.QuantifiedPathExpr:
		return x.Edge.Variable.Name, x.Edge.Direction, nil
	default:
		return model.CIStr{}, 0, fmt.Errorf("unsupported ast.VertexPairConnection(%T)", x)
	}
}
