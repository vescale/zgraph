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

package tests_test

import "testing"

// An example in https://pgql-lang.org/spec/1.5/#edge-patterns.
var initStudentNetwork = []string{
	`CREATE GRAPH student_network`,
	`USE student_network`,
	`CREATE LABEL Person`,
	`CREATE LABEL University`,
	`CREATE LABEL knows`,
	`CREATE LABEL studentOf`,
	`INSERT VERTEX x LABELS (Person) PROPERTIES (x.name = 'Kathrine', x.dob = DATE '1994-01-15')`,
	`INSERT VERTEX x LABELS (Person) PROPERTIES (x.name = 'Riya', x.dob = DATE '1995-03-20')`,
	`INSERT VERTEX x LABELS (Person) PROPERTIES (x.name = 'Lee', x.dob = DATE '1996-01-20')`,
	`INSERT VERTEX x LABELS (University) PROPERTIES (x.name = 'UC Berkeley')`,
	`INSERT EDGE e BETWEEN x AND y LABELS ( knows ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Kathrine' AND y.name = 'Lee'`,
	`INSERT EDGE e BETWEEN x AND y LABELS ( knows ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Kathrine' AND y.name = 'Riya'`,
	`INSERT EDGE e BETWEEN x AND y LABELS ( knows ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Lee' AND y.name = 'Kathrine'`,
	`INSERT EDGE e BETWEEN x AND y LABELS ( studentOf ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Kathrine' AND y.name = 'UC Berkeley'`,
	`INSERT EDGE e BETWEEN x AND y LABELS ( studentOf ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Riya' AND y.name = 'UC Berkeley'`,
	`INSERT EDGE e BETWEEN x AND y LABELS ( studentOf ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Lee' AND y.name = 'UC Berkeley'`,
}

func TestStudentNetwork(t *testing.T) {
	runDataDrivenTest(t, "testdata/student_network", initStudentNetwork)
}
