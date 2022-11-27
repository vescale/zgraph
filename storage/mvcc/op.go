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

package mvcc

type Op int32

const (
	Op_Put      Op = 0
	Op_Del      Op = 1
	Op_Lock     Op = 2
	Op_Rollback Op = 3
	// insert operation has a constraint that key should not exist before.
	Op_Insert         Op = 4
	Op_CheckNotExists Op = 5
)
