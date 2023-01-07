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

import "errors"

var (
	ErrFunctionNotExists       = errors.New("function not exists")
	ErrFunctionNotImplemented  = errors.New("function not implemented")
	ErrIncorrectParameterCount = errors.New("incorrect parameter count")
	ErrInvalidOp               = errors.New("invalid operation")
	ErrOverflow                = errors.New("overflow")
	ErrDivByZero               = errors.New("division by zero")
	ErrNoLabel                 = errors.New("no label")
	ErrLabelMoreThanOne        = errors.New("label more than one")
)
