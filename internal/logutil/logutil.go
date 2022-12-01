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

package logutil

import (
	"fmt"
	"log"
)

var g Logger = &defaultLogger{}

type Logger interface {
	Infof(msg string, args ...interface{})
	Fatalf(msg string, args ...interface{})
}

// SetLogger replaces the default logger
func SetLogger(l Logger) {
	g = l
}

// L returns the default global logger
func L() Logger {
	return g
}

type defaultLogger struct{}

func (defaultLogger) Infof(msg string, args ...interface{}) {
	_ = log.Output(2, fmt.Sprintf(msg, args...))
}

func (defaultLogger) Fatalf(msg string, args ...interface{}) {
	_ = log.Output(2, fmt.Sprintf(msg, args...))
}
