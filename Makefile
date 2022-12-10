# Copyright 2022 zGraph Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include build/Makefile.env

.PHONY: parser goyacc

parser: tools/bin/goyacc
	@echo "bin/goyacc -o parser/parser.y.go"
	@tools/bin/goyacc -o parser/parser.y.go parser/parser.y

fmt:
	tools/bin/goimports -w $(FILES)

test:
	$(GO) test -v $(PACKAGES)

tools/bin/goyacc:
	$(GO) build -o tools/bin/goyacc ./parser/goyacc/

tools/bin/goimports:
	cd tools && $(GO) build -o ./bin/goimports golang.org/x/tools/cmd/goimports
