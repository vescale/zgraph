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

import "fmt"

// LockedError is returned when trying to Read/Write on a locked key. Caller should
// backoff or cleanup the lock then retry.
type LockedError struct {
	Key      Key
	Primary  []byte
	StartVer Version
	TTL      uint64
}

// Error formats the lock to a string.
func (e *LockedError) Error() string {
	return fmt.Sprintf("key is locked, key: %q, primary: %q, startVer: %v",
		e.Key, e.Primary, e.StartVer)
}
