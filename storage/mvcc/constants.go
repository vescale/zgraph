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

import "math"

// Key layout:
// ...
// Key_lock        -- (0)
// Key_verMax      -- (1)
// ...
// Key_ver+1       -- (2)
// Key_ver         -- (3)
// Key_ver-1       -- (4)
// ...
// Key_0           -- (5)
// NextKey_lock    -- (6)
// NextKey_verMax  -- (7)
// ...
// NextKey_ver+1   -- (8)
// NextKey_ver     -- (9)
// NextKey_ver-1   -- (10)
// ...
// NextKey_0       -- (11)
// ...
// EOF

const LockVer = math.MaxUint64
