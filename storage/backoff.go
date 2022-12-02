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

package storage

import (
	"time"

	"github.com/cenkalti/backoff"
	"github.com/vescale/zgraph/internal/logutil"
)

func expoBackoff() backoff.BackOff {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     10 * time.Millisecond,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          backoff.DefaultMultiplier,
		MaxInterval:         5 * time.Second,
		MaxElapsedTime:      20 * time.Second,
		Clock:               backoff.SystemClock,
	}
	b.Reset()
	return b
}

// BackoffErrReporter reports backoff error events.
func BackoffErrReporter(site string) backoff.Notify {
	return func(err error, dur time.Duration) {
		logutil.Infof("Backoff in [%s](retry in %s) caused by: %+v", site, dur, err)
	}
}
