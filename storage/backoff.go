package storage

import (
	"time"

	"github.com/cenkalti/backoff"
	"github.com/vescale/zgraph/internal/logutil"
)

func expoBackoff() backoff.BackOff {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     50 * time.Millisecond,
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
