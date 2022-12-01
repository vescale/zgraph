package storage

import (
	"time"

	"github.com/cenkalti/backoff"
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
