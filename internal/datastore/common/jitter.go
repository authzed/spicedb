package common

import (
	"math"
	"math/rand"
	"time"
)

// WithJitter adjusts an interval by a random amount within some factor of the
// original interval. Intervals near half of max int64 can return durations
// outside the jitter factor.
func WithJitter(factor float64, interval time.Duration) time.Duration {
	if factor < 0.0 || factor > 1.0 {
		return interval
	}
	return time.Duration(math.Abs((1 - factor + 2*rand.Float64()*factor) * float64(interval)))
}
