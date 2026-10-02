package frequency

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

// BenchmarkEstimatorFalsePositives runs one window of skewed traffic at the 8 MiB budget.
// Each cold key gets 2 touches, so a reading of 3 or more is a false positive.
// Spread requires a count of at least spreadFloor before its share test runs.
// The benchmark reports three fractions of cold keys:
//   - touch-fp: some Touch of the key returned 3 or more.
//   - final-fp: the estimate at the end of the window is 3 or more.
//   - floor-fp: some Touch of the key returned spreadFloor or more, so the key reached the share test.
func BenchmarkEstimatorFalsePositives(b *testing.B) {
	const (
		budget      = 8 << 20
		hotKeys     = 10
		hotTouches  = 1000
		coldTouches = 2
		fpThreshold = 3
		// spreadFloor is minSpreadCount in internal/dispatch/remote.
		spreadFloor = 100
	)
	for _, cold := range []int{10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("cold=%d", cold), func(b *testing.B) {
			keys := make([]string, hotKeys+cold)
			for i := range keys {
				keys[i] = fmt.Sprintf("document:%d#viewer@1700000000000000000", i)
			}
			order := make([]int, 0, hotKeys*hotTouches+cold*coldTouches)
			for i := range hotKeys {
				for range hotTouches {
					order = append(order, i)
				}
			}
			for i := hotKeys; i < len(keys); i++ {
				for range coldTouches {
					order = append(order, i)
				}
			}
			// nolint:gosec
			// G404: the workload needs a fixed seed, not cryptographic randomness.
			rng := rand.New(rand.NewPCG(1, 2))
			rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })

			maxSeen := make([]uint64, len(keys))
			var touchFP, finalFP, floorFP int
			b.ResetTimer()
			for b.Loop() {
				s := newSketch(budget, nil)
				clear(maxSeen)
				for _, k := range order {
					maxSeen[k] = max(maxSeen[k], s.Touch(keys[k]))
				}
				touchFP, finalFP, floorFP = 0, 0, 0
				for i := hotKeys; i < len(keys); i++ {
					if maxSeen[i] >= fpThreshold {
						touchFP++
					}
					if s.estimate(keys[i]) >= fpThreshold {
						finalFP++
					}
					if maxSeen[i] >= spreadFloor {
						floorFP++
					}
				}
				s.Close()
			}
			b.ReportMetric(float64(touchFP)/float64(cold), "touch-fp")
			b.ReportMetric(float64(finalFP)/float64(cold), "final-fp")
			b.ReportMetric(float64(floorFP)/float64(cold), "floor-fp")
			b.ReportMetric(float64(len(order)), "touches/op")
		})
	}
}
