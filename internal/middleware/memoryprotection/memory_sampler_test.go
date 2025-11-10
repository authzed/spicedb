package memoryprotection

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMemorySamplerOnInterval(t *testing.T) {
	intervalSeconds := 1000
	interval := time.Duration(intervalSeconds) * time.Second

	testcases := map[string]struct {
		limit                     int64
		expectGaugeAndUsageUpdate bool
	}{
		`positive_limit`: {
			limit:                     100 * 1024 * 1024,
			expectGaugeAndUsageUpdate: true,
		},
		`very_low_limit`: {
			limit:                     1,
			expectGaugeAndUsageUpdate: true,
		},
		`negative_limit`: {
			limit:                     -1,
			expectGaugeAndUsageUpdate: false,
		},
		`zero`: {
			limit:                     0,
			expectGaugeAndUsageUpdate: false,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sampler := NewMemorySamplerOnInterval(intervalSeconds, &HardCodedMemoryLimitProvider{Hardcodedlimit: tc.limit})
				t.Cleanup(sampler.Close)

				now := time.Now()
				time.Sleep(interval - 1*time.Millisecond)
				synctest.Wait()
				t.Log("When we get here, the sampling is guaranteed to NOT have run yet")

				require.False(t, sampler.GetTimestampLastMemorySample().After(now))

				time.Sleep(1 * time.Millisecond)
				synctest.Wait()
				t.Log("When we get here, the sampling is guaranteed to have run")

				require.True(t, sampler.GetTimestampLastMemorySample().After(now))

				gaugeValue := testutil.ToFloat64(MemoryUsageGauge)
				if tc.expectGaugeAndUsageUpdate {
					require.Greater(t, gaugeValue, float64(0))
					require.Greater(t, sampler.GetMemoryUsagePercent(), float64(0))
					require.LessOrEqual(t, sampler.GetMemoryUsagePercent(), float64(1), "percentage should be between 0 and 1")
				} else {
					require.InDelta(t, 0, gaugeValue, 0.001) // near zero
				}
			})
		})
	}
}
