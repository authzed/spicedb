//go:build memoryprotection

package memoryprotection

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/middleware/memoryprotection/rtml"
	"github.com/authzed/spicedb/pkg/cmd/server"
)

func init() {
	server.DefaultMemoryUsageProvider = rtml.NewRealTimeMemoryUsageProvider()
}

func TestBuildMemoryProtectionConfig(t *testing.T) {
	testcases := map[string]struct {
		config       *server.Config
		expectedErr  string
		expectedType string
	}{
		`disabled`: {
			config: &server.Config{
				MemoryProtectionEnabled: false,
			},
			expectedType: "*memoryprotection.HarcodedMemoryLimitProvider",
		},
		`enabled`: {
			config: &server.Config{
				MemoryProtectionEnabled: true,
			},
			expectedType: "*rtml.GoRealTimeMemoryLimiter",
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			memoryUsageProvider := tc.config.BuildMemoryUsageProvider()
			require.NotNil(t, memoryUsageProvider)
			require.Equal(t, tc.expectedType, reflect.TypeOf(memoryUsageProvider).String())
		})
	}
}
