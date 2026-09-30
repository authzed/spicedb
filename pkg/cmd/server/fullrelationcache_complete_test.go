package server

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/datastore"
)

// hasFullRelationCacheProxy reports whether the datastore chain contains the full relation cache proxy.
func hasFullRelationCacheProxy(ds datastore.Datastore) bool {
	for ds != nil {
		if fmt.Sprintf("%T", ds) == "*fullrelationcache.proxy" {
			return true
		}
		unwrappable, ok := ds.(datastore.UnwrappableDatastore)
		if !ok {
			return false
		}
		ds = unwrappable.Unwrap()
	}
	return false
}

func TestCompleteFullRelationCache(t *testing.T) {
	const (
		inactiveWarning   = "the full relation cache is inactive"
		overBudgetWarning = "sum to more than 100%"
	)

	tcs := []struct {
		name            string
		mode            FullRelationCacheMode
		cacheConfig     CacheConfig
		wantProxy       bool
		wantInactiveLog bool
		wantOverLog     bool
	}{
		{
			name:        "full relation cache off",
			mode:        FullRelationCacheDisabled,
			cacheConfig: CacheConfig{Name: "full_relation", Enabled: true, MaxCost: "10%"},
		},
		{
			name:        "active",
			mode:        FullRelationCacheEnabled,
			cacheConfig: CacheConfig{Name: "full_relation", Enabled: true, MaxCost: "10%"},
			wantProxy:   true,
		},
		{
			name:            "cache disabled",
			mode:            FullRelationCacheEnabled,
			cacheConfig:     CacheConfig{Name: "full_relation", Enabled: false, MaxCost: "10%"},
			wantInactiveLog: true,
		},
		{
			name:            "zero percent max cost",
			mode:            FullRelationCacheEnabled,
			cacheConfig:     CacheConfig{Name: "full_relation", Enabled: true, MaxCost: "0%"},
			wantInactiveLog: true,
		},
		{
			name:            "zero max cost",
			mode:            FullRelationCacheEnabled,
			cacheConfig:     CacheConfig{Name: "full_relation", Enabled: true, MaxCost: "0"},
			wantInactiveLog: true,
		},
		{
			name:        "budgets over 100 percent",
			mode:        FullRelationCacheEnabled,
			cacheConfig: CacheConfig{Name: "full_relation", Enabled: true, MaxCost: "90%"},
			wantProxy:   true,
			wantOverLog: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			ctx := zerolog.New(&logs).Level(zerolog.WarnLevel).WithContext(t.Context())

			ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 1*time.Second, 10*time.Second)
			require.NoError(t, err)

			c := ConfigWithOptions(
				&Config{},
				WithPresharedSecureKey("psk"),
				WithDatastore(ds),
				WithGRPCServer(util.GRPCServerConfig{
					Network: util.BufferedNetwork,
					Enabled: true,
				}),
				WithNamespaceCacheConfig(CacheConfig{Enabled: true}),
				WithDispatchCacheConfig(CacheConfig{Enabled: true, MaxCost: "30%"}),
				WithClusterDispatchCacheConfig(CacheConfig{Enabled: true}),
				WithMetricsAPI(util.HTTPServerConfig{HTTPEnabled: false}),
				WithEnableMemoryProtectionMiddleware(false),
				WithExperimentalFullRelationCache(string(tc.mode)),
				WithFullRelationCacheConfig(tc.cacheConfig),
			)

			completed, err := c.complete(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, completed.closeFunc()) })

			require.Equal(t, tc.wantProxy, hasFullRelationCacheProxy(completed.ds))
			require.Equal(t, tc.wantInactiveLog, bytes.Contains(logs.Bytes(), []byte(inactiveWarning)))
			require.Equal(t, tc.wantOverLog, bytes.Contains(logs.Bytes(), []byte(overBudgetWarning)))
		})
	}
}
