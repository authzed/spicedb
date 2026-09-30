package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
)

func TestFullRelationCacheConfigDefaults(t *testing.T) {
	cfg := NewConfigWithOptionsAndDefaults()
	require.Equal(t, "disabled", cfg.ExperimentalFullRelationCache)
	require.Equal(t, "full_relation", cfg.FullRelationCacheConfig.Name)
	require.Equal(t, "30%", cfg.FullRelationCacheConfig.MaxCost)
	require.Equal(t, uint64(3), cfg.FullRelationCacheMaterializeThreshold)
	require.Equal(t, uint64(1024), cfg.FullRelationCacheMaximumSetSize)
}

func TestParseFullRelationCacheMode(t *testing.T) {
	mode, err := ParseFullRelationCacheMode("disabled")
	require.NoError(t, err)
	require.Equal(t, FullRelationCacheDisabled, mode)

	mode, err = ParseFullRelationCacheMode("enabled")
	require.NoError(t, err)
	require.Equal(t, FullRelationCacheEnabled, mode)

	for _, invalid := range []string{"", "true", "Enabled", "on"} {
		_, err = ParseFullRelationCacheMode(invalid)
		require.ErrorContains(t, err, "must be one of: disabled, enabled", invalid)
	}
}

func TestCompleteRejectsInvalidFullRelationCacheMode(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, 0)
	require.NoError(t, err)

	c := ConfigWithOptions(
		&Config{},
		WithPresharedSecureKey("psk"),
		WithDatastore(ds),
		WithExperimentalFullRelationCache("true"),
	)
	_, err = c.complete(t.Context())
	require.ErrorContains(t, err, `invalid full relation cache mode "true"`)
}
