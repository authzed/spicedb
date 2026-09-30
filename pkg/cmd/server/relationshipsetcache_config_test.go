package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelationshipSetCacheConfigDefaults(t *testing.T) {
	cfg := NewConfigWithOptionsAndDefaults()
	require.False(t, cfg.EnableExperimentalRelationshipSetCache)
	require.Equal(t, "relationship_set", cfg.RelationshipSetCacheConfig.Name)
	require.Equal(t, "30%", cfg.RelationshipSetCacheConfig.MaxCost)
	require.Equal(t, uint64(3), cfg.RelationshipSetCacheMaterializeThreshold)
	require.Equal(t, uint64(1024), cfg.RelationshipSetCacheMaximumSetSize)
}
