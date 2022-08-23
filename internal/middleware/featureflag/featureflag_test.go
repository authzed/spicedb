package featureflag

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFeatureFlags(t *testing.T) {
	ctx := context.Background()
	require.False(t, FromContext(ctx, "foo"))
	ctx = WithFlag(ctx, "foo", true)
	require.True(t, FromContext(ctx, "foo"))
}
