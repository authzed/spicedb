package graph

import (
	"context"
	"testing"

	"github.com/authzed/spicedb/internal/graph"

	"github.com/authzed/grpcutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnwrapStatusError(t *testing.T) {
	t.Parallel()

	err := rewriteError(context.Background(), graph.NewCheckFailureErr(status.Error(codes.Canceled, "canceled")))
	grpcutil.RequireStatus(t, codes.Canceled, err)
}

func TestConcurrencyLimitsWithOverallDefaultLimit(t *testing.T) {
	t.Parallel()
	cl := ConcurrencyLimits{}
	require.Equal(t, uint16(0), cl.Check)
	require.Equal(t, uint16(0), cl.LookupResources)
	require.Equal(t, uint16(0), cl.LookupSubjects)
	require.Equal(t, uint16(0), cl.ReachableResources)

	withDefaults := cl.WithOverallDefaultLimit(51)

	require.Equal(t, uint16(0), cl.Check)
	require.Equal(t, uint16(0), cl.LookupResources)
	require.Equal(t, uint16(0), cl.LookupSubjects)
	require.Equal(t, uint16(0), cl.ReachableResources)

	require.Equal(t, uint16(51), withDefaults.Check)
	require.Equal(t, uint16(51), withDefaults.LookupResources)
	require.Equal(t, uint16(51), withDefaults.LookupSubjects)
	require.Equal(t, uint16(51), withDefaults.ReachableResources)
}

func TestSharedConcurrencyLimits(t *testing.T) {
	t.Parallel()
	cl := SharedConcurrencyLimits(42)
	require.Equal(t, uint16(42), cl.Check)
	require.Equal(t, uint16(42), cl.LookupResources)
	require.Equal(t, uint16(42), cl.LookupSubjects)
	require.Equal(t, uint16(42), cl.ReachableResources)

	withDefaults := cl.WithOverallDefaultLimit(51)

	require.Equal(t, uint16(42), cl.Check)
	require.Equal(t, uint16(42), cl.LookupResources)
	require.Equal(t, uint16(42), cl.LookupSubjects)
	require.Equal(t, uint16(42), cl.ReachableResources)

	require.Equal(t, uint16(42), withDefaults.Check)
	require.Equal(t, uint16(42), withDefaults.LookupResources)
	require.Equal(t, uint16(42), withDefaults.LookupSubjects)
	require.Equal(t, uint16(42), withDefaults.ReachableResources)
}
