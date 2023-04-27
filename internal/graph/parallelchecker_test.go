package graph

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func TestParallelCheckerDirectOverload(t *testing.T) {
	pc := newParallelChecker(context.Background(), func() {}, nil, ValidatedLookupRequest{
		DispatchLookupRequest: &v1.DispatchLookupResourcesRequest{
			Limit: 50,
		},
	}, 10)

	// Add a conditional item and ensure it is added.
	pc.addResultsUnsafe(&v1.ResolvedResource{
		ResourceId:     "foo",
		Permissionship: v1.ResolvedResource_CONDITIONALLY_HAS_PERMISSION,
	})

	require.Equal(t, v1.ResolvedResource_CONDITIONALLY_HAS_PERMISSION, pc.foundResourceIDs["foo"].Permissionship)

	// Add a concrete item and ensure it overloads.
	pc.addResultsUnsafe(&v1.ResolvedResource{
		ResourceId:     "foo",
		Permissionship: v1.ResolvedResource_HAS_PERMISSION,
	})

	require.Equal(t, v1.ResolvedResource_HAS_PERMISSION, pc.foundResourceIDs["foo"].Permissionship)

	// Add a conditional item and ensure it is ignored.
	pc.addResultsUnsafe(&v1.ResolvedResource{
		ResourceId:     "foo",
		Permissionship: v1.ResolvedResource_CONDITIONALLY_HAS_PERMISSION,
	})

	require.Equal(t, v1.ResolvedResource_HAS_PERMISSION, pc.foundResourceIDs["foo"].Permissionship)
}

func TestQueueToCheckLimit(t *testing.T) {
	pc := newParallelChecker(context.Background(), func() {}, nil, ValidatedLookupRequest{
		DispatchLookupRequest: &v1.DispatchLookupResourcesRequest{
			Limit: 1,
		},
	}, 10)

	pc.addResultsUnsafe(&v1.ResolvedResource{
		ResourceId:     "foo",
		Permissionship: v1.ResolvedResource_HAS_PERMISSION,
	})

	// Queue a second and ensure it is ignored.
	require.False(t, pc.QueueToCheck("bar"))
}
