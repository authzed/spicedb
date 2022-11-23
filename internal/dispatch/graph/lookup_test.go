package graph

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func RR(namespaceName string, relationName string) *core.RelationReference {
	return &core.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
	}
}

func resolvedRes(resourceID string) *v1.ResolvedResource {
	return &v1.ResolvedResource{
		ResourceId:     resourceID,
		Permissionship: v1.ResolvedResource_HAS_PERMISSION,
	}
}

func TestSimpleLookup(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	testCases := []struct {
		start                 *core.RelationReference
		target                *core.ObjectAndRelation
		expectedResources     []*v1.ResolvedResource
		expectedDispatchCount int
		expectedDepthRequired int
	}{
		{
			RR("document", "view"),
			ONR("user", "unknown", "..."),
			[]*v1.ResolvedResource{},
			1,
			1,
		},
		{
			RR("document", "view"),
			ONR("user", "eng_lead", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("masterplan"),
			},
			2,
			2,
		},
		{
			RR("document", "owner"),
			ONR("user", "product_manager", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("masterplan"),
			},
			2,
			1,
		},
		{
			RR("document", "view"),
			ONR("user", "legal", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("companyplan"),
				resolvedRes("masterplan"),
			},
			6,
			4,
		},
		{
			RR("document", "view_and_edit"),
			ONR("user", "multiroleguy", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("specialplan"),
			},
			7,
			4,
		},
		{
			RR("folder", "view"),
			ONR("user", "owner", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("strategy"),
				resolvedRes("company"),
			},
			8,
			5,
		},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"%s#%s->%s",
			tc.start.Namespace,
			tc.start.Relation,
			tuple.StringONR(tc.target),
		)

		t.Run(name, func(t *testing.T) {
			require := require.New(t)
			ctx, dispatch, revision := newLocalDispatcher(t)

			lookupResult, err := dispatch.DispatchLookup(ctx, &v1.DispatchLookupRequest{
				ObjectRelation: tc.start,
				Subject:        tc.target,
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				Limit: 10,
			})

			require.NoError(err)
			require.ElementsMatch(tc.expectedResources, lookupResult.ResolvedResources, "Found: %v, Expected: %v", lookupResult.ResolvedResources, tc.expectedResources)
			require.GreaterOrEqual(lookupResult.Metadata.DepthRequired, uint32(1))
			require.LessOrEqual(int(lookupResult.Metadata.DispatchCount), tc.expectedDispatchCount, "Found dispatch count greater than expected")
			require.Equal(0, int(lookupResult.Metadata.CachedDispatchCount))
			require.Equal(tc.expectedDepthRequired, int(lookupResult.Metadata.DepthRequired), "Depth required mismatch")

			// We have to sleep a while to let the cache converge:
			// https://github.com/outcaste-io/ristretto/blob/01b9f37dd0fd453225e042d6f3a27cd14f252cd0/cache_test.go#L17
			time.Sleep(10 * time.Millisecond)

			// Run again with the cache available.
			lookupResult, err = dispatch.DispatchLookup(context.Background(), &v1.DispatchLookupRequest{
				ObjectRelation: tc.start,
				Subject:        tc.target,
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				Limit: 10,
			})

			require.NoError(err)
			require.ElementsMatch(tc.expectedResources, lookupResult.ResolvedResources, "Found: %v, Expected: %v", lookupResult.ResolvedResources, tc.expectedResources)
			require.GreaterOrEqual(lookupResult.Metadata.DepthRequired, uint32(1))
			require.Equal(0, int(lookupResult.Metadata.DispatchCount))
			require.LessOrEqual(int(lookupResult.Metadata.CachedDispatchCount), tc.expectedDispatchCount)
			require.Equal(tc.expectedDepthRequired, int(lookupResult.Metadata.DepthRequired))
		})
	}
}

func TestMaxDepthLookup(t *testing.T) {
	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatch := NewLocalOnlyDispatcher(10)
	ctx := datastoremw.ContextWithHandle(context.Background())
	require.NoError(datastoremw.SetInContext(ctx, ds))

	_, err = dispatch.DispatchLookup(ctx, &v1.DispatchLookupRequest{
		ObjectRelation: RR("document", "view"),
		Subject:        ONR("user", "legal", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 0,
		},
		Limit: 10,
	})

	require.Error(err)
}

type OrderedResolved []*v1.ResolvedResource

func (a OrderedResolved) Len() int { return len(a) }

func (a OrderedResolved) Less(i, j int) bool {
	return strings.Compare(a[i].ResourceId, a[j].ResourceId) < 0
}

func (a OrderedResolved) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
