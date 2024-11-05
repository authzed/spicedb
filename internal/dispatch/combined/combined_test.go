package combined

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestCombinedRecursiveCall(t *testing.T) {
	dispatcher, err := NewDispatcher()
	require.NoError(t, err)

	t.Cleanup(func() { dispatcher.Close() })

	ctx := datastoremw.ContextWithHandle(context.Background())

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
		definition user {}

		definition resource {
			relation viewer: resource#viewer | user
			permission view = viewer
		}
	`, []tuple.Relationship{
		tuple.MustParse("resource:someresource#viewer@resource:someresource#viewer"),
	}, require.New(t))

	require.NoError(t, datastoremw.SetInContext(ctx, ds))

	_, err = dispatcher.DispatchCheck(ctx, &dispatchv1.DispatchCheckRequest{
		ResourceRelation: &core.RelationReference{
			Namespace: "resource",
			Relation:  "view",
		},
		ResourceIds: []string{"someresource"},
		Subject: &core.ObjectAndRelation{
			Namespace: "user",
			ObjectId:  "fred",
			Relation:  tuple.Ellipsis,
		},
		ResultsSetting: dispatchv1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
		Metadata: &dispatchv1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "max depth exceeded")
}
