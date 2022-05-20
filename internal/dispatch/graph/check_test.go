package graph

import (
	"context"
	"fmt"
	"os"
	"testing"

	v1_api "github.com/authzed/authzed-go/proto/authzed/api/v1"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/internal/graph"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})

	// Set this to Trace to dump log statements in tests.
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

var ONR = tuple.ObjectAndRelation

func TestSimpleCheck(t *testing.T) {
	type expected struct {
		relation string
		isMember bool
	}

	type userset struct {
		userset  *core.ObjectAndRelation
		expected []expected
	}

	testCases := []struct {
		namespace string
		objectID  string
		usersets  []userset
	}{
		{"document", "masterplan", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
		}},
		{"document", "healthplan", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
		}},
		{"folder", "company", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("folder", "auditors", "viewer"), []expected{{"viewer", true}}},
		}},
		{"folder", "strategy", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("folder", "company", graph.Ellipsis), []expected{{"parent", true}}},
		}},
		{"folder", "isolated", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
		}},
	}

	for _, tc := range testCases {
		for _, userset := range tc.usersets {
			for _, expected := range userset.expected {
				name := fmt.Sprintf(
					"simple:%s:%s#%s@%s:%s#%s=>%t",
					tc.namespace,
					tc.objectID,
					expected.relation,
					userset.userset.Namespace,
					userset.userset.ObjectId,
					userset.userset.Relation,
					expected.isMember,
				)

				t.Run(name, func(t *testing.T) {
					require := require.New(t)

					ctx, dispatch, revision := newLocalDispatcher(require)

					checkResult, err := dispatch.DispatchCheck(ctx, &v1.DispatchCheckRequest{
						ObjectAndRelation: ONR(tc.namespace, tc.objectID, expected.relation),
						Subject:           userset.userset,
						Metadata: &v1.ResolverMeta{
							AtRevision:     revision.String(),
							DepthRemaining: 50,
						},
					})

					require.NoError(err)
					require.Equal(expected.isMember, checkResult.Membership == v1.DispatchCheckResponse_MEMBER)
					require.GreaterOrEqual(checkResult.Metadata.DepthRequired, uint32(1))
				})
			}
		}
	}
}

func TestMaxDepth(t *testing.T) {
	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)

	mutations := []*v1_api.RelationshipUpdate{{
		Operation: v1_api.RelationshipUpdate_OPERATION_CREATE,
		Relationship: &v1_api.Relationship{
			Resource: &v1_api.ObjectReference{
				ObjectType: "folder",
				ObjectId:   "oops",
			},
			Relation: "owner",
			Subject: &v1_api.SubjectReference{
				Object: &v1_api.ObjectReference{
					ObjectType: "folder",
					ObjectId:   "oops",
				},
				OptionalRelation: "editor",
			},
		},
	}}

	ctx := datastoremw.ContextWithHandle(context.Background())
	require.NoError(datastoremw.SetInContext(ctx, ds))

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(mutations)
	})
	require.NoError(err)
	require.True(revision.GreaterThan(decimal.Zero))

	dispatch := NewLocalOnlyDispatcher()

	checkResult, err := dispatch.DispatchCheck(ctx, &v1.DispatchCheckRequest{
		ObjectAndRelation: ONR("folder", "oops", "owner"),
		Subject:           ONR("user", "fake", graph.Ellipsis),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
	})

	require.Error(err)
	require.Equal(v1.DispatchCheckResponse_UNKNOWN, checkResult.Membership)
}

func TestCheckMetadata(t *testing.T) {
	type expected struct {
		relation              string
		isMember              bool
		expectedDispatchCount int
		expectedDepthRequired int
	}

	type userset struct {
		userset  *core.ObjectAndRelation
		expected []expected
	}

	testCases := []struct {
		namespace string
		objectID  string
		usersets  []userset
	}{
		{"document", "masterplan", []userset{
			{
				ONR("user", "product_manager", graph.Ellipsis),
				[]expected{
					{"owner", true, 1, 1},
					{"editor", true, 2, 2},
					{"viewer", true, 3, 3},
				},
			},
			{
				ONR("user", "owner", graph.Ellipsis),
				[]expected{
					{"owner", false, 1, 1},
					{"editor", false, 2, 2},
					{"viewer", true, 5, 5},
				},
			},
		}},
		{"folder", "strategy", []userset{
			{
				ONR("user", "vp_product", graph.Ellipsis),
				[]expected{
					{"owner", true, 1, 1},
					{"editor", true, 2, 2},
					{"viewer", true, 3, 3},
				},
			},
		}},
		{"folder", "company", []userset{
			{
				ONR("user", "unknown", graph.Ellipsis),
				[]expected{
					{"viewer", false, 6, 4},
				},
			},
		}},
	}

	for _, tc := range testCases {
		for _, userset := range tc.usersets {
			for _, expected := range userset.expected {
				name := fmt.Sprintf(
					"metadata:%s:%s#%s@%s:%s#%s=>%t",
					tc.namespace,
					tc.objectID,
					expected.relation,
					userset.userset.Namespace,
					userset.userset.ObjectId,
					userset.userset.Relation,
					expected.isMember,
				)

				t.Run(name, func(t *testing.T) {
					require := require.New(t)

					ctx, dispatch, revision := newLocalDispatcher(require)

					checkResult, err := dispatch.DispatchCheck(ctx, &v1.DispatchCheckRequest{
						ObjectAndRelation: ONR(tc.namespace, tc.objectID, expected.relation),
						Subject:           userset.userset,
						Metadata: &v1.ResolverMeta{
							AtRevision:     revision.String(),
							DepthRemaining: 50,
						},
					})

					require.NoError(err)
					require.Equal(expected.isMember, checkResult.Membership == v1.DispatchCheckResponse_MEMBER)
					require.Equal(expected.expectedDispatchCount, int(checkResult.Metadata.DispatchCount))
					require.Equal(expected.expectedDepthRequired, int(checkResult.Metadata.DepthRequired))
				})
			}
		}
	}
}

func newLocalDispatcher(require *require.Assertions) (context.Context, dispatch.Dispatcher, decimal.Decimal) {
	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatch := NewLocalOnlyDispatcher()

	cachingDispatcher, err := caching.NewCachingDispatcher(nil, "", &keys.CanonicalKeyHandler{})
	cachingDispatcher.SetDelegate(dispatch)
	require.NoError(err)

	ctx := datastoremw.ContextWithHandle(context.Background())
	require.NoError(datastoremw.SetInContext(ctx, ds))

	return ctx, cachingDispatcher, revision
}
