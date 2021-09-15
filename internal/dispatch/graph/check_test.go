package graph

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	v0 "github.com/authzed/spicedb/internal/proto/authzed/api/v0"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/tuple"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})

	// Set this to Trace to dump log statements in tests.
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

var ONR = tuple.ObjectAndRelation

var testCacheConfig = &ristretto.Config{
	NumCounters: 1e2,     // number of keys to track frequency of (10k).
	MaxCost:     1 << 20, // maximum cost of cache (1MB).
	BufferItems: 64,      // number of keys per Get buffer.
}

func TestSimple(t *testing.T) {
	type expected struct {
		relation string
		isMember bool
	}

	type userset struct {
		userset  *v0.ObjectAndRelation
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
					"%s:%s#%s@%s:%s#%s=>%t",
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

					dispatch, revision := newLocalDispatcher(require)

					checkResult, err := dispatch.DispatchCheck(context.Background(), &v1.DispatchCheckRequest{
						ObjectAndRelation: ONR(tc.namespace, tc.objectID, expected.relation),
						Subject:           userset.userset,
						Metadata: &v1.ResolverMeta{
							AtRevision:     revision.String(),
							DepthRemaining: 50,
						},
					})

					require.NoError(err)
					require.Equal(expected.isMember, checkResult.Membership == v1.DispatchCheckResponse_MEMBER)
				})
			}
		}
	}
}

func TestMaxDepth(t *testing.T) {
	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)

	mutations := []*v0.RelationTupleUpdate{
		tuple.Create(&v0.RelationTuple{
			ObjectAndRelation: ONR("folder", "oops", "owner"),
			User:              tuple.User(ONR("folder", "oops", "editor")),
		}),
	}

	ctx := context.Background()

	revision, err := ds.WriteTuples(ctx, nil, mutations)
	require.NoError(err)
	require.True(revision.GreaterThan(decimal.Zero))

	nsm, err := namespace.NewCachingNamespaceManager(ds, 1*time.Second, testCacheConfig)
	require.NoError(err)

	dispatch := NewLocalOnlyDispatcher(nsm, ds)

	checkResult, err := dispatch.DispatchCheck(context.Background(), &v1.DispatchCheckRequest{
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

func newLocalDispatcher(require *require.Assertions) (dispatch.Dispatcher, decimal.Decimal) {
	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	nsm, err := namespace.NewCachingNamespaceManager(ds, 1*time.Second, testCacheConfig)
	require.NoError(err)

	dispatch := NewLocalOnlyDispatcher(nsm, ds)

	cachingDispatcher, err := caching.NewCachingDispatcher(dispatch, nil, "")
	require.NoError(err)

	return cachingDispatcher, revision
}
