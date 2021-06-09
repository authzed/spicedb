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
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/testfixtures"
	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
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
		userset  *pb.ObjectAndRelation
		expected []expected
	}

	testCases := []struct {
		namespace string
		objectID  string
		usersets  []userset
	}{
		{"document", "masterplan", []userset{
			{ONR("user", "product_manager", Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{ONR("user", "chief_financial_officer", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
		}},
		{"document", "healthplan", []userset{
			{ONR("user", "product_manager", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "chief_financial_officer", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
		}},
		{"folder", "company", []userset{
			{ONR("user", "product_manager", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "chief_financial_officer", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "owner", Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{ONR("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("folder", "auditors", "viewer"), []expected{{"viewer", true}}},
		}},
		{"folder", "strategy", []userset{
			{ONR("user", "product_manager", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "chief_financial_officer", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "vp_product", Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{ONR("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("folder", "company", Ellipsis), []expected{{"parent", true}}},
		}},
		{"folder", "isolated", []userset{
			{ONR("user", "product_manager", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "chief_financial_officer", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
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

					checkResult := dispatch.Check(context.Background(), CheckRequest{
						Start:          ONR(tc.namespace, tc.objectID, expected.relation),
						Goal:           userset.userset,
						AtRevision:     revision,
						DepthRemaining: 50,
					})

					require.NoError(checkResult.Err)
					require.Equal(expected.isMember, checkResult.IsMember)
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

	mutations := []*pb.RelationTupleUpdate{
		tuple.Create(&pb.RelationTuple{
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

	dispatch, err := NewLocalDispatcher(nsm, ds)
	require.NoError(err)

	checkResult := dispatch.Check(context.Background(), CheckRequest{
		Start:          ONR("folder", "oops", "owner"),
		Goal:           ONR("user", "fake", Ellipsis),
		AtRevision:     revision,
		DepthRemaining: 50,
	})

	require.Error(checkResult.Err)
	require.False(checkResult.IsMember)
}

func newLocalDispatcher(require *require.Assertions) (Dispatcher, decimal.Decimal) {
	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	nsm, err := namespace.NewCachingNamespaceManager(ds, 1*time.Second, testCacheConfig)
	require.NoError(err)

	dispatch, err := NewLocalDispatcher(nsm, ds)
	require.NoError(err)

	cachingDispatcher, err := NewCachingDispatcher(dispatch, nil, DisablePromMetrics)
	require.NoError(err)

	return cachingDispatcher, revision
}
