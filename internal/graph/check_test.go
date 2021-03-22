package graph

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})

	// Set this to Trace to dump log statements in tests.
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

var ONR = tuple.ObjectAndRelation

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
			{ONR("user", "pm", Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{ONR("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
		}},
		{"document", "healthplan", []userset{
			{ONR("user", "pm", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
		}},
		{"folder", "company", []userset{
			{ONR("user", "pm", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "owner", Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{ONR("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("folder", "auditors", "viewer"), []expected{{"viewer", true}}},
		}},
		{"folder", "strategy", []userset{
			{ONR("user", "pm", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "vp_product", Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{ONR("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{ONR("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("folder", "company", Ellipsis), []expected{{"parent", true}}},
		}},
		{"folder", "isolated", []userset{
			{ONR("user", "pm", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{ONR("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
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

					rawDS, err := memdb.NewMemdbDatastore(0)
					require.NoError(err)

					ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

					dispatch, err := NewLocalDispatcher(ds)
					require.NoError(err)

					checkResult := dispatch.Check(context.Background(), CheckRequest{
						Start:          ONR(tc.namespace, tc.objectID, expected.relation),
						Goal:           userset.userset,
						AtRevision:     revision,
						DepthRemaining: 50,
					})

					require.NoError(checkResult.Err)
					require.Equal(expected.isMember, checkResult.IsMember)

					// Check for goroutine leaks.
					defer goleak.VerifyNone(t)
				})
			}
		}
	}
}

func TestMaxDepth(t *testing.T) {
	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)

	mutations := []*pb.RelationTupleUpdate{
		tuple.Create(&pb.RelationTuple{
			ObjectAndRelation: ONR("folder", "oops", "owner"),
			User:              tuple.User(ONR("folder", "oops", "editor")),
		}),
	}

	revision, err := ds.WriteTuples(nil, mutations)
	require.NoError(err)
	require.Greater(revision, uint64(0))

	dispatch, err := NewLocalDispatcher(ds)
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
