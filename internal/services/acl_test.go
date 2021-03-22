package services

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/graph"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	g "github.com/authzed/spicedb/pkg/graph"
	"github.com/authzed/spicedb/pkg/tuple"
)

var ONR = tuple.ObjectAndRelation

func TestRead(t *testing.T) {
	testCases := []struct {
		name     string
		filter   *api.RelationTupleFilter
		expected []string
	}{
		{
			"namespace only",
			&api.RelationTupleFilter{Namespace: tf.DocumentNS.Name},
			[]string{
				"document:masterplan#parent@folder:strategy#...",
				"document:masterplan#owner@user:pm#...",
				"document:masterplan#viewer@user:eng_lead#...",
				"document:masterplan#parent@folder:plans#...",
				"document:healthplan#parent@folder:plans#...",
			},
		},
		{
			"namespace and object id",
			&api.RelationTupleFilter{
				Namespace: tf.DocumentNS.Name,
				ObjectId:  "healthplan",
				Filters: []api.RelationTupleFilter_Filter{
					api.RelationTupleFilter_OBJECT_ID,
				},
			},
			[]string{
				"document:healthplan#parent@folder:plans#...",
			},
		},
		{
			"namespace and relation",
			&api.RelationTupleFilter{
				Namespace: tf.DocumentNS.Name,
				Relation:  "parent",
				Filters: []api.RelationTupleFilter_Filter{
					api.RelationTupleFilter_RELATION,
				},
			},
			[]string{
				"document:masterplan#parent@folder:strategy#...",
				"document:masterplan#parent@folder:plans#...",
				"document:healthplan#parent@folder:plans#...",
			},
		},
		{
			"namespace and userset",
			&api.RelationTupleFilter{
				Namespace: tf.DocumentNS.Name,
				Userset:   ONR("folder", "plans", "..."),
				Filters: []api.RelationTupleFilter_Filter{
					api.RelationTupleFilter_USERSET,
				},
			},
			[]string{
				"document:masterplan#parent@folder:plans#...",
				"document:healthplan#parent@folder:plans#...",
			},
		},
		{
			"multiple filters",
			&api.RelationTupleFilter{
				Namespace: tf.DocumentNS.Name,
				ObjectId:  "masterplan",
				Userset:   ONR("folder", "plans", "..."),
				Filters: []api.RelationTupleFilter_Filter{
					api.RelationTupleFilter_USERSET,
					api.RelationTupleFilter_OBJECT_ID,
				},
			},
			[]string{
				"document:masterplan#parent@folder:plans#...",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			srv := newACLServicer(require)

			resp, err := srv.Read(context.Background(), &api.ReadRequest{
				Tuplesets: []*api.RelationTupleFilter{tc.filter},
			})
			require.NoError(err)
			require.NotNil(resp.Revision)
			require.Len(resp.Tuplesets, 1)

			verifyTuples(tc.expected, resp.Tuplesets[0].Tuples, require)
		})
	}
}

func TestWrite(t *testing.T) {
	require := require.New(t)

	srv := newACLServicer(require)

	toWriteStr := "document:totallynew#parent@folder:plans#..."
	toWrite := tuple.Scan(toWriteStr)
	require.NotNil(toWrite)

	resp, err := srv.Write(context.Background(), &api.WriteRequest{
		WriteConditions: []*api.RelationTuple{toWrite},
		Updates:         []*api.RelationTupleUpdate{tuple.Create(toWrite)},
	})
	require.Nil(resp)
	requireGRPCStatus(codes.FailedPrecondition, err, require)

	existing := tuple.Scan(tf.StandardTuples[0])
	require.NotNil(existing)

	resp, err = srv.Write(context.Background(), &api.WriteRequest{
		WriteConditions: []*api.RelationTuple{existing},
		Updates:         []*api.RelationTupleUpdate{tuple.Create(toWrite)},
	})
	require.NoError(err)
	require.NotNil(resp.Revision)
	require.NotZero(resp.Revision.Token)

	findWritten := []*api.RelationTupleFilter{
		{
			Namespace: "document",
			ObjectId:  "totallynew",
			Filters: []api.RelationTupleFilter_Filter{
				api.RelationTupleFilter_OBJECT_ID,
			},
		},
	}
	readBack, err := srv.Read(context.Background(), &api.ReadRequest{
		AtRevision: resp.Revision,
		Tuplesets:  findWritten,
	})
	require.NoError(err)
	require.Len(readBack.Tuplesets, 1)

	verifyTuples([]string{toWriteStr}, readBack.Tuplesets[0].Tuples, require)

	deleted, err := srv.Write(context.Background(), &api.WriteRequest{
		Updates: []*api.RelationTupleUpdate{tuple.Delete(toWrite)},
	})
	require.NoError(err)

	verifyMissing, err := srv.Read(context.Background(), &api.ReadRequest{
		AtRevision: deleted.Revision,
		Tuplesets:  findWritten,
	})
	require.NoError(err)
	require.Len(verifyMissing.Tuplesets, 1)

	verifyTuples(nil, verifyMissing.Tuplesets[0].Tuples, require)
}

func TestCheck(t *testing.T) {
	type checkTest struct {
		user       *api.ObjectAndRelation
		membership bool
	}

	testCases := []struct {
		start             *api.ObjectAndRelation
		expectedErrorCode codes.Code
		checkTests        []checkTest
	}{
		{
			ONR("document", "masterplan", "owner"),
			codes.OK,
			[]checkTest{
				{ONR("user", "pm", "..."), true},
				{ONR("user", "unknown", "..."), false},
				{ONR("user", "eng_lead", "..."), false},
				{ONR("user", "villain", "..."), false},
			},
		},
		{
			ONR("document", "masterplan", "viewer"),
			codes.OK,
			[]checkTest{
				{ONR("user", "pm", "..."), true},
				{ONR("user", "cfo", "..."), true},
				{ONR("user", "villain", "..."), false},
			},
		},
		{
			ONR("document", "masterplan", "fakerelation"),
			codes.FailedPrecondition,
			[]checkTest{
				{ONR("user", "pm", "..."), false},
			},
		},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("%s:%s#%s", tc.start.Namespace, tc.start.ObjectId, tc.start.Relation)
		t.Run(name, func(t *testing.T) {
			require := require.New(t)
			srv := newACLServicer(require)

			for _, checkTest := range tc.checkTests {
				resp, err := srv.Check(context.Background(), &api.CheckRequest{
					TestUserset: tc.start,
					User: &api.User{
						UserOneof: &api.User_Userset{
							Userset: checkTest.user,
						},
					},
				})

				if tc.expectedErrorCode == codes.OK {
					require.NoError(err)
					require.NotNil(resp.Revision)
					require.NotEmpty(resp.Revision.Token)
					require.Equal(checkTest.membership, resp.IsMember)
					if checkTest.membership {
						require.Equal(api.CheckResponse_MEMBER, resp.Membership)
					} else {
						require.Equal(api.CheckResponse_NOT_MEMBER, resp.Membership)
					}
				} else {
					requireGRPCStatus(tc.expectedErrorCode, err, require)
				}
			}
		})
	}
}

func TestExpand(t *testing.T) {
	testCases := []struct {
		start              *api.ObjectAndRelation
		expandRelatedCount int
		expectedErrorCode  codes.Code
	}{
		{ONR("document", "masterplan", "owner"), 1, codes.OK},
		{ONR("document", "masterplan", "viewer"), 7, codes.OK},
		{ONR("document", "masterplan", "fakerelation"), 0, codes.FailedPrecondition},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("%s:%s#%s", tc.start.Namespace, tc.start.ObjectId, tc.start.Relation)
		t.Run(name, func(t *testing.T) {
			require := require.New(t)
			srv := newACLServicer(require)

			expanded, err := srv.Expand(context.Background(), &api.ExpandRequest{
				Userset: tc.start,
			})
			if tc.expectedErrorCode == codes.OK {
				require.NoError(err)
				require.NotNil(expanded.Revision)
				require.NotEmpty(expanded.Revision.Token)

				require.Equal(tc.expandRelatedCount, len(g.Simplify(expanded.TreeNode)))
			} else {
				requireGRPCStatus(tc.expectedErrorCode, err, require)
			}
		})
	}
}

func newACLServicer(require *require.Assertions) api.ACLServiceServer {
	emptyDS, err := memdb.NewMemdbDatastore(0)
	require.NoError(err)

	ds, _ := tf.StandardDatastoreWithData(emptyDS, require)

	dispatch, err := graph.NewLocalDispatcher(ds)
	require.NoError(err)

	return NewACLServer(ds, dispatch, 50)
}

func verifyTuples(expected []string, found []*api.RelationTuple, require *require.Assertions) {
	expectedTuples := make(map[string]struct{}, len(expected))
	for _, expTpl := range expected {
		expectedTuples[expTpl] = struct{}{}
	}

	for _, foundTpl := range found {
		serialized := tuple.String(foundTpl)
		require.NotZero(serialized)
		_, ok := expectedTuples[serialized]
		require.True(ok, "unexpected tuple: %s", serialized)
		delete(expectedTuples, serialized)
	}

	require.Empty(expectedTuples, "expected tuples remaining: %#v", expectedTuples)
}
