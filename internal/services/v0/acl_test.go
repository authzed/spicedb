package v0

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	g "github.com/authzed/spicedb/pkg/graph"
	"github.com/authzed/spicedb/pkg/grpcutil"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zookie"
)

var ONR = tuple.ObjectAndRelation

func RR(namespaceName string, relationName string) *v0.RelationReference {
	return &v0.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
	}
}

var testTimedeltas = []time.Duration{0, 1 * time.Second}

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})

	// Set this to Trace to dump log statements in tests.
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

func TestRead(t *testing.T) {
	testCases := []struct {
		name         string
		filter       *v0.RelationTupleFilter
		expectedCode codes.Code
		expected     []string
	}{
		{
			"namespace only",
			&v0.RelationTupleFilter{Namespace: tf.DocumentNS.Name},
			codes.OK,
			[]string{
				"document:companyplan#parent@folder:company#...",
				"document:masterplan#parent@folder:strategy#...",
				"document:masterplan#owner@user:product_manager#...",
				"document:masterplan#viewer@user:eng_lead#...",
				"document:masterplan#parent@folder:plans#...",
				"document:healthplan#parent@folder:plans#...",
				"document:specialplan#editor@user:multiroleguy#...",
				"document:specialplan#viewer_and_editor@user:multiroleguy#...",
				"document:specialplan#viewer_and_editor@user:missingrolegal#...",
			},
		},
		{
			"namespace and object id",
			&v0.RelationTupleFilter{
				Namespace: tf.DocumentNS.Name,
				ObjectId:  "healthplan",
				Filters: []v0.RelationTupleFilter_Filter{
					v0.RelationTupleFilter_OBJECT_ID,
				},
			},
			codes.OK,
			[]string{
				"document:healthplan#parent@folder:plans#...",
			},
		},
		{
			"namespace and relation",
			&v0.RelationTupleFilter{
				Namespace: tf.DocumentNS.Name,
				Relation:  "parent",
				Filters: []v0.RelationTupleFilter_Filter{
					v0.RelationTupleFilter_RELATION,
				},
			},
			codes.OK,
			[]string{
				"document:companyplan#parent@folder:company#...",
				"document:masterplan#parent@folder:strategy#...",
				"document:masterplan#parent@folder:plans#...",
				"document:healthplan#parent@folder:plans#...",
			},
		},
		{
			"namespace and userset",
			&v0.RelationTupleFilter{
				Namespace: tf.DocumentNS.Name,
				Userset:   ONR("folder", "plans", "..."),
				Filters: []v0.RelationTupleFilter_Filter{
					v0.RelationTupleFilter_USERSET,
				},
			},
			codes.OK,
			[]string{
				"document:masterplan#parent@folder:plans#...",
				"document:healthplan#parent@folder:plans#...",
			},
		},
		{
			"multiple filters",
			&v0.RelationTupleFilter{
				Namespace: tf.DocumentNS.Name,
				ObjectId:  "masterplan",
				Userset:   ONR("folder", "plans", "..."),
				Filters: []v0.RelationTupleFilter_Filter{
					v0.RelationTupleFilter_USERSET,
					v0.RelationTupleFilter_OBJECT_ID,
				},
			},
			codes.OK,
			[]string{
				"document:masterplan#parent@folder:plans#...",
			},
		},
		{
			"bad namespace",
			&v0.RelationTupleFilter{Namespace: ""},
			codes.InvalidArgument,
			nil,
		},
		{
			"bad objectId",
			&v0.RelationTupleFilter{
				Namespace: tf.DocumentNS.Name,
				ObjectId:  "ma",
				Userset:   ONR("folder", "plans", "..."),
				Filters: []v0.RelationTupleFilter_Filter{
					v0.RelationTupleFilter_USERSET,
					v0.RelationTupleFilter_OBJECT_ID,
				},
			},
			codes.InvalidArgument,
			nil,
		},
		{
			"bad object relation",
			&v0.RelationTupleFilter{
				Namespace: tf.DocumentNS.Name,
				Relation:  "ad",
				Filters: []v0.RelationTupleFilter_Filter{
					v0.RelationTupleFilter_RELATION,
				},
			},
			codes.InvalidArgument,
			nil,
		},
		{
			"bad userset",
			&v0.RelationTupleFilter{
				Namespace: tf.DocumentNS.Name,
				ObjectId:  "ma",
				Userset:   ONR("folder", "", "..."),
				Filters: []v0.RelationTupleFilter_Filter{
					v0.RelationTupleFilter_USERSET,
					v0.RelationTupleFilter_OBJECT_ID,
				},
			},
			codes.InvalidArgument,
			nil,
		},
		{
			"nil argument required filter",
			&v0.RelationTupleFilter{
				Namespace: tf.DocumentNS.Name,
				Userset:   nil,
				Filters: []v0.RelationTupleFilter_Filter{
					v0.RelationTupleFilter_OBJECT_ID,
				},
			},
			codes.InvalidArgument,
			nil,
		},
		{
			"missing namespace",
			&v0.RelationTupleFilter{
				Namespace: "doesnotexist",
			},
			codes.FailedPrecondition,
			nil,
		},
		{
			"missing relation",
			&v0.RelationTupleFilter{
				Namespace: tf.DocumentNS.Name,
				Relation:  "invalidrelation",
				Filters: []v0.RelationTupleFilter_Filter{
					v0.RelationTupleFilter_RELATION,
				},
			},
			codes.FailedPrecondition,
			nil,
		},
	}

	for _, delta := range testTimedeltas {
		t.Run(fmt.Sprintf("fuzz%d", delta), func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					require := require.New(t)

					srv, revision := newACLServicer(require, delta, memdb.DisableGC, 0)

					resp, err := srv.Read(context.Background(), &v0.ReadRequest{
						Tuplesets:  []*v0.RelationTupleFilter{tc.filter},
						AtRevision: zookie.NewFromRevision(revision),
					})

					if tc.expectedCode == codes.OK {
						require.NoError(err)
						require.NotNil(resp.Revision)
						require.Len(resp.Tuplesets, 1)

						verifyTuples(tc.expected, resp.Tuplesets[0].Tuples, require)
					} else {
						grpcutil.RequireStatus(t, tc.expectedCode, err)
					}
				})
			}
		})
	}
}

func TestReadBadZookie(t *testing.T) {
	require := require.New(t)

	srv, revision := newACLServicer(require, 0, 10*time.Millisecond, 0)

	_, err := srv.Read(context.Background(), &v0.ReadRequest{
		Tuplesets: []*v0.RelationTupleFilter{
			{Namespace: tf.DocumentNS.Name},
		},
		AtRevision: zookie.NewFromRevision(revision),
	})
	require.NoError(err)

	_, err = srv.Read(context.Background(), &v0.ReadRequest{
		Tuplesets: []*v0.RelationTupleFilter{
			{Namespace: tf.DocumentNS.Name},
		},
		AtRevision: zookie.NewFromRevision(revision.Sub(decimal.NewFromInt(1))),
	})
	require.NoError(err)

	// Wait until the gc window expires
	time.Sleep(20 * time.Millisecond)

	_, err = srv.Read(context.Background(), &v0.ReadRequest{
		Tuplesets: []*v0.RelationTupleFilter{
			{Namespace: tf.DocumentNS.Name},
		},
		AtRevision: zookie.NewFromRevision(revision),
	})
	require.NoError(err)

	_, err = srv.Read(context.Background(), &v0.ReadRequest{
		Tuplesets: []*v0.RelationTupleFilter{
			{Namespace: tf.DocumentNS.Name},
		},
		AtRevision: zookie.NewFromRevision(revision.Sub(decimal.NewFromInt(1))),
	})
	grpcutil.RequireStatus(t, codes.OutOfRange, err)

	_, err = srv.Read(context.Background(), &v0.ReadRequest{
		Tuplesets: []*v0.RelationTupleFilter{
			{Namespace: tf.DocumentNS.Name},
		},
		AtRevision: zookie.NewFromRevision(revision.Add(decimal.NewFromInt(1))),
	})
	grpcutil.RequireStatus(t, codes.OutOfRange, err)
}

func TestWrite(t *testing.T) {
	require := require.New(t)

	srv, _ := newACLServicer(require, 0, memdb.DisableGC, 0)

	toWriteStr := "document:totallynew#parent@folder:plans#..."
	toWrite := tuple.Scan(toWriteStr)
	require.NotNil(toWrite)

	resp, err := srv.Write(context.Background(), &v0.WriteRequest{
		WriteConditions: []*v0.RelationTuple{toWrite},
		Updates:         []*v0.RelationTupleUpdate{tuple.Create(toWrite)},
	})
	require.Nil(resp)
	grpcutil.RequireStatus(t, codes.FailedPrecondition, err)

	existing := tuple.Scan(tf.StandardTuples[0])
	require.NotNil(existing)

	resp, err = srv.Write(context.Background(), &v0.WriteRequest{
		WriteConditions: []*v0.RelationTuple{existing},
		Updates:         []*v0.RelationTupleUpdate{tuple.Create(toWrite)},
	})
	require.NoError(err)
	require.NotNil(resp.Revision)
	require.NotZero(resp.Revision.Token)

	findWritten := []*v0.RelationTupleFilter{
		{
			Namespace: "document",
			ObjectId:  "totallynew",
			Filters: []v0.RelationTupleFilter_Filter{
				v0.RelationTupleFilter_OBJECT_ID,
			},
		},
	}
	readBack, err := srv.Read(context.Background(), &v0.ReadRequest{
		AtRevision: resp.Revision,
		Tuplesets:  findWritten,
	})
	require.NoError(err)
	require.Len(readBack.Tuplesets, 1)

	verifyTuples([]string{toWriteStr}, readBack.Tuplesets[0].Tuples, require)

	deleted, err := srv.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Delete(toWrite)},
	})
	require.NoError(err)

	verifyMissing, err := srv.Read(context.Background(), &v0.ReadRequest{
		AtRevision: deleted.Revision,
		Tuplesets:  findWritten,
	})
	require.NoError(err)
	require.Len(verifyMissing.Tuplesets, 1)

	verifyTuples(nil, verifyMissing.Tuplesets[0].Tuples, require)
}

func TestInvalidWriteArguments(t *testing.T) {
	testCases := []struct {
		name          string
		preconditions []string
		tuples        []string
		expectedCode  codes.Code
	}{
		{
			"empty tuple",
			nil,
			[]string{":#@:#"},
			codes.InvalidArgument,
		},
		{
			"bad precondition",
			[]string{":#@:#"},
			nil,
			codes.InvalidArgument,
		},
		{
			"good precondition, short object ID",
			[]string{"document:newdoc#parent@folder:afolder#..."},
			[]string{"document:a#parent@folder:afolder#..."},
			codes.InvalidArgument,
		},
		{
			"bad precondition, good write",
			[]string{"document:newdoc#parent@folder:a#..."},
			[]string{"document:newdoc#parent@folder:afolder#..."},
			codes.InvalidArgument,
		},
		{
			"bad write namespace",
			nil,
			[]string{"docu:newdoc#parent@folder:afolder#..."},
			codes.FailedPrecondition,
		},
		{
			"bad write relation",
			nil,
			[]string{"document:newdoc#pare@folder:afolder#..."},
			codes.FailedPrecondition,
		},
		{
			"bad write userset namespace",
			nil,
			[]string{"document:newdoc#parent@fold:afolder#..."},
			codes.FailedPrecondition,
		},
		{
			"bad write userset relation",
			nil,
			[]string{"document:newdoc#parent@folder:afolder#adsfasfsd"},
			codes.FailedPrecondition,
		},
		{
			"bad write wrong relation type",
			nil,
			[]string{"document:newdoc#parent@user:someuser#..."},
			codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			srv, _ := newACLServicer(require, 0, memdb.DisableGC, 0)

			var preconditions []*v0.RelationTuple
			for _, p := range tc.preconditions {
				preconditions = append(preconditions, tuple.Scan(p))
			}

			var mutations []*v0.RelationTupleUpdate
			for _, tpl := range tc.tuples {
				mutations = append(mutations, tuple.Touch(tuple.Scan(tpl)))
			}

			_, err := srv.Write(context.Background(), &v0.WriteRequest{
				WriteConditions: preconditions,
				Updates:         mutations,
			})
			grpcutil.RequireStatus(t, tc.expectedCode, err)
		})
	}
}

func TestCheck(t *testing.T) {
	type checkTest struct {
		user       *v0.ObjectAndRelation
		membership bool
	}

	testCases := []struct {
		start             *v0.ObjectAndRelation
		expectedErrorCode codes.Code
		checkTests        []checkTest
	}{
		{
			ONR("document", "masterplan", "owner"),
			codes.OK,
			[]checkTest{
				{ONR("user", "product_manager", "..."), true},
				{ONR("user", "unknown", "..."), false},
				{ONR("user", "eng_lead", "..."), false},
				{ONR("user", "villain", "..."), false},
			},
		},
		{
			ONR("document", "masterplan", "viewer"),
			codes.OK,
			[]checkTest{
				{ONR("user", "product_manager", "..."), true},
				{ONR("user", "chief_financial_officer", "..."), true},
				{ONR("user", "villain", "..."), false},
			},
		},
		{
			ONR("document", "masterplan", "fakerelation"),
			codes.FailedPrecondition,
			[]checkTest{
				{ONR("user", "product_manager", "..."), false},
			},
		},
		{
			ONR("docs", "masterplan", "owner"),
			codes.FailedPrecondition,
			[]checkTest{
				{ONR("user", "product_manager", "..."), false},
			},
		},
		{
			ONR("document", "", "fakerelation"),
			codes.InvalidArgument,
			[]checkTest{
				{ONR("user", "product_manager", "..."), false},
			},
		},
		{
			ONR("document", "masterplan", "fakerelation"),
			codes.InvalidArgument,
			[]checkTest{
				{ONR("user", "", "..."), false},
			},
		},
	}

	for _, delta := range testTimedeltas {
		t.Run(fmt.Sprintf("fuzz%d", delta), func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tuple.StringONR(tc.start), func(t *testing.T) {
					for _, checkTest := range tc.checkTests {
						name := fmt.Sprintf(
							"%s=>%t",
							tuple.StringONR(checkTest.user),
							checkTest.membership,
						)
						t.Run(name, func(t *testing.T) {
							require := require.New(t)
							srv, revision := newACLServicer(require, delta, memdb.DisableGC, 0)

							resp, err := srv.Check(context.Background(), &v0.CheckRequest{
								TestUserset: tc.start,
								User: &v0.User{
									UserOneof: &v0.User_Userset{
										Userset: checkTest.user,
									},
								},
								AtRevision: zookie.NewFromRevision(revision),
							})

							ccResp, ccErr := srv.ContentChangeCheck(context.Background(), &v0.ContentChangeCheckRequest{
								TestUserset: tc.start,
								User: &v0.User{
									UserOneof: &v0.User_Userset{
										Userset: checkTest.user,
									},
								},
							})

							if tc.expectedErrorCode == codes.OK {
								require.NoError(err)
								require.NoError(ccErr)
								require.NotNil(resp.Revision)
								require.NotNil(ccResp.Revision)
								require.NotEmpty(resp.Revision.Token)
								require.NotEmpty(ccResp.Revision.Token)
								require.Equal(checkTest.membership, resp.IsMember)
								require.Equal(checkTest.membership, ccResp.IsMember)
								if checkTest.membership {
									require.Equal(v0.CheckResponse_MEMBER, resp.Membership)
									require.Equal(v0.CheckResponse_MEMBER, ccResp.Membership)
								} else {
									require.Equal(v0.CheckResponse_NOT_MEMBER, resp.Membership)
									require.Equal(v0.CheckResponse_NOT_MEMBER, ccResp.Membership)
								}
							} else {
								grpcutil.RequireStatus(t, tc.expectedErrorCode, err)
								grpcutil.RequireStatus(t, tc.expectedErrorCode, ccErr)
							}
						})
					}
				})
			}
		})
	}
}

func BenchmarkACL(b *testing.B) {
	require := require.New(b)
	srv, revision := newACLServicer(require, 0, memdb.DisableGC, 3*time.Millisecond)

	b.Run("check", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			resp, err := srv.Check(context.Background(), &v0.CheckRequest{
				TestUserset: ONR("document", "masterplan", "viewer"),
				User: &v0.User{
					UserOneof: &v0.User_Userset{
						Userset: ONR("user", "villain", "..."),
					},
				},
				AtRevision: zookie.NewFromRevision(revision),
			})
			require.NoError(err)
			require.Equal(v0.CheckResponse_NOT_MEMBER, resp.Membership)
		}
	})
}

func TestExpand(t *testing.T) {
	testCases := []struct {
		start              *v0.ObjectAndRelation
		expandRelatedCount int
		expectedErrorCode  codes.Code
	}{
		{ONR("document", "masterplan", "owner"), 1, codes.OK},
		{ONR("document", "masterplan", "viewer"), 7, codes.OK},
		{ONR("document", "masterplan", "fakerelation"), 0, codes.FailedPrecondition},
		{ONR("fake", "masterplan", "owner"), 0, codes.FailedPrecondition},
		{ONR("document", "", "owner"), 1, codes.InvalidArgument},
	}

	for _, delta := range testTimedeltas {
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tuple.StringONR(tc.start), func(t *testing.T) {
					require := require.New(t)
					srv, revision := newACLServicer(require, delta, memdb.DisableGC, 0)

					expanded, err := srv.Expand(context.Background(), &v0.ExpandRequest{
						Userset:    tc.start,
						AtRevision: zookie.NewFromRevision(revision),
					})
					if tc.expectedErrorCode == codes.OK {
						require.NoError(err)
						require.NotNil(expanded.Revision)
						require.NotEmpty(expanded.Revision.Token)

						require.Equal(tc.expandRelatedCount, len(g.Simplify(expanded.TreeNode)))
					} else {
						grpcutil.RequireStatus(t, tc.expectedErrorCode, err)
					}
				})
			}
		})
	}
}

func TestLookup(t *testing.T) {
	testCases := []struct {
		relation          *v0.RelationReference
		user              *v0.ObjectAndRelation
		expectedObjectIds []string
		expectedErrorCode codes.Code
	}{
		{
			RR("document", "viewer"),
			ONR("user", "eng_lead", "..."),
			[]string{"masterplan"},
			codes.OK,
		},
		{
			RR("document", "viewer"),
			ONR("user", "product_manager", "..."),
			[]string{"masterplan"},
			codes.OK,
		},
		{
			RR("document", "viewer"),
			ONR("user", "chief_financial_officer", "..."),
			[]string{"masterplan", "healthplan"},
			codes.OK,
		},
		{
			RR("document", "viewer"),
			ONR("user", "auditor", "..."),
			[]string{"masterplan", "companyplan"},
			codes.OK,
		},
		{
			RR("document", "viewer"),
			ONR("user", "vp_product", "..."),
			[]string{"masterplan"},
			codes.OK,
		},
		{
			RR("document", "viewer"),
			ONR("user", "legal", "..."),
			[]string{"masterplan", "companyplan"},
			codes.OK,
		},
		{
			RR("document", "viewer"),
			ONR("user", "owner", "..."),
			[]string{"masterplan", "companyplan"},
			codes.OK,
		},
		{
			RR("document", "viewer"),
			ONR("user", "villain", "..."),
			[]string{},
			codes.OK,
		},
		{
			RR("document", "viewer"),
			ONR("user", "unknowngal", "..."),
			[]string{},
			codes.OK,
		},

		{
			RR("document", "viewer_and_editor"),
			ONR("user", "eng_lead", "..."),
			[]string{},
			codes.OK,
		},
		{
			RR("document", "viewer_and_editor"),
			ONR("user", "multiroleguy", "..."),
			[]string{"specialplan"},
			codes.OK,
		},
		{
			RR("document", "viewer_and_editor"),
			ONR("user", "missingrolegal", "..."),
			[]string{},
			codes.OK,
		},
		{
			RR("document", "viewer_and_editor_derived"),
			ONR("user", "multiroleguy", "..."),
			[]string{"specialplan"},
			codes.OK,
		},
		{
			RR("document", "viewer_and_editor_derived"),
			ONR("user", "missingrolegal", "..."),
			[]string{},
			codes.OK,
		},
		{
			RR("document", "invalidrelation"),
			ONR("user", "missingrolegal", "..."),
			[]string{},
			codes.FailedPrecondition,
		},
		{
			RR("document", "viewer_and_editor_derived"),
			ONR("user", "someuser", "invalidrelation"),
			[]string{},
			codes.FailedPrecondition,
		},
		{
			RR("invalidnamespace", "viewer_and_editor_derived"),
			ONR("user", "someuser", "..."),
			[]string{},
			codes.FailedPrecondition,
		},
		{
			RR("document", "viewer_and_editor_derived"),
			ONR("invalidnamespace", "someuser", "..."),
			[]string{},
			codes.FailedPrecondition,
		},
	}

	for _, delta := range testTimedeltas {
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("%s::%s from %s", tc.relation.Namespace, tc.relation.Relation, tuple.StringONR(tc.user)), func(t *testing.T) {
					require := require.New(t)
					srv, revision := newACLServicer(require, delta, memdb.DisableGC, 0)

					result, err := srv.Lookup(context.Background(), &v0.LookupRequest{
						User:           tc.user,
						ObjectRelation: tc.relation,
						Limit:          100,
						AtRevision:     zookie.NewFromRevision(revision),
					})
					if tc.expectedErrorCode == codes.OK {
						require.NoError(err)
						require.NotNil(result.Revision)
						require.NotEmpty(result.Revision.Token)

						sort.Strings(tc.expectedObjectIds)
						sort.Strings(result.ResolvedObjectIds)

						require.Equal(tc.expectedObjectIds, result.ResolvedObjectIds)

						// Sanity check: Issue a check on every ID returned.
						for _, objId := range result.ResolvedObjectIds {
							checkResp, err := srv.Check(context.Background(), &v0.CheckRequest{
								TestUserset: &v0.ObjectAndRelation{
									Namespace: tc.relation.Namespace,
									Relation:  tc.relation.Relation,
									ObjectId:  objId,
								},
								User: &v0.User{
									UserOneof: &v0.User_Userset{
										Userset: tc.user,
									},
								},
								AtRevision: zookie.NewFromRevision(revision),
							})
							require.NoError(err)
							require.Equal(true, checkResp.IsMember, "Object ID %s is not a member", objId)
						}
					} else {
						grpcutil.RequireStatus(t, tc.expectedErrorCode, err)
					}
				})
			}
		})
	}
}

func newACLServicer(
	require *require.Assertions,
	revisionFuzzingTimedelta time.Duration,
	gcWindow time.Duration,
	simulatedLatency time.Duration,
) (v0.ACLServiceServer, decimal.Decimal) {
	emptyDS, err := memdb.NewMemdbDatastore(0, revisionFuzzingTimedelta, gcWindow, simulatedLatency)
	require.NoError(err)

	ds, revision := tf.StandardDatastoreWithData(emptyDS, require)

	ns, err := namespace.NewCachingNamespaceManager(ds, 1*time.Second, nil)
	require.NoError(err)

	dispatch, err := graph.NewLocalDispatcher(ns, ds)
	require.NoError(err)

	return NewACLServer(ds, ns, dispatch, 50), revision
}

func verifyTuples(expected []string, found []*v0.RelationTuple, require *require.Assertions) {
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
