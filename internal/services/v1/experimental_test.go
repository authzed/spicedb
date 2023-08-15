package v1_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/authzed/authzed-go/pkg/responsemeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/scylladb/go-set"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/shared"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestBulkImportRelationships(t *testing.T) {
	testCases := []struct {
		name       string
		batchSize  func() int
		numBatches int
	}{
		{"one small batch", constBatch(1), 1},
		{"one big batch", constBatch(10_000), 1},
		{"many small batches", constBatch(5), 1_000},
		{"one empty batch", constBatch(0), 1},
		{"small random batches", randomBatch(1, 10), 100},
		{"big random batches", randomBatch(1_000, 3_000), 50},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithSchema)
			client := v1.NewExperimentalServiceClient(conn)
			t.Cleanup(cleanup)

			ctx := context.Background()

			writer, err := client.BulkImportRelationships(ctx)
			require.NoError(err)

			var expectedTotal uint64
			for batchNum := 0; batchNum < tc.numBatches; batchNum++ {
				batchSize := tc.batchSize()
				batch := make([]*v1.Relationship, 0, batchSize)

				for i := 0; i < batchSize; i++ {
					batch = append(batch, rel(
						tf.DocumentNS.Name,
						strconv.Itoa(batchNum)+"_"+strconv.Itoa(i),
						"viewer",
						tf.UserNS.Name,
						strconv.Itoa(i),
						"",
					))
				}

				err := writer.Send(&v1.BulkImportRelationshipsRequest{
					Relationships: batch,
				})
				require.NoError(err)

				expectedTotal += uint64(batchSize)
			}

			resp, err := writer.CloseAndRecv()
			require.NoError(err)
			require.Equal(expectedTotal, resp.NumLoaded)

			readerClient := v1.NewPermissionsServiceClient(conn)
			stream, err := readerClient.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType: tf.DocumentNS.Name,
				},
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
				},
			})
			require.NoError(err)

			var readBack uint64
			for _, err = stream.Recv(); err == nil; _, err = stream.Recv() {
				readBack++
			}
			require.ErrorIs(err, io.EOF)
			require.Equal(expectedTotal, readBack)
		})
	}
}

func constBatch(size int) func() int {
	return func() int {
		return size
	}
}

func randomBatch(min, max int) func() int {
	return func() int {
		return rand.Intn(max-min) + min
	}
}

func TestBulkExportRelationships(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.StandardDatastoreWithSchema)
	client := v1.NewExperimentalServiceClient(conn)
	t.Cleanup(cleanup)

	nsAndRels := []struct {
		namespace string
		relation  string
	}{
		{tf.DocumentNS.Name, "viewer"},
		{tf.FolderNS.Name, "viewer"},
		{tf.DocumentNS.Name, "owner"},
		{tf.FolderNS.Name, "owner"},
		{tf.DocumentNS.Name, "editor"},
		{tf.FolderNS.Name, "editor"},
	}

	totalToWrite := uint64(1_000)
	expectedRels := set.NewStringSetWithSize(int(totalToWrite))
	batch := make([]*v1.Relationship, totalToWrite)
	for i := range batch {
		nsAndRel := nsAndRels[i%len(nsAndRels)]
		rel := rel(
			nsAndRel.namespace,
			strconv.Itoa(i),
			nsAndRel.relation,
			tf.UserNS.Name,
			strconv.Itoa(i),
			"",
		)
		batch[i] = rel
		expectedRels.Add(tuple.MustStringRelationship(rel))
	}

	ctx := context.Background()
	writer, err := client.BulkImportRelationships(ctx)
	require.NoError(t, err)

	require.NoError(t, writer.Send(&v1.BulkImportRelationshipsRequest{
		Relationships: batch,
	}))

	resp, err := writer.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, totalToWrite, resp.NumLoaded)

	testCases := []struct {
		batchSize      uint32
		paginateEveryN int
	}{
		{1_000, math.MaxInt},
		{10, math.MaxInt},
		{1_000, 1},
		{100, 5},
		{97, 7},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d-%d", tc.batchSize, tc.paginateEveryN), func(t *testing.T) {
			require := require.New(t)

			var totalRead uint64
			remainingRels := expectedRels.Copy()
			require.Equal(totalToWrite, uint64(expectedRels.Size()))
			var cursor *v1.Cursor

			var done bool
			for !done {
				streamCtx, cancel := context.WithCancel(ctx)

				stream, err := client.BulkExportRelationships(streamCtx, &v1.BulkExportRelationshipsRequest{
					OptionalLimit:  tc.batchSize,
					OptionalCursor: cursor,
				})
				require.NoError(err)

				for i := 0; i < tc.paginateEveryN; i++ {
					batch, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						done = true
						break
					}

					require.NoError(err)
					require.LessOrEqual(uint32(len(batch.Relationships)), tc.batchSize)
					require.NotNil(batch.AfterResultCursor)
					require.NotEmpty(batch.AfterResultCursor.Token)

					cursor = batch.AfterResultCursor
					totalRead += uint64(len(batch.Relationships))

					for _, rel := range batch.Relationships {
						remainingRels.Remove(tuple.MustStringRelationship(rel))
					}
				}

				cancel()
			}

			require.Equal(totalToWrite, totalRead)
			require.True(remainingRels.IsEmpty(), "rels were not exported %#v", remainingRels.List())
		})
	}
}

type bulkCheckTest struct {
	req     string
	resp    v1.CheckPermissionResponse_Permissionship
	partial []string
	err     error
}

func TestBulkCheckPermission(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.StandardDatastoreWithCaveatedData)
	client := v1.NewExperimentalServiceClient(conn)
	defer cleanup()

	testCases := []struct {
		name                  string
		requests              []string
		response              []bulkCheckTest
		expectedDispatchCount int
	}{
		{
			name: "same resource and permission, different subjects",
			requests: []string{
				`document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
				`document:masterplan#view@user:product_manager[test:{"secret": "1234"}]`,
				`document:masterplan#view@user:villain[test:{"secret": "1234"}]`,
			},
			response: []bulkCheckTest{
				{
					req:  `document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					req:  `document:masterplan#view@user:product_manager[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					req:  `document:masterplan#view@user:villain[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
				},
			},
			expectedDispatchCount: 49,
		},
		{
			name: "different resources, same permission and subject",
			requests: []string{
				`document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
				`document:companyplan#view@user:eng_lead[test:{"secret": "1234"}]`,
				`document:healthplan#view@user:eng_lead[test:{"secret": "1234"}]`,
			},
			response: []bulkCheckTest{
				{
					req:  `document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					req:  `document:companyplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
				},
				{
					req:  `document:healthplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
				},
			},
			expectedDispatchCount: 18,
		},
		{
			name: "some items fail",
			requests: []string{
				`document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
				"fake:fake#fake@fake:fake",
				"superfake:plan#view@user:eng_lead",
			},
			response: []bulkCheckTest{
				{
					req:  `document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					req: "fake:fake#fake@fake:fake",
					err: namespace.NewNamespaceNotFoundErr("fake"),
				},
				{
					req: "superfake:plan#view@user:eng_lead",
					err: namespace.NewNamespaceNotFoundErr("superfake"),
				},
			},
			expectedDispatchCount: 17,
		},
		{
			name: "different caveat context is not clustered",
			requests: []string{
				`document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
				`document:companyplan#view@user:eng_lead[test:{"secret": "1234"}]`,
				`document:masterplan#view@user:eng_lead[test:{"secret": "4321"}]`,
				`document:masterplan#view@user:eng_lead`,
			},
			response: []bulkCheckTest{
				{
					req:  `document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					req:  `document:companyplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
				},
				{
					req:  `document:masterplan#view@user:eng_lead[test:{"secret": "4321"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
				},
				{
					req:     `document:masterplan#view@user:eng_lead`,
					resp:    v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
					partial: []string{"secret"},
				},
			},
			expectedDispatchCount: 50,
		},
		{
			name: "namespace validation",
			requests: []string{
				"document:masterplan#view@fake:fake",
				"fake:fake#fake@user:eng_lead",
			},
			response: []bulkCheckTest{
				{
					req: "fake:fake#fake@user:eng_lead",
					err: namespace.NewNamespaceNotFoundErr("fake"),
				},
				{
					req: "document:masterplan#view@fake:fake",
					err: namespace.NewNamespaceNotFoundErr("fake"),
				},
			},
			expectedDispatchCount: 1,
		},
		{
			name: "chunking test",
			requests: (func() []string {
				toReturn := make([]string, 0, datastore.FilterMaximumIDCount+5)
				for i := 0; i < int(datastore.FilterMaximumIDCount+5); i++ {
					toReturn = append(toReturn, fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i))
				}

				return toReturn
			})(),
			response: (func() []bulkCheckTest {
				toReturn := make([]bulkCheckTest, 0, datastore.FilterMaximumIDCount+5)
				for i := 0; i < int(datastore.FilterMaximumIDCount+5); i++ {
					toReturn = append(toReturn, bulkCheckTest{
						req:  fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i),
						resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
					})
				}

				return toReturn
			})(),
			expectedDispatchCount: 11,
		},
		{
			name: "chunking test with errors",
			requests: (func() []string {
				toReturn := make([]string, 0, datastore.FilterMaximumIDCount+6)
				toReturn = append(toReturn, `nondoc:masterplan#view@user:eng_lead`)

				for i := 0; i < int(datastore.FilterMaximumIDCount+5); i++ {
					toReturn = append(toReturn, fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i))
				}

				return toReturn
			})(),
			response: (func() []bulkCheckTest {
				toReturn := make([]bulkCheckTest, 0, datastore.FilterMaximumIDCount+6)
				toReturn = append(toReturn, bulkCheckTest{
					req: `nondoc:masterplan#view@user:eng_lead`,
					err: namespace.NewNamespaceNotFoundErr("nondoc"),
				})

				for i := 0; i < int(datastore.FilterMaximumIDCount+5); i++ {
					toReturn = append(toReturn, bulkCheckTest{
						req:  fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i),
						resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
					})
				}

				return toReturn
			})(),
			expectedDispatchCount: 11,
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			req := v1.BulkCheckPermissionRequest{
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
				},
				Items: make([]*v1.BulkCheckPermissionRequestItem, 0, len(tt.requests)),
			}

			for _, r := range tt.requests {
				req.Items = append(req.Items, relToBulkRequestItem(r))
			}

			expected := make([]*v1.BulkCheckPermissionPair, 0, len(tt.response))
			for _, r := range tt.response {
				reqRel := tuple.ParseRel(r.req)
				resp := &v1.BulkCheckPermissionPair_Item{
					Item: &v1.BulkCheckPermissionResponseItem{
						Permissionship: r.resp,
					},
				}
				pair := &v1.BulkCheckPermissionPair{
					Request: &v1.BulkCheckPermissionRequestItem{
						Resource:   reqRel.Resource,
						Permission: reqRel.Relation,
						Subject:    reqRel.Subject,
					},
					Response: resp,
				}
				if reqRel.OptionalCaveat != nil {
					pair.Request.Context = reqRel.OptionalCaveat.Context
				}
				if len(r.partial) > 0 {
					resp.Item.PartialCaveatInfo = &v1.PartialCaveatInfo{
						MissingRequiredContext: r.partial,
					}
				}

				if r.err != nil {
					rewritten := shared.RewriteError(context.Background(), r.err, &shared.ConfigForErrors{})
					s, ok := status.FromError(rewritten)
					require.True(t, ok, "expected provided error to be status")
					pair.Response = &v1.BulkCheckPermissionPair_Error{
						Error: s.Proto(),
					}
				}
				expected = append(expected, pair)
			}

			var trailer metadata.MD
			actual, err := client.BulkCheckPermission(context.Background(), &req, grpc.Trailer(&trailer))
			require.NoError(t, err)

			dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(trailer, responsemeta.DispatchedOperationsCount)
			require.NoError(t, err)
			require.Equal(t, tt.expectedDispatchCount, dispatchCount)

			testutil.RequireProtoSlicesEqual(t, expected, actual.Pairs, sortByResource, "response bulk check pairs did not match")
		})
	}
}

func relToBulkRequestItem(rel string) *v1.BulkCheckPermissionRequestItem {
	r := tuple.ParseRel(rel)
	item := &v1.BulkCheckPermissionRequestItem{
		Resource:   r.Resource,
		Permission: r.Relation,
		Subject:    r.Subject,
	}
	if r.OptionalCaveat != nil {
		item.Context = r.OptionalCaveat.Context
	}
	return item
}

func sortByResource(first *v1.BulkCheckPermissionPair, second *v1.BulkCheckPermissionPair) int {
	if res := strings.Compare(first.Request.Resource.ObjectId, second.Request.Resource.ObjectId); res != 0 {
		return res
	}
	if res := strings.Compare(first.Request.Permission, second.Request.Permission); res != 0 {
		return res
	}
	if res := strings.Compare(first.Request.Subject.Object.ObjectId, second.Request.Subject.Object.ObjectId); res != 0 {
		return res
	}
	return strings.Compare(caveats.StableContextStringForHashing(first.Request.Context), caveats.StableContextStringForHashing(second.Request.Context))
}
