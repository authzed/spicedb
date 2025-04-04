package v1_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strconv"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/ccoveille/go-safecast"
	"github.com/jzelinskie/stringz"
	"github.com/scylladb/go-set"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/shared"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

const defaultFilterMaximumIDCountForTest = 100

func TestBulkImportRelationships(t *testing.T) {
	testCases := []struct {
		name       string
		batchSize  func() uint64
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, withCaveats := range []bool{true, false} {
				withCaveats := withCaveats
				t.Run(fmt.Sprintf("withCaveats=%t", withCaveats), func(t *testing.T) {
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

						for i := uint64(0); i < batchSize; i++ {
							if withCaveats {
								batch = append(batch, mustRelWithCaveatAndContext(
									tf.DocumentNS.Name,
									strconv.Itoa(batchNum)+"_"+strconv.FormatUint(i, 10),
									"caveated_viewer",
									tf.UserNS.Name,
									strconv.FormatUint(i, 10),
									"",
									"test",
									map[string]any{"secret": strconv.FormatUint(i, 10)},
								))
							} else {
								batch = append(batch, rel(
									tf.DocumentNS.Name,
									strconv.Itoa(batchNum)+"_"+strconv.FormatUint(i, 10),
									"viewer",
									tf.UserNS.Name,
									strconv.FormatUint(i, 10),
									"",
								))
							}
						}

						err := writer.Send(&v1.BulkImportRelationshipsRequest{
							Relationships: batch,
						})
						require.NoError(err)

						expectedTotal += batchSize
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
		})
	}
}

func constBatch(size uint64) func() uint64 {
	return func() uint64 {
		return size
	}
}

func randomBatch(minimum, maximum int) func() uint64 {
	return func() uint64 {
		// nolint:gosec
		// G404 use of non cryptographically secure random number generator is not a security concern here,
		// as this is only used for generating fixtures in testing.
		// This number should be non-negative
		return uint64(rand.Intn(maximum-minimum) + minimum)
	}
}

func TestBulkExportRelationshipsBeyondAllowedLimit(t *testing.T) {
	require := require.New(t)
	conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
	client := v1.NewExperimentalServiceClient(conn)
	t.Cleanup(cleanup)

	resp, err := client.BulkExportRelationships(context.Background(), &v1.BulkExportRelationshipsRequest{
		OptionalLimit: 10000005,
	})
	require.NoError(err)

	_, err = resp.Recv()
	require.Error(err)
	require.Contains(err.Error(), "provided limit 10000005 is greater than maximum allowed of 100000")
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
		{tf.DocumentNS.Name, "caveated_viewer"},
		{tf.DocumentNS.Name, "expiring_viewer"},
	}

	totalToWrite := 1_000
	expectedRels := set.NewStringSetWithSize(totalToWrite)
	batch := make([]*v1.Relationship, totalToWrite)
	for i := range batch {
		nsAndRel := nsAndRels[i%len(nsAndRels)]
		v1rel := relationshipForBulkTesting(nsAndRel, i)
		batch[i] = v1rel
		expectedRels.Add(tuple.MustV1RelString(v1rel))
	}

	ctx := context.Background()
	writer, err := client.BulkImportRelationships(ctx)
	require.NoError(t, err)

	require.NoError(t, writer.Send(&v1.BulkImportRelationshipsRequest{
		Relationships: batch,
	}))

	resp, err := writer.CloseAndRecv()
	require.NoError(t, err)
	numLoaded, err := safecast.ToInt(resp.NumLoaded)
	require.NoError(t, err)
	require.Equal(t, totalToWrite, numLoaded)

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

			var totalRead int
			remainingRels := expectedRels.Copy()
			require.Equal(totalToWrite, expectedRels.Size())
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
					require.LessOrEqual(uint64(len(batch.Relationships)), uint64(tc.batchSize))
					require.NotNil(batch.AfterResultCursor)
					require.NotEmpty(batch.AfterResultCursor.Token)

					cursor = batch.AfterResultCursor
					totalRead += len(batch.Relationships)

					for _, rel := range batch.Relationships {
						remainingRels.Remove(tuple.MustV1RelString(rel))
					}
				}

				cancel()
			}

			require.Equal(totalToWrite, totalRead)
			require.True(remainingRels.IsEmpty(), "rels were not exported %#v", remainingRels.List())
		})
	}
}

func TestBulkExportRelationshipsWithFilter(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		filter        *v1.RelationshipFilter
		expectedCount int
	}{
		{
			"basic filter",
			&v1.RelationshipFilter{
				ResourceType: tf.DocumentNS.Name,
			},
			625,
		},
		{
			"filter by resource ID",
			&v1.RelationshipFilter{
				OptionalResourceId: "12",
			},
			1,
		},
		{
			"filter by resource ID prefix",
			&v1.RelationshipFilter{
				OptionalResourceIdPrefix: "1",
			},
			111,
		},
		{
			"filter by resource ID prefix and resource type",
			&v1.RelationshipFilter{
				ResourceType:             tf.DocumentNS.Name,
				OptionalResourceIdPrefix: "1",
			},
			69,
		},
		{
			"filter by invalid resource type",
			&v1.RelationshipFilter{
				ResourceType: "invalid",
			},
			0,
		},
	}

	batchSize := uint32(14)

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithSchema)
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
				{tf.DocumentNS.Name, "caveated_viewer"},
				{tf.DocumentNS.Name, "expiring_viewer"},
			}

			expectedRels := set.NewStringSetWithSize(1000)
			batch := make([]*v1.Relationship, 1000)
			for i := range batch {
				nsAndRel := nsAndRels[i%len(nsAndRels)]
				v1rel := relationshipForBulkTesting(nsAndRel, i)
				batch[i] = v1rel

				if tc.filter != nil {
					filter, err := datastore.RelationshipsFilterFromPublicFilter(tc.filter)
					require.NoError(err)
					if !filter.Test(tuple.FromV1Relationship(v1rel)) {
						continue
					}
				}

				expectedRels.Add(tuple.MustV1RelString(v1rel))
			}

			require.Equal(tc.expectedCount, expectedRels.Size())

			ctx := context.Background()
			writer, err := client.BulkImportRelationships(ctx)
			require.NoError(err)

			require.NoError(writer.Send(&v1.BulkImportRelationshipsRequest{
				Relationships: batch,
			}))

			_, err = writer.CloseAndRecv()
			require.NoError(err)

			var totalRead uint64
			remainingRels := expectedRels.Copy()
			var cursor *v1.Cursor

			foundRels := mapz.NewSet[string]()
			for {
				streamCtx, cancel := context.WithCancel(ctx)

				stream, err := client.BulkExportRelationships(streamCtx, &v1.BulkExportRelationshipsRequest{
					OptionalRelationshipFilter: tc.filter,
					OptionalLimit:              batchSize,
					OptionalCursor:             cursor,
				})
				require.NoError(err)

				batch, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					cancel()
					break
				}

				require.NoError(err)
				relLength, err := safecast.ToUint32(len(batch.Relationships))
				require.NoError(err)
				require.LessOrEqual(relLength, batchSize)
				require.NotNil(batch.AfterResultCursor)
				require.NotEmpty(batch.AfterResultCursor.Token)

				cursor = batch.AfterResultCursor
				totalRead += uint64(len(batch.Relationships))

				for _, rel := range batch.Relationships {
					if tc.filter != nil {
						filter, err := datastore.RelationshipsFilterFromPublicFilter(tc.filter)
						require.NoError(err)
						require.True(filter.Test(tuple.FromV1Relationship(rel)), "relationship did not match filter: %s", rel)
					}

					require.True(remainingRels.Has(tuple.MustV1RelString(rel)), "relationship was not expected or was repeated: %s", rel)
					remainingRels.Remove(tuple.MustV1RelString(rel))
					foundRels.Add(tuple.MustV1RelString(rel))
				}

				cancel()
			}

			// These are statically defined.
			expectedCount, _ := safecast.ToUint64(tc.expectedCount)
			require.Equal(expectedCount, totalRead, "found: %v", foundRels.AsSlice())
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
	defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)

	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.StandardDatastoreWithCaveatedData)
	client := v1.NewExperimentalServiceClient(conn)
	defer cleanup()

	testCases := []struct {
		name     string
		requests []string
		response []bulkCheckTest
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
		},
		{
			name: "namespace validation",
			requests: []string{
				"document:masterplan#view@fake:fake",
				"fake:fake#fake@user:eng_lead",
			},
			response: []bulkCheckTest{
				{
					req: "document:masterplan#view@fake:fake",
					err: namespace.NewNamespaceNotFoundErr("fake"),
				},
				{
					req: "fake:fake#fake@user:eng_lead",
					err: namespace.NewNamespaceNotFoundErr("fake"),
				},
			},
		},
		{
			name: "chunking test",
			requests: (func() []string {
				toReturn := make([]string, 0, defaultFilterMaximumIDCountForTest+5)
				for i := 0; i < int(defaultFilterMaximumIDCountForTest+5); i++ {
					toReturn = append(toReturn, fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i))
				}

				return toReturn
			})(),
			response: (func() []bulkCheckTest {
				toReturn := make([]bulkCheckTest, 0, defaultFilterMaximumIDCountForTest+5)
				for i := 0; i < int(defaultFilterMaximumIDCountForTest+5); i++ {
					toReturn = append(toReturn, bulkCheckTest{
						req:  fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i),
						resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
					})
				}

				return toReturn
			})(),
		},
		{
			name: "chunking test with errors",
			requests: (func() []string {
				toReturn := make([]string, 0, defaultFilterMaximumIDCountForTest+6)
				toReturn = append(toReturn, `nondoc:masterplan#view@user:eng_lead`)

				for i := 0; i < int(defaultFilterMaximumIDCountForTest+5); i++ {
					toReturn = append(toReturn, fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i))
				}

				return toReturn
			})(),
			response: (func() []bulkCheckTest {
				toReturn := make([]bulkCheckTest, 0, defaultFilterMaximumIDCountForTest+6)
				toReturn = append(toReturn, bulkCheckTest{
					req: `nondoc:masterplan#view@user:eng_lead`,
					err: namespace.NewNamespaceNotFoundErr("nondoc"),
				})

				for i := 0; i < int(defaultFilterMaximumIDCountForTest+5); i++ {
					toReturn = append(toReturn, bulkCheckTest{
						req:  fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i),
						resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
					})
				}

				return toReturn
			})(),
		},
		{
			name: "same resource and permission with same subject, repeated",
			requests: []string{
				`document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
				`document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
			},
			response: []bulkCheckTest{
				{
					req:  `document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					req:  `document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
			},
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
				reqRel := tuple.MustParse(r.req)
				resp := &v1.BulkCheckPermissionPair_Item{
					Item: &v1.BulkCheckPermissionResponseItem{
						Permissionship: r.resp,
					},
				}

				rel := stringz.Default(reqRel.Subject.Relation, "", tuple.Ellipsis)
				pair := &v1.BulkCheckPermissionPair{
					Request: &v1.BulkCheckPermissionRequestItem{
						Resource: &v1.ObjectReference{
							ObjectType: reqRel.Resource.ObjectType,
							ObjectId:   reqRel.Resource.ObjectID,
						},
						Permission: reqRel.Resource.Relation,
						Subject: &v1.SubjectReference{
							Object: &v1.ObjectReference{
								ObjectType: reqRel.Subject.ObjectType,
								ObjectId:   reqRel.Subject.ObjectID,
							},
							OptionalRelation: rel,
						},
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

			testutil.RequireProtoSlicesEqual(t, expected, actual.Pairs, nil, "response bulk check pairs did not match")
		})
	}
}

func relToBulkRequestItem(rel string) *v1.BulkCheckPermissionRequestItem {
	r := tuple.MustParse(rel)
	subjectRel := stringz.Default(r.Subject.Relation, "", tuple.Ellipsis)

	item := &v1.BulkCheckPermissionRequestItem{
		Resource: &v1.ObjectReference{
			ObjectType: r.Resource.ObjectType,
			ObjectId:   r.Resource.ObjectID,
		},
		Permission: r.Resource.Relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: r.Subject.ObjectType,
				ObjectId:   r.Subject.ObjectID,
			},
			OptionalRelation: subjectRel,
		},
	}
	if r.OptionalCaveat != nil {
		item.Context = r.OptionalCaveat.Context
	}
	return item
}

func TestExperimentalSchemaDiff(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	expClient := v1.NewExperimentalServiceClient(conn)
	schemaClient := v1.NewSchemaServiceClient(conn)
	defer cleanup()

	testCases := []struct {
		name             string
		existingSchema   string
		comparisonSchema string
		expectedError    string
		expectedCode     codes.Code
		expectedResponse *v1.ExperimentalDiffSchemaResponse
	}{
		{
			name:             "no changes",
			existingSchema:   `definition user {}`,
			comparisonSchema: `definition user {}`,
			expectedResponse: &v1.ExperimentalDiffSchemaResponse{},
		},
		{
			name:             "addition from existing schema",
			existingSchema:   `definition user {}`,
			comparisonSchema: `definition user {} definition document {}`,
			expectedResponse: &v1.ExperimentalDiffSchemaResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_DefinitionAdded{
							DefinitionAdded: &v1.ExpDefinition{
								Name:    "document",
								Comment: "",
							},
						},
					},
				},
			},
		},
		{
			name:             "removal from existing schema",
			existingSchema:   `definition user {} definition document {}`,
			comparisonSchema: `definition user {}`,
			expectedResponse: &v1.ExperimentalDiffSchemaResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_DefinitionRemoved{
							DefinitionRemoved: &v1.ExpDefinition{
								Name:    "document",
								Comment: "",
							},
						},
					},
				},
			},
		},
		{
			name:             "invalid comparison schema",
			existingSchema:   `definition user {}`,
			comparisonSchema: `definition user { invalid`,
			expectedCode:     codes.InvalidArgument,
			expectedError:    "Expected end of statement or definition, found: TokenTypeIdentifier",
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Write the existing schema.
			_, err := schemaClient.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
				Schema: tt.existingSchema,
			})
			require.NoError(t, err)

			actual, err := expClient.ExperimentalDiffSchema(context.Background(), &v1.ExperimentalDiffSchemaRequest{
				ComparisonSchema: tt.comparisonSchema,
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
				},
			})

			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
				grpcutil.RequireStatus(t, tt.expectedCode, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, actual.ReadAt)
				actual.ReadAt = nil

				testutil.RequireProtoEqual(t, tt.expectedResponse, actual, "mismatch in response")
			}
		})
	}
}

func TestExperimentalReflectSchema(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	expClient := v1.NewExperimentalServiceClient(conn)
	schemaClient := v1.NewSchemaServiceClient(conn)
	defer cleanup()

	testCases := []struct {
		name             string
		schema           string
		filters          []*v1.ExpSchemaFilter
		expectedCode     codes.Code
		expectedError    string
		expectedResponse *v1.ExperimentalReflectSchemaResponse
	}{
		{
			name:   "simple schema",
			schema: `definition user {}`,
			expectedResponse: &v1.ExperimentalReflectSchemaResponse{
				Definitions: []*v1.ExpDefinition{
					{
						Name:    "user",
						Comment: "",
					},
				},
			},
		},
		{
			name: "schema with comment",
			schema: `// this is a user
definition user {}`,
			expectedResponse: &v1.ExperimentalReflectSchemaResponse{
				Definitions: []*v1.ExpDefinition{
					{
						Name:    "user",
						Comment: "// this is a user",
					},
				},
			},
		},
		{
			name:   "invalid filter",
			schema: `definition user {}`,
			filters: []*v1.ExpSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalCaveatNameFilter:     "invalid",
				},
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "cannot filter by both definition and caveat name",
		},
		{
			name:   "another invalid filter",
			schema: `definition user {}`,
			filters: []*v1.ExpSchemaFilter{
				{
					OptionalRelationNameFilter: "doc",
				},
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "relation name match requires definition name match",
		},
		{
			name: "full schema",
			schema: `
				/** user represents a user */
				definition user {}

				/** group represents a group */
				definition group {
					relation direct_member: user | group#member
					relation admin: user
					permission member = direct_member + admin
				}

				/** somecaveat is a caveat */
				caveat somecaveat(first int, second string) {
					first == 1 && second == "two"
				}

				/** document is a protected document */
				definition document {
					// editor is a relation
					relation editor: user | group#member
					relation viewer: user | user with somecaveat | group#member | user:*

					// read all the things
					permission read = viewer + editor
				}
			`,
			expectedResponse: &v1.ExperimentalReflectSchemaResponse{
				Definitions: []*v1.ExpDefinition{
					{
						Name:    "document",
						Comment: "/** document is a protected document */",
						Relations: []*v1.ExpRelation{
							{
								Name:                 "editor",
								Comment:              "// editor is a relation",
								ParentDefinitionName: "document",
								SubjectTypes: []*v1.ExpTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "group",
										Typeref: &v1.ExpTypeReference_OptionalRelationName{
											OptionalRelationName: "member",
										},
									},
								},
							},
							{
								Name:                 "viewer",
								Comment:              "",
								ParentDefinitionName: "document",
								SubjectTypes: []*v1.ExpTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "user",
										OptionalCaveatName:    "somecaveat",
										Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "group",
										Typeref: &v1.ExpTypeReference_OptionalRelationName{
											OptionalRelationName: "member",
										},
									},
									{
										SubjectDefinitionName: "user",
										Typeref: &v1.ExpTypeReference_IsPublicWildcard{
											IsPublicWildcard: true,
										},
									},
								},
							},
						},
						Permissions: []*v1.ExpPermission{
							{
								Name:                 "read",
								Comment:              "// read all the things",
								ParentDefinitionName: "document",
							},
						},
					},
					{
						Name:    "group",
						Comment: "/** group represents a group */",
						Relations: []*v1.ExpRelation{
							{
								Name:                 "direct_member",
								Comment:              "",
								ParentDefinitionName: "group",
								SubjectTypes: []*v1.ExpTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "group",
										Typeref:               &v1.ExpTypeReference_OptionalRelationName{OptionalRelationName: "member"},
									},
								},
							},
							{
								Name:                 "admin",
								Comment:              "",
								ParentDefinitionName: "group",
								SubjectTypes: []*v1.ExpTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
									},
								},
							},
						},
						Permissions: []*v1.ExpPermission{
							{
								Name:                 "member",
								Comment:              "",
								ParentDefinitionName: "group",
							},
						},
					},
					{
						Name:    "user",
						Comment: "/** user represents a user */",
					},
				},
				Caveats: []*v1.ExpCaveat{
					{
						Name:       "somecaveat",
						Comment:    "/** somecaveat is a caveat */",
						Expression: "first == 1 && second == \"two\"",
						Parameters: []*v1.ExpCaveatParameter{
							{
								Name:             "first",
								Type:             "int",
								ParentCaveatName: "somecaveat",
							},
							{
								Name:             "second",
								Type:             "string",
								ParentCaveatName: "somecaveat",
							},
						},
					},
				},
			},
		},
		{
			name: "full schema with definition filter",
			schema: `
				/** user represents a user */
				definition user {}

				/** group represents a group */
				definition group {
					relation direct_member: user | group#member
					relation admin: user
					permission member = direct_member + admin
				}

				caveat somecaveat(first int, second string) {
					first == 1 && second == "two"
				}

				/** document is a protected document */
				definition document {
					// editor is a relation
					relation editor: user | group#member
					relation viewer: user | user with somecaveat | group#member

					// read all the things
					permission read = viewer + editor
				}
			`,
			filters: []*v1.ExpSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
				},
			},
			expectedResponse: &v1.ExperimentalReflectSchemaResponse{
				Definitions: []*v1.ExpDefinition{
					{
						Name:    "document",
						Comment: "/** document is a protected document */",
						Relations: []*v1.ExpRelation{
							{
								Name:                 "editor",
								Comment:              "// editor is a relation",
								ParentDefinitionName: "document",
								SubjectTypes: []*v1.ExpTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "group",
										Typeref: &v1.ExpTypeReference_OptionalRelationName{
											OptionalRelationName: "member",
										},
									},
								},
							},
							{
								Name:                 "viewer",
								Comment:              "",
								ParentDefinitionName: "document",
								SubjectTypes: []*v1.ExpTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "user",
										OptionalCaveatName:    "somecaveat",
										Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "group",
										Typeref: &v1.ExpTypeReference_OptionalRelationName{
											OptionalRelationName: "member",
										},
									},
								},
							},
						},
						Permissions: []*v1.ExpPermission{
							{
								Name:                 "read",
								Comment:              "// read all the things",
								ParentDefinitionName: "document",
							},
						},
					},
				},
			},
		},
		{
			name: "full schema with definition, relation and permission filters",
			schema: `
				/** user represents a user */
				definition user {}

				/** group represents a group */
				definition group {
					relation direct_member: user | group#member
					relation admin: user
					permission member = direct_member + admin
				}

				caveat somecaveat(first int, second string) {
					first == 1 && second == "two"
				}

				/** document is a protected document */
				definition document {
					// editor is a relation
					relation editor: user | group#member
					relation viewer: user | user with somecaveat | group#member

					// read all the things
					permission read = viewer + editor
				}
			`,
			filters: []*v1.ExpSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalRelationNameFilter:   "viewer",
				},
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalPermissionNameFilter: "read",
				},
			},
			expectedResponse: &v1.ExperimentalReflectSchemaResponse{
				Definitions: []*v1.ExpDefinition{
					{
						Name:    "document",
						Comment: "/** document is a protected document */",
						Relations: []*v1.ExpRelation{
							{
								Name:                 "viewer",
								Comment:              "",
								ParentDefinitionName: "document",
								SubjectTypes: []*v1.ExpTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "user",
										OptionalCaveatName:    "somecaveat",
										Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "group",
										Typeref: &v1.ExpTypeReference_OptionalRelationName{
											OptionalRelationName: "member",
										},
									},
								},
							},
						},
						Permissions: []*v1.ExpPermission{
							{
								Name:                 "read",
								Comment:              "// read all the things",
								ParentDefinitionName: "document",
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Write the schema.
			_, err := schemaClient.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
				Schema: tt.schema,
			})
			require.NoError(t, err)

			actual, err := expClient.ExperimentalReflectSchema(context.Background(), &v1.ExperimentalReflectSchemaRequest{
				OptionalFilters: tt.filters,
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
				},
			})

			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
				grpcutil.RequireStatus(t, tt.expectedCode, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, actual.ReadAt)
				actual.ReadAt = nil

				testutil.RequireProtoEqual(t, tt.expectedResponse, actual, "mismatch in response")
			}
		})
	}
}

func TestExperimentalDependentRelations(t *testing.T) {
	tcs := []struct {
		name             string
		schema           string
		definitionName   string
		permissionName   string
		expectedCode     codes.Code
		expectedError    string
		expectedResponse []*v1.ExpRelationReference
	}{
		{
			name:           "invalid definition",
			schema:         `definition user {}`,
			definitionName: "invalid",
			expectedCode:   codes.FailedPrecondition,
			expectedError:  "object definition `invalid` not found",
		},
		{
			name:           "invalid permission",
			schema:         `definition user {}`,
			definitionName: "user",
			permissionName: "invalid",
			expectedCode:   codes.FailedPrecondition,
			expectedError:  "permission `invalid` not found",
		},
		{
			name: "specified relation",
			schema: `
				definition user {}

				definition document {
					relation editor: user
				}
			`,
			definitionName: "document",
			permissionName: "editor",
			expectedCode:   codes.InvalidArgument,
			expectedError:  "is not a permission",
		},
		{
			name: "simple schema",
			schema: `
				definition user {}

				definition document {
					relation unused: user
					relation editor: user
					relation viewer: user
					permission view = viewer + editor
				}
			`,
			definitionName: "document",
			permissionName: "view",
			expectedResponse: []*v1.ExpRelationReference{
				{
					DefinitionName: "document",
					RelationName:   "editor",
					IsPermission:   false,
				},
				{
					DefinitionName: "document",
					RelationName:   "viewer",
					IsPermission:   false,
				},
			},
		},
		{
			name: "schema with nested relation",
			schema: `
				definition user {}

				definition group {
					relation direct_member: user | group#member
					relation admin: user
					permission member = direct_member + admin
				}

				definition document {
					relation unused: user
					relation viewer: user | group#member
					permission view = viewer
				}
			`,
			definitionName: "document",
			permissionName: "view",
			expectedResponse: []*v1.ExpRelationReference{
				{
					DefinitionName: "document",
					RelationName:   "viewer",
					IsPermission:   false,
				},
				{
					DefinitionName: "group",
					RelationName:   "admin",
					IsPermission:   false,
				},
				{
					DefinitionName: "group",
					RelationName:   "direct_member",
					IsPermission:   false,
				},
				{
					DefinitionName: "group",
					RelationName:   "member",
					IsPermission:   true,
				},
			},
		},
		{
			name: "schema with arrow",
			schema: `
				definition user {}

				definition folder {
					relation alsounused: user
					relation viewer: user
					permission view = viewer
				}

				definition document {
					relation unused: user
					relation parent: folder
					relation viewer: user
					permission view = viewer + parent->view
				}
			`,
			definitionName: "document",
			permissionName: "view",
			expectedResponse: []*v1.ExpRelationReference{
				{
					DefinitionName: "document",
					RelationName:   "parent",
					IsPermission:   false,
				},
				{
					DefinitionName: "document",
					RelationName:   "viewer",
					IsPermission:   false,
				},
				{
					DefinitionName: "folder",
					RelationName:   "view",
					IsPermission:   true,
				},
				{
					DefinitionName: "folder",
					RelationName:   "viewer",
					IsPermission:   false,
				},
			},
		},
		{
			name: "empty response",
			schema: `
				definition user {}

				definition folder {
					relation alsounused: user
					relation viewer: user
					permission view = viewer
				}

				definition document {
					relation unused: user
					relation parent: folder
					relation viewer: user
					permission view = viewer + parent->view
					permission empty = nil
				}
			`,
			definitionName:   "document",
			permissionName:   "empty",
			expectedResponse: []*v1.ExpRelationReference{},
		},
		{
			name: "empty definition",
			schema: `
				definition user {}
			`,
			definitionName: "",
			permissionName: "empty",
			expectedCode:   codes.FailedPrecondition,
			expectedError:  "object definition `` not found",
		},
		{
			name: "empty permission",
			schema: `
				definition user {}
			`,
			definitionName: "user",
			permissionName: "",
			expectedCode:   codes.FailedPrecondition,
			expectedError:  "permission `` not found",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
			expClient := v1.NewExperimentalServiceClient(conn)
			schemaClient := v1.NewSchemaServiceClient(conn)
			defer cleanup()

			// Write the schema.
			_, err := schemaClient.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
				Schema: tc.schema,
			})
			require.NoError(t, err)

			actual, err := expClient.ExperimentalDependentRelations(context.Background(), &v1.ExperimentalDependentRelationsRequest{
				DefinitionName: tc.definitionName,
				PermissionName: tc.permissionName,
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
				},
			})

			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				grpcutil.RequireStatus(t, tc.expectedCode, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, actual.ReadAt)
				actual.ReadAt = nil

				testutil.RequireProtoEqual(t, &v1.ExperimentalDependentRelationsResponse{
					Relations: tc.expectedResponse,
				}, actual, "mismatch in response")
			}
		})
	}
}

func TestExperimentalComputablePermissions(t *testing.T) {
	tcs := []struct {
		name             string
		schema           string
		definitionName   string
		relationName     string
		filter           string
		expectedCode     codes.Code
		expectedError    string
		expectedResponse []*v1.ExpRelationReference
	}{
		{
			name:           "invalid definition",
			schema:         `definition user {}`,
			definitionName: "invalid",
			expectedCode:   codes.FailedPrecondition,
			expectedError:  "object definition `invalid` not found",
		},
		{
			name:           "invalid relation",
			schema:         `definition user {}`,
			definitionName: "user",
			relationName:   "invalid",
			expectedCode:   codes.FailedPrecondition,
			expectedError:  "relation/permission `invalid` not found",
		},
		{
			name: "basic",
			schema: `
				definition user {}

				definition document {
					relation unused: user
					relation editor: user
					relation viewer: user
					permission view = viewer + editor
					permission another = unused
				}`,
			definitionName: "user",
			relationName:   "",
			expectedResponse: []*v1.ExpRelationReference{
				{
					DefinitionName: "document",
					RelationName:   "another",
					IsPermission:   true,
				},
				{
					DefinitionName: "document",
					RelationName:   "editor",
					IsPermission:   false,
				},
				{
					DefinitionName: "document",
					RelationName:   "unused",
					IsPermission:   false,
				},
				{
					DefinitionName: "document",
					RelationName:   "view",
					IsPermission:   true,
				},
				{
					DefinitionName: "document",
					RelationName:   "viewer",
					IsPermission:   false,
				},
			},
		},
		{
			name: "filtered",
			schema: `
				definition user {}

				definition folder {
					relation viewer: user
				}

				definition document {
					relation unused: user
					relation editor: user
					relation viewer: user
					permission view = viewer + editor
					permission another = unused
				}`,
			definitionName: "user",
			relationName:   "",
			filter:         "folder",
			expectedResponse: []*v1.ExpRelationReference{
				{
					DefinitionName: "folder",
					RelationName:   "viewer",
					IsPermission:   false,
				},
			},
		},
		{
			name: "basic relation",
			schema: `
				definition user {}

				definition document {
					relation unused: user
					relation editor: user
					relation viewer: user
					permission view = viewer + editor
					permission another = unused
				}`,
			definitionName: "document",
			relationName:   "viewer",
			expectedResponse: []*v1.ExpRelationReference{
				{
					DefinitionName: "document",
					RelationName:   "view",
					IsPermission:   true,
				},
			},
		},
		{
			name: "multiple permissions",
			schema: `
				definition user {}

				definition document {
					relation unused: user
					relation editor: user
					relation viewer: user
					permission view = viewer + editor
					permission only_view = viewer
					permission another = unused
				}`,
			definitionName: "document",
			relationName:   "viewer",
			expectedResponse: []*v1.ExpRelationReference{
				{
					DefinitionName: "document",
					RelationName:   "only_view",
					IsPermission:   true,
				},
				{
					DefinitionName: "document",
					RelationName:   "view",
					IsPermission:   true,
				},
			},
		},
		{
			name: "empty response",
			schema: `
				definition user {}

				definition document {
					relation unused: user
					permission empty = nil
				}
			`,
			definitionName:   "document",
			relationName:     "unused",
			expectedResponse: []*v1.ExpRelationReference{},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
			expClient := v1.NewExperimentalServiceClient(conn)
			schemaClient := v1.NewSchemaServiceClient(conn)
			defer cleanup()

			// Write the schema.
			_, err := schemaClient.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
				Schema: tc.schema,
			})
			require.NoError(t, err)

			actual, err := expClient.ExperimentalComputablePermissions(context.Background(), &v1.ExperimentalComputablePermissionsRequest{
				DefinitionName:               tc.definitionName,
				RelationName:                 tc.relationName,
				OptionalDefinitionNameFilter: tc.filter,
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
				},
			})

			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				grpcutil.RequireStatus(t, tc.expectedCode, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, actual.ReadAt)
				actual.ReadAt = nil

				testutil.RequireProtoEqual(t, &v1.ExperimentalComputablePermissionsResponse{
					Permissions: tc.expectedResponse,
				}, actual, "mismatch in response")
			}
		})
	}
}

func TestExperimentalCountRelationships(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	expClient := v1.NewExperimentalServiceClient(conn)
	schemaClient := v1.NewSchemaServiceClient(conn)
	permsClient := v1.NewPermissionsServiceClient(conn)
	defer cleanup()

	// Write the schema.
	_, err := schemaClient.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition user {}
		
		definition document {
			relation viewer: user
			relation editor: user
			permission view = viewer + editor
		}
		`,
	})
	require.NoError(t, err)

	// Write some relationships.
	updates, err := tuple.UpdatesToV1RelationshipUpdates([]tuple.RelationshipUpdate{
		tuple.Create(tuple.MustParse("document:doc1#viewer@user:alice")),
		tuple.Create(tuple.MustParse("document:doc1#viewer@user:bob")),
		tuple.Create(tuple.MustParse("document:doc1#viewer@user:charlie")),
		tuple.Create(tuple.MustParse("document:doc1#editor@user:alice")),
		tuple.Create(tuple.MustParse("document:doc1#editor@user:bob")),

		tuple.Create(tuple.MustParse("document:doc2#viewer@user:alice")),
		tuple.Create(tuple.MustParse("document:doc2#viewer@user:adam")),

		tuple.Create(tuple.MustParse("document:adoc#viewer@user:alice")),
		tuple.Create(tuple.MustParse("document:anotherdoc#viewer@user:alice")),
	})
	require.NoError(t, err)

	_, err = permsClient.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: updates,
	})
	require.NoError(t, err)

	// Try to read the count on an unregistered filter.
	_, err = expClient.ExperimentalCountRelationships(context.Background(), &v1.ExperimentalCountRelationshipsRequest{
		Name: "unregistered",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "counter with name `unregistered` not found")
	grpcutil.RequireStatus(t, codes.FailedPrecondition, err)

	// Register some filters.
	_, err = expClient.ExperimentalRegisterRelationshipCounter(context.Background(), &v1.ExperimentalRegisterRelationshipCounterRequest{
		Name: "somedocfilter",
		RelationshipFilter: &v1.RelationshipFilter{
			ResourceType: "document",
		},
	})
	require.NoError(t, err)

	_, err = expClient.ExperimentalRegisterRelationshipCounter(context.Background(), &v1.ExperimentalRegisterRelationshipCounterRequest{
		Name: "somedocfilter",
		RelationshipFilter: &v1.RelationshipFilter{
			ResourceType: "document",
		},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "counter with name `somedocfilter` already registered")
	grpcutil.RequireStatus(t, codes.FailedPrecondition, err)

	_, err = expClient.ExperimentalRegisterRelationshipCounter(context.Background(), &v1.ExperimentalRegisterRelationshipCounterRequest{
		Name: "someotherfilter",
		RelationshipFilter: &v1.RelationshipFilter{
			ResourceType:     "document",
			OptionalRelation: "viewer",
		},
	})
	require.NoError(t, err)

	// Read the counts on the registered filers.
	actual, err := expClient.ExperimentalCountRelationships(context.Background(), &v1.ExperimentalCountRelationshipsRequest{
		Name: "somedocfilter",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(9), actual.GetReadCounterValue().RelationshipCount)
	require.NotNil(t, actual.GetReadCounterValue().ReadAt)

	actual, err = expClient.ExperimentalCountRelationships(context.Background(), &v1.ExperimentalCountRelationshipsRequest{
		Name: "someotherfilter",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(7), actual.GetReadCounterValue().RelationshipCount)

	// Register one more filter.
	_, err = expClient.ExperimentalRegisterRelationshipCounter(context.Background(), &v1.ExperimentalRegisterRelationshipCounterRequest{
		Name: "somethirdfilter",
		RelationshipFilter: &v1.RelationshipFilter{
			OptionalResourceIdPrefix: "a",
		},
	})
	require.NoError(t, err)

	// Get the count.
	actual, err = expClient.ExperimentalCountRelationships(context.Background(), &v1.ExperimentalCountRelationshipsRequest{
		Name: "somethirdfilter",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), actual.GetReadCounterValue().RelationshipCount)
}
