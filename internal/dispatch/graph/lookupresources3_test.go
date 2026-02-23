package graph

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSimpleLookupResources3(t *testing.T) {
	// FIXME marking this parallel makes goleak detect a leaked goroutine
	testCases := []struct {
		start                 tuple.RelationReference
		target                tuple.ObjectAndRelation
		expectedResources     []*v1.PossibleResource
		expectedDispatchCount uint32
		expectedDepthRequired uint32
	}{
		{
			RR("document", "view"),
			ONR("user", "unknown", "..."),
			[]*v1.PossibleResource{},
			0,
			0,
		},
		{
			RR("document", "view"),
			ONR("user", "eng_lead", "..."),
			[]*v1.PossibleResource{
				possibleRes("masterplan"),
			},
			3,
			3,
		},
		{
			RR("document", "owner"),
			ONR("user", "product_manager", "..."),
			[]*v1.PossibleResource{
				possibleRes("masterplan"),
			},
			3,
			2,
		},
		{
			RR("document", "view"),
			ONR("user", "legal", "..."),
			[]*v1.PossibleResource{
				possibleRes("companyplan"),
				possibleRes("masterplan"),
			},
			7,
			5,
		},
		{
			RR("document", "view_and_edit"),
			ONR("user", "multiroleguy", "..."),
			[]*v1.PossibleResource{
				possibleRes("specialplan"),
			},
			8,
			5,
		},
		{
			RR("folder", "view"),
			ONR("user", "owner", "..."),
			[]*v1.PossibleResource{
				possibleRes("strategy"),
				possibleRes("company"),
			},
			9,
			5,
		},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"%s#%s->%s",
			tc.start.ObjectType,
			tc.start.Relation,
			tuple.StringONR(tc.target),
		)

		tc := tc
		t.Run(name, func(t *testing.T) {
			defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)

			require := require.New(t)
			ctx, dispatcher, revision := newLocalDispatcher(t)
			defer dispatcher.Close()

			stream := dispatch.NewCloningCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)
			err := dispatcher.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
				ResourceRelation: tc.start.ToCoreRR(),
				SubjectRelation:  RR(tc.target.ObjectType, tc.target.Relation).ToCoreRR(),
				SubjectIds:       []string{tc.target.ObjectID},
				TerminalSubject:  tc.target.ToCoreONR(),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				OptionalLimit: veryLargeLimit,
			}, stream)

			require.NoError(err)

			foundResources := processResults3(stream)
			require.ElementsMatch(tc.expectedResources, foundResources, "Found: %v, Expected: %v", foundResources, tc.expectedResources)

			// We have to sleep a while to let the cache converge:
			// https://github.com/outcaste-io/ristretto/blob/01b9f37dd0fd453225e042d6f3a27cd14f252cd0/cache_test.go#L17
			time.Sleep(10 * time.Millisecond)

			// Run again with the cache available.
			stream = dispatch.NewCloningCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)
			err = dispatcher.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
				ResourceRelation: tc.start.ToCoreRR(),
				SubjectRelation:  RR(tc.target.ObjectType, tc.target.Relation).ToCoreRR(),
				SubjectIds:       []string{tc.target.ObjectID},
				TerminalSubject:  tc.target.ToCoreONR(),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				OptionalLimit: veryLargeLimit,
			}, stream)
			dispatcher.Close()

			require.NoError(err)

			foundResources = processResults3(stream)
			require.ElementsMatch(tc.expectedResources, foundResources, "Found: %v, Expected: %v", foundResources, tc.expectedResources)
		})
	}
}

func TestSimpleLookupResourcesWithCursor3(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		subject        string
		expectedFirst  []string
		expectedSecond []string
	}{
		{
			subject:        "owner",
			expectedFirst:  []string{"ownerplan"},
			expectedSecond: []string{"companyplan", "masterplan", "ownerplan"},
		},
		{
			subject:        "chief_financial_officer",
			expectedFirst:  []string{"healthplan"},
			expectedSecond: []string{"healthplan", "masterplan"},
		},
		{
			subject:        "auditor",
			expectedFirst:  []string{"companyplan"},
			expectedSecond: []string{"companyplan", "masterplan"},
		},
	} {
		t.Run(tc.subject, func(t *testing.T) {
			t.Parallel()

			require := require.New(t)
			ctx, dispatcher, revision := newLocalDispatcher(t)
			defer dispatcher.Close()

			found := mapz.NewSet[string]()

			stream := dispatch.NewCloningCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)
			err := dispatcher.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
				ResourceRelation: RR("document", "view").ToCoreRR(),
				SubjectRelation:  RR("user", "...").ToCoreRR(),
				SubjectIds:       []string{tc.subject},
				TerminalSubject:  ONR("user", tc.subject, "...").ToCoreONR(),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				OptionalLimit: 1,
			}, stream)

			require.NoError(err)

			require.Len(stream.Results(), 1)

			found.Insert(stream.Results()[0].Items[0].ResourceId)
			require.Equal(tc.expectedFirst, found.AsSlice())

			cursor := stream.Results()[0].Items[0].AfterResponseCursorSections
			require.NotNil(cursor)

			stream = dispatch.NewCloningCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)
			err = dispatcher.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
				ResourceRelation: RR("document", "view").ToCoreRR(),
				SubjectRelation:  RR("user", "...").ToCoreRR(),
				SubjectIds:       []string{tc.subject},
				TerminalSubject:  ONR("user", tc.subject, "...").ToCoreONR(),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				OptionalCursor: cursor,
				OptionalLimit:  2,
			}, stream)

			require.NoError(err)

			for _, result := range stream.Results() {
				for _, item := range result.Items {
					found.Insert(item.ResourceId)
				}
			}

			foundResults := found.AsSlice()
			slices.Sort(foundResults)

			require.Equal(tc.expectedSecond, foundResults)
		})
	}
}

func processResults3(stream *dispatch.CloningCollectingDispatchStream[*v1.DispatchLookupResources3Response]) []*v1.PossibleResource {
	foundResources := []*v1.PossibleResource{} //nolint: prealloc  // we can't easily know the length of foundResources
	for _, result := range stream.Results() {
		for _, item := range result.Items {
			foundResources = append(foundResources, &v1.PossibleResource{
				ResourceId:           item.ResourceId,
				ForSubjectIds:        nil,
				MissingContextParams: item.MissingContextParams,
			})
		}
	}
	return foundResources
}

func TestMaxDepthLookup3(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatcher, err := NewLocalOnlyDispatcher(MustNewDefaultDispatcherParametersForTesting())
	require.NoError(err)
	defer dispatcher.Close()

	ctx := datalayer.ContextWithHandle(t.Context())
	require.NoError(datalayer.SetInContext(ctx, datalayer.NewDataLayer(ds)))
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)

	err = dispatcher.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
		ResourceRelation: RR("document", "view").ToCoreRR(),
		SubjectRelation:  RR("user", "...").ToCoreRR(),
		SubjectIds:       []string{"legal"},
		TerminalSubject:  ONR("user", "legal", "...").ToCoreONR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 0,
		},
	}, stream)

	require.Error(err)
}

func TestLookupResources3OverSchemaWithCursors(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name                  string
		schema                string
		relationships         []tuple.Relationship
		permission            tuple.RelationReference
		subject               tuple.ObjectAndRelation
		optionalCaveatContext map[string]any
		expectedResourceIDs   []string
		expectedMissingFields []string
	}{
		{
			"basic union",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer + editor
  			 }`,
			joinTuples(
				genRels("document", "viewer", "user", "tom", 1510),
				genRels("document", "editor", "user", "tom", 1510),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			genResourceIds("document", 1510),
			nil,
		},
		{
			"basic exclusion",
			`definition user {}
		
		 	 definition document {
				relation banned: user
				relation viewer: user
				permission view = viewer - banned
  			 }`,
			genRels("document", "viewer", "user", "tom", 1010),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			genResourceIds("document", 1010),
			nil,
		},
		{
			"basic intersection",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer & editor
  			 }`,
			joinTuples(
				genRels("document", "viewer", "user", "tom", 510),
				genRels("document", "editor", "user", "tom", 510),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			genResourceIds("document", 510),
			nil,
		},
		{
			"union and excluded union",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				relation banned: user
				permission can_view = viewer - banned
				permission view = can_view + editor
  			 }`,
			joinTuples(
				genRels("document", "viewer", "user", "tom", 1310),
				genRelsWithOffset("document", "editor", "user", "tom", 1250, 1200),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			genResourceIds("document", 2450),
			nil,
		},
		{
			"basic caveats",
			`definition user {}

 			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }
		
		 	 definition document {
				relation viewer: user with somecaveat
				permission view = viewer
  			 }`,
			genRelsWithCaveat("document", "viewer", "user", "tom", "somecaveat", map[string]any{"somecondition": 42}, 0, 2450),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			genResourceIds("document", 2450),
			nil,
		},
		{
			"excluded items",
			`definition user {}
		
		 	 definition document {
				relation banned: user
				relation viewer: user
				permission view = viewer - banned
  			 }`,
			joinTuples(
				genRels("document", "viewer", "user", "tom", 1310),
				genRelsWithOffset("document", "banned", "user", "tom", 1210, 100),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			genResourceIds("document", 1210),
			nil,
		},
		{
			"basic caveats with missing field",
			`definition user {}

 			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }
		
		 	 definition document {
				relation viewer: user with somecaveat
				permission view = viewer
  			 }`,
			genRelsWithCaveat("document", "viewer", "user", "tom", "somecaveat", map[string]any{}, 0, 2450),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			genResourceIds("document", 2450),
			[]string{"somecondition"},
		},
		{
			"larger arrow dispatch",
			`definition user {}
	
			 definition folder {
				relation viewer: user
			 }

		 	 definition document {
				relation folder: folder
				permission view = folder->viewer
  			 }`,
			joinTuples(
				genRels("folder", "viewer", "user", "tom", 150),
				genSubjectRels("document", "folder", "folder", "...", 150),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			genResourceIds("document", 150),
			nil,
		},
		{
			"big",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer + editor
  			 }`,
			joinTuples(
				genRels("document", "viewer", "user", "tom", 15100),
				genRels("document", "editor", "user", "tom", 15100),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			genResourceIds("document", 15100),
			nil,
		},
		{
			"arrow under intersection",
			`definition user {}
		
			 definition organization {
				relation member: user
			 }

		 	 definition document {
				relation org: organization
				relation viewer: user
				permission view = org->member & viewer
  			 }`,
			joinTuples(
				genRels("document", "viewer", "user", "tom", 510),
				genRels("document", "org", "organization", "someorg", 510),
				[]tuple.Relationship{
					tuple.MustParse("organization:someorg#member@user:tom"),
				},
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			genResourceIds("document", 510),
			nil,
		},
		{
			"all arrow",
			`definition user {}
		
			 definition folder {
				relation viewer: user
			 }

		 	 definition document {
			 	relation parent: folder
				relation viewer: user
				permission view = parent.all(viewer) + viewer
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:doc0#parent@folder:folder0"),
				tuple.MustParse("folder:folder0#viewer@user:tom"),

				tuple.MustParse("document:doc1#parent@folder:folder1-1"),
				tuple.MustParse("document:doc1#parent@folder:folder1-2"),
				tuple.MustParse("document:doc1#parent@folder:folder1-3"),
				tuple.MustParse("folder:folder1-1#viewer@user:tom"),
				tuple.MustParse("folder:folder1-2#viewer@user:tom"),
				tuple.MustParse("folder:folder1-3#viewer@user:tom"),

				tuple.MustParse("document:doc2#parent@folder:folder2-1"),
				tuple.MustParse("document:doc2#parent@folder:folder2-2"),
				tuple.MustParse("document:doc2#parent@folder:folder2-3"),
				tuple.MustParse("folder:folder2-1#viewer@user:tom"),
				tuple.MustParse("folder:folder2-2#viewer@user:tom"),

				tuple.MustParse("document:doc3#parent@folder:folder3-1"),

				tuple.MustParse("document:doc4#viewer@user:tom"),

				tuple.MustParse("document:doc5#viewer@user:fred"),
			},
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			[]string{"doc0", "doc1", "doc4"},
			nil,
		},
		{
			"indirect intersection and exclusion",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				relation banned: user
				permission indirect = viewer & editor
				permission view = indirect - banned
  			 }`,
			joinTuples(
				genRels("document", "viewer", "user", "tom", 1510),
				genRels("document", "editor", "user", "tom", 1510),
				genRelsWithOffset("document", "banned", "user", "tom", 1410, 100),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			genResourceIds("document", 1410),
			nil,
		},
		{
			"indirect intersections",
			`definition user {}
		
			 definition folder {
			 	relation viewer: user
				permission view = viewer
			 }

		 	 definition document {
				relation folder: folder
				relation editor: user
				relation admin: user
				permission indirect = folder->view & editor
				permission view = indirect & admin
  			 }`,
			joinTuples(
				[]tuple.Relationship{
					tuple.MustParse("folder:folder0#viewer@user:tom"),
				},
				genRels("document", "folder", "folder", "folder0", 1510),
				genRels("document", "editor", "user", "tom", 1510),
				genRels("document", "admin", "user", "tom", 1410),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			genResourceIds("document", 1410),
			nil,
		},
		{
			"indirect over arrow",
			`definition user {}
		
			 definition folder {
			 	relation viewer: user
				permission view = viewer
			 }

		 	 definition document {
				relation folder: folder
				relation editor: user
				relation admin: user
				permission indirect = folder->view
				permission view = indirect & admin
  			 }`,
			joinTuples(
				[]tuple.Relationship{
					tuple.MustParse("folder:folder0#viewer@user:tom"),
				},
				genRels("document", "folder", "folder", "folder0", 1510),
				genRels("document", "editor", "user", "tom", 1510),
				genRels("document", "admin", "user", "tom", 1410),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			genResourceIds("document", 1410),
			nil,
		},
		{
			"root indirect with intermediate shearing",
			`definition user {}
		
			 definition folder {
			 	relation viewer: user
				permission view = viewer
			 }

		 	 definition middle {
				relation folder: folder
				relation editor: user

				permission folder_view = folder->view
				permission indirect_view = folder_view & editor
				permission view = indirect_view
  			 }
			 	
			  definition document {
			    relation viewer: middle#view
				permission view = viewer
			  }
			 `,
			joinTuples(
				[]tuple.Relationship{
					tuple.MustParse("folder:folder0#viewer@user:tom"),
					tuple.MustParse("folder:folder1#viewer@user:tom"),
				},
				genRels("middle", "folder", "folder", "folder0", 1510),
				genRels("middle", "editor", "user", "tom", 1),
				genRelsWithCaveatAndSubjectRelation("document", "viewer", "middle", "middle-0", "view", "", nil, 0, 2000),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			nil,
			genResourceIds("document", 2000),
			nil,
		},
		{
			"indirect caveats",
			`caveat somecaveat(somevalue int) {
				somevalue == 42
			}

			definition user {}

			definition container {
				relation access: user with somecaveat 
				permission accesses = access
			}

			definition document {
				relation container: container
				relation viewer: user with somecaveat
				permission view = viewer & container->accesses
			}`,
			joinTuples(
				[]tuple.Relationship{
					tuple.MustParse("container:somecontainer#access@user:tom[somecaveat]"),
				},
				genRelsWithCaveat("document", "viewer", "user", "tom", "somecaveat", map[string]any{}, 0, 2450),
				genRels("document", "container", "container", "somecontainer", 2450),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			map[string]any{"somevalue": 42},
			genResourceIds("document", 2450),
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, pageSize := range []int{0, 104, 1023} {
				t.Run(fmt.Sprintf("ps-%d_", pageSize), func(t *testing.T) {
					t.Parallel()
					require := require.New(t)

					dispatcher, err := NewLocalOnlyDispatcher(MustNewDefaultDispatcherParametersForTesting())
					require.NoError(err)
					t.Cleanup(func() {
						dispatcher.Close()
					})

					ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
					require.NoError(err)

					ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, require)

					ctx := datalayer.ContextWithHandle(t.Context())
					require.NoError(datalayer.SetInContext(ctx, datalayer.NewDataLayer(ds)))

					var currentCursor []string
					foundResourceIDs := mapz.NewSet[string]()
					foundChunks := [][]*v1.DispatchLookupResources3Response{}
					for {
						stream := dispatch.NewCloningCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)

						uintPageSize, err := safecast.Convert[uint32](pageSize)
						require.NoError(err)

						var caveatContext *structpb.Struct
						if tc.optionalCaveatContext != nil {
							caveatContext, err = structpb.NewStruct(tc.optionalCaveatContext)
							require.NoError(err)
						}

						err = dispatcher.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
							ResourceRelation: tc.permission.ToCoreRR(),
							SubjectRelation:  RR(tc.subject.ObjectType, "...").ToCoreRR(),
							SubjectIds:       []string{tc.subject.ObjectID},
							TerminalSubject:  tc.subject.ToCoreONR(),
							Metadata: &v1.ResolverMeta{
								AtRevision:     revision.String(),
								DepthRemaining: 50,
							},
							OptionalLimit:  uintPageSize,
							OptionalCursor: currentCursor,
							Context:        caveatContext,
						}, stream)
						require.NoError(err)

						foundChunks = append(foundChunks, stream.Results())

						itemCount := 0
						for _, result := range stream.Results() {
							for _, item := range result.Items {
								require.ElementsMatch(tc.expectedMissingFields, item.MissingContextParams)
								foundResourceIDs.Insert(item.ResourceId)
								currentCursor = item.AfterResponseCursorSections
								itemCount++
							}
						}

						if pageSize > 0 {
							require.LessOrEqual(itemCount, pageSize)
						}

						if pageSize == 0 || itemCount < pageSize {
							break
						}
					}

					for _, chunk := range foundChunks[0 : len(foundChunks)-1] {
						chunkItemCount := 0
						for _, result := range chunk {
							chunkItemCount += len(result.Items)
						}
						require.Equal(pageSize, chunkItemCount)
					}

					foundResourceIDsSlice := foundResourceIDs.AsSlice()
					expectedResourceIDs := slices.Clone(tc.expectedResourceIDs)
					slices.Sort(foundResourceIDsSlice)
					slices.Sort(expectedResourceIDs)

					require.Equal(expectedResourceIDs, foundResourceIDsSlice)
				})
			}
		})
	}
}

func TestLookupResources3WithError(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatcher, err := NewLocalOnlyDispatcher(MustNewDefaultDispatcherParametersForTesting())
	require.NoError(err)
	defer dispatcher.Close()

	ctx := datalayer.ContextWithHandle(t.Context())
	cctx, cancel := context.WithCancel(ctx)
	cancel()

	require.NoError(datalayer.SetInContext(cctx, datalayer.NewDataLayer(ds)))
	stream := dispatch.NewCloningCollectingDispatchStream[*v1.DispatchLookupResources3Response](cctx)

	err = dispatcher.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
		ResourceRelation: RR("document", "view").ToCoreRR(),
		SubjectRelation:  RR("user", "...").ToCoreRR(),
		SubjectIds:       []string{"legal"},
		TerminalSubject:  ONR("user", "legal", "...").ToCoreONR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 10,
		},
	}, stream)
	require.Error(err)
}

func TestLookupResources3EnsureCheckHints(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name          string
		schema        string
		relationships []tuple.Relationship

		resourceRelation tuple.RelationReference
		subject          tuple.ObjectAndRelation

		disallowedQueries []tuple.RelationReference
		expectedResources []string
		expectedError     string
	}{
		{
			name: "basic intersection",
			schema: `definition user {}

			 definition document {
			 	relation editor: user
				relation viewer: user
				permission view = viewer & editor
			}`,
			relationships: []tuple.Relationship{
				tuple.MustParse("document:masterplan#viewer@user:tom"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
				tuple.MustParse("document:anotherplan#viewer@user:tom"),
				tuple.MustParse("document:anotherplan#editor@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("document", "viewer"),
			},
			expectedResources: []string{"masterplan", "anotherplan"},
		},
		{
			name: "basic arrow",
			schema: `definition user {}

			 definition organization {
			 	relation member: user
			 }

			 definition document {
			 	relation org: organization
			 	relation editor: user
				permission view = org->member & editor
			}`,
			relationships: []tuple.Relationship{
				tuple.MustParse("document:masterplan#org@organization:someorg"),
				tuple.MustParse("organization:someorg#member@user:tom"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("organization", "member"),
			},
			expectedResources: []string{"masterplan"},
		},
		{
			name: "basic intersection with disallowed query (sanity check to ensure test is working)",
			schema: `definition user {}

			 definition document {
			 	relation editor: user
				relation viewer: user
				permission view = viewer & editor
			}`,
			relationships: []tuple.Relationship{
				tuple.MustParse("document:masterplan#viewer@user:tom"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("document", "editor"),
			},
			expectedError: "disallowed query: document#editor",
		},
		{
			name: "indirect result without alias",
			schema: `definition user {}

			 definition document {
			 	relation editor: user
				relation viewer: user
				permission indirect_viewer = viewer + nil
				permission indirect_editor = editor
				permission view = indirect_viewer & indirect_editor
			}`,
			relationships: []tuple.Relationship{
				tuple.MustParse("document:masterplan#viewer@user:tom"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("document", "viewer"),
			},
			expectedResources: []string{"masterplan"},
		},
		{
			name: "indirect nested",
			schema: `definition user {}

			 definition document {
			 	relation editor: user
				relation viewer: user
				permission indirect_view = viewer & editor
				permission view = indirect_view
			}`,
			relationships: []tuple.Relationship{
				tuple.MustParse("document:masterplan#viewer@user:tom"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("document", "viewer"),
			},
			expectedResources: []string{"masterplan"},
		},
		{
			name: "indirect result with alias",
			schema: `definition user {}

			 definition document {
			 	relation editor: user
				relation viewer: user
				permission indirect_viewer = viewer
				permission indirect_editor = editor
				permission view = indirect_viewer & indirect_editor
			}`,
			relationships: []tuple.Relationship{
				tuple.MustParse("document:masterplan#viewer@user:tom"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("document", "viewer"),
			},
			expectedResources: []string{"masterplan"},
		},
		{
			name: "public document",
			schema: `definition user {}

			 definition document {
			 	relation editor: user
				relation viewer: user | user:*
				permission view = viewer & editor
			}`,
			relationships: []tuple.Relationship{
				tuple.MustParse("document:masterplan#viewer@user:*"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("document", "viewer"),
			},
			expectedResources: []string{"masterplan"},
		},
		{
			name: "multiple paths for checking",
			schema: `definition user {}

			 definition document {
			 	relation editor: user
				relation viewer: user
				relation viewer2: user
				relation admin: user
				permission viewer_of_some_kind = viewer + viewer2
				permission view = viewer_of_some_kind & editor
				permission view_and_admin = view & admin
			}`,
			relationships: []tuple.Relationship{
				tuple.MustParse("document:masterplan#viewer@user:tom"),
				tuple.MustParse("document:masterplan#viewer2@user:tom"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
				tuple.MustParse("document:masterplan#admin@user:tom"),
			},
			resourceRelation: RR("document", "view_and_admin"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("document", "viewer"),
				RR("document", "viewer2"),
			},
			expectedResources: []string{"masterplan"},
		},
		{
			name: "multiple paths for checking variation",
			schema: `definition user {}

			 definition document {
			 	relation editor: user
				relation viewer: user
				relation viewer2: user
				relation admin: user
				permission viewer_of_some_kind = viewer + viewer2
				permission view_and_admin = viewer_of_some_kind & editor & admin
			}`,
			relationships: []tuple.Relationship{
				tuple.MustParse("document:masterplan#viewer@user:tom"),
				tuple.MustParse("document:masterplan#viewer2@user:tom"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
				tuple.MustParse("document:masterplan#admin@user:tom"),
			},
			resourceRelation: RR("document", "view_and_admin"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("document", "viewer"),
				RR("document", "viewer2"),
			},
			expectedResources: []string{"masterplan"},
		},
		{
			name: "multiple paths with relation walk",
			schema: `definition user {}

			 definition group {
			 	relation member: user
			 }

			 definition document {
			 	relation editor: user
				relation viewer: group#member
				permission view = viewer & editor
			}`,
			relationships: []tuple.Relationship{
				tuple.MustParse("document:masterplan#viewer@group:first#member"),
				tuple.MustParse("document:masterplan#viewer@group:second#member"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
				tuple.MustParse("group:first#member@user:tom"),
				tuple.MustParse("group:second#member@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("document", "viewer"),
			},
			expectedResources: []string{"masterplan"},
		},
		{
			name: "multiple paths with relation walk on right",
			schema: `definition user {}

			 definition group {
			 	relation member: user
			 }

			 definition document {
			 	relation editor: user
				relation viewer: group#member
				permission view = editor & viewer
			}`,
			relationships: []tuple.Relationship{
				tuple.MustParse("document:masterplan#viewer@group:first#member"),
				tuple.MustParse("document:masterplan#viewer@group:second#member"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
				tuple.MustParse("group:first#member@user:tom"),
				tuple.MustParse("group:second#member@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("document", "editor"),
			},
			expectedResources: []string{"masterplan"},
		},
		{
			name: "multiple paths with arrow on left",
			schema: `definition user {}

			 definition group {
			 	relation member: user
			 }

			 definition document {
			 	relation editor: user
				relation viewer: group
				permission view = viewer->member & editor
			}`,
			relationships: []tuple.Relationship{
				tuple.MustParse("document:masterplan#viewer@group:first"),
				tuple.MustParse("document:masterplan#viewer@group:second"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
				tuple.MustParse("group:first#member@user:tom"),
				tuple.MustParse("group:second#member@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("document", "viewer"),
			},
			expectedResources: []string{"masterplan"},
		},
		{
			name: "multiple paths with arrow on right",
			schema: `definition user {}

			 definition group {
			 	relation member: user
			 }

			 definition document {
			 	relation editor: user
				relation viewer: group
				permission view = editor & viewer->member
			}`,
			relationships: []tuple.Relationship{
				tuple.MustParse("document:masterplan#viewer@group:first"),
				tuple.MustParse("document:masterplan#viewer@group:second"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
				tuple.MustParse("group:first#member@user:tom"),
				tuple.MustParse("group:second#member@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("document", "editor"),
			},
			expectedResources: []string{"masterplan"},
		},
		{
			name: "duplicate resources for checking",
			schema: `definition user {}

			 definition group {
			 	relation member: user
			 }

			 definition folder {
				relation group: group
			 	relation editor: user
				permission view = group->member & editor
			 }

			 definition document {
			 	relation folder: folder
				permission view = folder->view
 			 }
			`,
			relationships: []tuple.Relationship{
				tuple.MustParse("group:first#member@user:tom"),
				tuple.MustParse("group:second#member@user:tom"),
				tuple.MustParse("folder:folder1#group@group:first"),
				tuple.MustParse("folder:folder1#group@group:second"),
				tuple.MustParse("folder:folder1#editor@user:tom"),
				tuple.MustParse("document:masterplan#folder@folder:folder1"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("group", "member"),
				RR("folder", "group"),
			},
			expectedResources: []string{"masterplan"},
		},
		{
			name: "duplicate resources for checking with missing caveat context on checked side",
			schema: `definition user {}

			 definition group {
			 	relation member: user
			 }

			 caveat somecaveat(somecondition int) {
			 	somecondition == 42
		     }

			 definition folder {
				relation group: group
			 	relation editor: user with somecaveat
				permission view = group->member & editor
			 }

			 definition document {
			 	relation folder: folder
				permission view = folder->view
 			 }
			`,
			relationships: []tuple.Relationship{
				tuple.MustParse("group:first#member@user:tom"),
				tuple.MustParse("group:second#member@user:tom"),
				tuple.MustParse("folder:folder1#group@group:first"),
				tuple.MustParse("folder:folder1#group@group:second"),
				tuple.MustParse("folder:folder1#editor@user:tom[somecaveat]"),
				tuple.MustParse("document:masterplan#folder@folder:folder1"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("group", "member"),
				RR("folder", "group"),
			},
			expectedResources: []string{"masterplan[somecondition]"},
		},
		{
			name: "duplicate resources for checking with missing caveat context on hinted side",
			schema: `definition user {}

			 definition group {
			 	relation member: user | user with somecaveat
			 }

			 caveat somecaveat(somecondition int) {
			 	somecondition == 42
		     }

			 definition folder {
				relation group: group
			 	relation editor: user
				permission view = group->member & editor
			 }

			 definition document {
			 	relation folder: folder
				permission view = folder->view
 			 }
			`,
			relationships: []tuple.Relationship{
				tuple.MustParse("group:first#member@user:tom[somecaveat]"),
				tuple.MustParse("folder:folder1#group@group:first"),
				tuple.MustParse("folder:folder1#editor@user:tom"),
				tuple.MustParse("document:masterplan#folder@folder:folder1"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []tuple.RelationReference{
				RR("group", "member"),
				RR("folder", "group"),
			},
			expectedResources: []string{"masterplan[somecondition]"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require := require.New(t)

			rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
			require.NoError(err)

			ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, tc.schema, tc.relationships, require)

			checkingDS := disallowedWrapper{ds, tc.disallowedQueries}

			dispatcher, err := NewLocalOnlyDispatcher(MustNewDefaultDispatcherParametersForTesting())
			require.NoError(err)
			defer dispatcher.Close()

			ctx := datalayer.ContextWithHandle(t.Context())
			cctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
			defer cancel()

			require.NoError(datalayer.SetInContext(cctx, datalayer.NewDataLayer(checkingDS)))
			stream := dispatch.NewCloningCollectingDispatchStream[*v1.DispatchLookupResources3Response](cctx)

			err = dispatcher.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
				ResourceRelation: tc.resourceRelation.ToCoreRR(),
				SubjectRelation:  RR(tc.subject.ObjectType, tc.subject.Relation).ToCoreRR(),
				SubjectIds:       []string{tc.subject.ObjectID},
				TerminalSubject:  tc.subject.ToCoreONR(),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
			}, stream)
			if tc.expectedError != "" {
				require.Error(err)
				require.Contains(err.Error(), tc.expectedError)
				return
			}

			require.NoError(err)

			foundResourceIDs := mapz.NewSet[string]()
			for _, result := range stream.Results() {
				for _, item := range result.Items {
					if len(item.MissingContextParams) > 0 {
						foundResourceIDs.Insert(item.ResourceId + "[" + strings.Join(item.MissingContextParams, ",") + "]")
						continue
					}

					foundResourceIDs.Insert(item.ResourceId)
				}
			}

			foundResourceIDsSlice := foundResourceIDs.AsSlice()
			slices.Sort(foundResourceIDsSlice)

			require.ElementsMatch(tc.expectedResources, foundResourceIDsSlice)
		})
	}
}
