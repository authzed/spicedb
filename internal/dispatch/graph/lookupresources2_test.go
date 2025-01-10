package graph

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ccoveille/go-safecast"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSimpleLookupResources2(t *testing.T) {
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
			2,
			1,
		},
		{
			RR("document", "owner"),
			ONR("user", "product_manager", "..."),
			[]*v1.PossibleResource{
				possibleRes("masterplan"),
			},
			2,
			0,
		},
		{
			RR("document", "view"),
			ONR("user", "legal", "..."),
			[]*v1.PossibleResource{
				possibleRes("companyplan"),
				possibleRes("masterplan"),
			},
			6,
			3,
		},
		{
			RR("document", "view_and_edit"),
			ONR("user", "multiroleguy", "..."),
			[]*v1.PossibleResource{
				possibleRes("specialplan"),
			},
			7,
			5,
		},
		{
			RR("folder", "view"),
			ONR("user", "owner", "..."),
			[]*v1.PossibleResource{
				possibleRes("strategy"),
				possibleRes("company"),
			},
			8,
			4,
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
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

			require := require.New(t)
			ctx, dispatcher, revision := newLocalDispatcher(t)
			defer dispatcher.Close()

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)
			err := dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
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

			foundResources, maxDepthRequired, maxDispatchCount, maxCachedDispatchCount := processResults2(stream)
			require.ElementsMatch(tc.expectedResources, foundResources, "Found: %v, Expected: %v", foundResources, tc.expectedResources)
			require.Equal(tc.expectedDepthRequired, maxDepthRequired, "Depth required mismatch")
			require.LessOrEqual(maxDispatchCount, tc.expectedDispatchCount, "Found dispatch count greater than expected")
			require.Equal(uint32(0), maxCachedDispatchCount)

			// We have to sleep a while to let the cache converge:
			// https://github.com/outcaste-io/ristretto/blob/01b9f37dd0fd453225e042d6f3a27cd14f252cd0/cache_test.go#L17
			time.Sleep(10 * time.Millisecond)

			// Run again with the cache available.
			stream = dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)
			err = dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
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

			foundResources, maxDepthRequired, maxDispatchCount, maxCachedDispatchCount = processResults2(stream)
			require.ElementsMatch(tc.expectedResources, foundResources, "Found: %v, Expected: %v", foundResources, tc.expectedResources)
			require.Equal(tc.expectedDepthRequired, maxDepthRequired, "Depth required mismatch")
			require.LessOrEqual(maxCachedDispatchCount, tc.expectedDispatchCount, "Found dispatch count greater than expected")
			require.Equal(uint32(0), maxDispatchCount)
		})
	}
}

func TestSimpleLookupResourcesWithCursor2(t *testing.T) {
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
		tc := tc
		t.Run(tc.subject, func(t *testing.T) {
			require := require.New(t)
			ctx, dispatcher, revision := newLocalDispatcher(t)
			defer dispatcher.Close()

			found := mapz.NewSet[string]()

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)
			err := dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
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

			require.Equal(1, len(stream.Results()))

			found.Insert(stream.Results()[0].Resource.ResourceId)
			require.Equal(tc.expectedFirst, found.AsSlice())

			cursor := stream.Results()[0].AfterResponseCursor
			require.NotNil(cursor)

			stream = dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)
			err = dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
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
				found.Insert(result.Resource.ResourceId)
			}

			foundResults := found.AsSlice()
			slices.Sort(foundResults)

			require.Equal(tc.expectedSecond, foundResults)
		})
	}
}

func TestLookupResourcesCursorStability2(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	ctx, dispatcher, revision := newLocalDispatcher(t)
	defer dispatcher.Close()

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)

	// Make the first first request.
	err := dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
		ResourceRelation: RR("document", "view").ToCoreRR(),
		SubjectRelation:  RR("user", "...").ToCoreRR(),
		SubjectIds:       []string{"owner"},
		TerminalSubject:  ONR("user", "owner", "...").ToCoreONR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		OptionalLimit: 2,
	}, stream)

	require.NoError(err)
	require.Equal(2, len(stream.Results()))

	cursor := stream.Results()[1].AfterResponseCursor
	require.NotNil(cursor)

	// Make the same request and ensure the cursor has not changed.
	stream = dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)
	err = dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
		ResourceRelation: RR("document", "view").ToCoreRR(),
		SubjectRelation:  RR("user", "...").ToCoreRR(),
		SubjectIds:       []string{"owner"},
		TerminalSubject:  ONR("user", "owner", "...").ToCoreONR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		OptionalLimit: 2,
	}, stream)

	require.NoError(err)

	require.NoError(err)
	require.Equal(2, len(stream.Results()))

	cursorAgain := stream.Results()[1].AfterResponseCursor
	require.NotNil(cursor)
	require.Equal(cursor, cursorAgain)
}

func processResults2(stream *dispatch.CollectingDispatchStream[*v1.DispatchLookupResources2Response]) ([]*v1.PossibleResource, uint32, uint32, uint32) {
	foundResources := []*v1.PossibleResource{}
	var maxDepthRequired uint32
	var maxDispatchCount uint32
	var maxCachedDispatchCount uint32
	for _, result := range stream.Results() {
		result.Resource.ForSubjectIds = nil
		foundResources = append(foundResources, result.Resource)
		maxDepthRequired = max(maxDepthRequired, result.Metadata.DepthRequired)
		maxDispatchCount = max(maxDispatchCount, result.Metadata.DispatchCount)
		maxCachedDispatchCount = max(maxCachedDispatchCount, result.Metadata.CachedDispatchCount)
	}
	return foundResources, maxDepthRequired, maxDispatchCount, maxCachedDispatchCount
}

func TestMaxDepthLookup2(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatcher := NewLocalOnlyDispatcher(10, 100)
	defer dispatcher.Close()

	ctx := datastoremw.ContextWithHandle(context.Background())
	require.NoError(datastoremw.SetInContext(ctx, ds))
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)

	err = dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
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

func TestLookupResources2OverSchemaWithCursors(t *testing.T) {
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, pageSize := range []int{0, 104, 1023} {
				pageSize := pageSize
				t.Run(fmt.Sprintf("ps-%d_", pageSize), func(t *testing.T) {
					t.Parallel()
					require := require.New(t)

					dispatcher := NewLocalOnlyDispatcher(10, 100)

					ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
					require.NoError(err)

					ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, require)

					ctx := datastoremw.ContextWithHandle(context.Background())
					require.NoError(datastoremw.SetInContext(ctx, ds))

					var currentCursor *v1.Cursor
					foundResourceIDs := mapz.NewSet[string]()
					foundChunks := [][]*v1.DispatchLookupResources2Response{}
					for {
						stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)

						uintPageSize, err := safecast.ToUint32(pageSize)
						require.NoError(err)

						var caveatContext *structpb.Struct
						if tc.optionalCaveatContext != nil {
							caveatContext, err = structpb.NewStruct(tc.optionalCaveatContext)
							require.NoError(err)
						}

						err = dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
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

						if pageSize > 0 {
							require.LessOrEqual(len(stream.Results()), pageSize)
						}

						foundChunks = append(foundChunks, stream.Results())

						for _, result := range stream.Results() {
							require.ElementsMatch(tc.expectedMissingFields, result.Resource.MissingContextParams)
							foundResourceIDs.Insert(result.Resource.ResourceId)
							currentCursor = result.AfterResponseCursor
						}

						if pageSize == 0 || len(stream.Results()) < pageSize {
							break
						}
					}

					for _, chunk := range foundChunks[0 : len(foundChunks)-1] {
						require.Equal(pageSize, len(chunk))
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

func TestLookupResources2ImmediateTimeout(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatcher := NewLocalOnlyDispatcher(10, 100)
	defer dispatcher.Close()

	ctx := datastoremw.ContextWithHandle(context.Background())
	cctx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
	defer cancel()

	require.NoError(datastoremw.SetInContext(cctx, ds))
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](cctx)

	err = dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
		ResourceRelation: RR("document", "view").ToCoreRR(),
		SubjectRelation:  RR("user", "...").ToCoreRR(),
		SubjectIds:       []string{"legal"},
		TerminalSubject:  ONR("user", "legal", "...").ToCoreONR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 10,
		},
	}, stream)

	require.ErrorIs(err, context.DeadlineExceeded)
	require.ErrorContains(err, "context deadline exceeded")
}

func TestLookupResources2WithError(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatcher := NewLocalOnlyDispatcher(10, 100)
	defer dispatcher.Close()

	ctx := datastoremw.ContextWithHandle(context.Background())
	cctx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
	defer cancel()

	require.NoError(datastoremw.SetInContext(cctx, ds))
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](cctx)

	err = dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
		ResourceRelation: RR("document", "view").ToCoreRR(),
		SubjectRelation:  RR("user", "...").ToCoreRR(),
		SubjectIds:       []string{"legal"},
		TerminalSubject:  ONR("user", "legal", "...").ToCoreONR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 10,
		},
	}, stream)

	require.ErrorIs(err, context.DeadlineExceeded)
	require.ErrorContains(err, "context deadline exceeded")
}

func TestLookupResources2EnsureCheckHints(t *testing.T) {
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			require.NoError(err)

			ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, tc.schema, tc.relationships, require)

			checkingDS := disallowedWrapper{ds, tc.disallowedQueries}

			dispatcher := NewLocalOnlyDispatcher(10, 100)
			defer dispatcher.Close()

			ctx := datastoremw.ContextWithHandle(context.Background())
			cctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
			defer cancel()

			require.NoError(datastoremw.SetInContext(cctx, checkingDS))
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](cctx)

			err = dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
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
				if len(result.Resource.MissingContextParams) > 0 {
					foundResourceIDs.Insert(result.Resource.ResourceId + "[" + strings.Join(result.Resource.MissingContextParams, ",") + "]")
					continue
				}

				foundResourceIDs.Insert(result.Resource.ResourceId)
			}

			foundResourceIDsSlice := foundResourceIDs.AsSlice()
			slices.Sort(foundResourceIDsSlice)

			require.ElementsMatch(tc.expectedResources, foundResourceIDsSlice)
		})
	}
}

type disallowedWrapper struct {
	datastore.Datastore
	disallowedQueries []tuple.RelationReference
}

func (dw disallowedWrapper) SnapshotReader(rev datastore.Revision) datastore.Reader {
	return disallowedReader{dw.Datastore.SnapshotReader(rev), dw.disallowedQueries}
}

type disallowedReader struct {
	datastore.Reader
	disallowedQueries []tuple.RelationReference
}

func (dr disallowedReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	options ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	for _, disallowedQuery := range dr.disallowedQueries {
		if disallowedQuery.ObjectType == filter.OptionalResourceType && disallowedQuery.Relation == filter.OptionalResourceRelation {
			return nil, fmt.Errorf("disallowed query: %s", tuple.StringRR(disallowedQuery))
		}
	}

	return dr.Reader.QueryRelationships(ctx, filter, options...)
}

func possibleRes(resourceID string) *v1.PossibleResource {
	return &v1.PossibleResource{
		ResourceId: resourceID,
	}
}

func joinTuples(first []tuple.Relationship, others ...[]tuple.Relationship) []tuple.Relationship {
	current := first
	for _, second := range others {
		current = append(current, second...)
	}
	return current
}

func genRelsWithOffset(resourceName string, relation string, subjectName string, subjectID string, offset int, number int) []tuple.Relationship {
	return genRelsWithCaveat(resourceName, relation, subjectName, subjectID, "", nil, offset, number)
}

func genRels(resourceName string, relation string, subjectName string, subjectID string, number int) []tuple.Relationship {
	return genRelsWithOffset(resourceName, relation, subjectName, subjectID, 0, number)
}

func genSubjectRels(resourceName string, relation string, subjectName string, subjectRelation string, number int) []tuple.Relationship {
	rels := make([]tuple.Relationship, 0, number)
	for i := 0; i < number; i++ {
		rel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: ONR(resourceName, fmt.Sprintf("%s-%d", resourceName, i), relation),
				Subject:  ONR(subjectName, fmt.Sprintf("%s-%d", subjectName, i), subjectRelation),
			},
		}
		rels = append(rels, rel)
	}

	return rels
}

func genRelsWithCaveat(resourceName string, relation string, subjectName string, subjectID string, caveatName string, context map[string]any, offset int, number int) []tuple.Relationship {
	return genRelsWithCaveatAndSubjectRelation(resourceName, relation, subjectName, subjectID, "...", caveatName, context, offset, number)
}

func genRelsWithCaveatAndSubjectRelation(resourceName string, relation string, subjectName string, subjectID string, subjectRelation string, caveatName string, context map[string]any, offset int, number int) []tuple.Relationship {
	rels := make([]tuple.Relationship, 0, number)
	for i := 0; i < number; i++ {
		rel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: ONR(resourceName, fmt.Sprintf("%s-%d", resourceName, i+offset), relation),
				Subject:  ONR(subjectName, subjectID, subjectRelation),
			},
		}

		if caveatName != "" {
			rel = tuple.MustWithCaveat(rel, caveatName, context)
		}
		rels = append(rels, rel)
	}
	return rels
}

func genResourceIds(resourceName string, number int) []string {
	resourceIDs := make([]string, 0, number)
	for i := 0; i < number; i++ {
		resourceIDs = append(resourceIDs, fmt.Sprintf("%s-%d", resourceName, i))
	}
	return resourceIDs
}

func processResults(stream *dispatch.CollectingDispatchStream[*v1.DispatchLookupResources2Response]) ([]*v1.PossibleResource, uint32, uint32, uint32) {
	foundResources := []*v1.PossibleResource{}
	var maxDepthRequired uint32
	var maxDispatchCount uint32
	var maxCachedDispatchCount uint32
	for _, result := range stream.Results() {
		foundResources = append(foundResources, result.Resource)
		maxDepthRequired = max(maxDepthRequired, result.Metadata.DepthRequired)
		maxDispatchCount = max(maxDispatchCount, result.Metadata.DispatchCount)
		maxCachedDispatchCount = max(maxCachedDispatchCount, result.Metadata.CachedDispatchCount)
	}
	return foundResources, maxDepthRequired, maxDispatchCount, maxCachedDispatchCount
}
