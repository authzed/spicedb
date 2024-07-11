package graph

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSimpleLookupResources2(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	testCases := []struct {
		start                 *core.RelationReference
		target                *core.ObjectAndRelation
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
			tc.start.Namespace,
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
				ResourceRelation: tc.start,
				SubjectRelation:  RR(tc.target.Namespace, tc.target.Relation),
				SubjectIds:       []string{tc.target.ObjectId},
				TerminalSubject:  tc.target,
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
				ResourceRelation: tc.start,
				SubjectRelation:  RR(tc.target.Namespace, tc.target.Relation),
				SubjectIds:       []string{tc.target.ObjectId},
				TerminalSubject:  tc.target,
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
	defer goleak.VerifyNone(t, goleakIgnores...)

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
				ResourceRelation: RR("document", "view"),
				SubjectRelation:  RR("user", "..."),
				SubjectIds:       []string{tc.subject},
				TerminalSubject:  ONR("user", tc.subject, "..."),
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
				ResourceRelation: RR("document", "view"),
				SubjectRelation:  RR("user", "..."),
				SubjectIds:       []string{tc.subject},
				TerminalSubject:  ONR("user", tc.subject, "..."),
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
	defer goleak.VerifyNone(t, goleakIgnores...)

	require := require.New(t)
	ctx, dispatcher, revision := newLocalDispatcher(t)
	defer dispatcher.Close()

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)

	// Make the first first request.
	err := dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
		ResourceRelation: RR("document", "view"),
		SubjectRelation:  RR("user", "..."),
		SubjectIds:       []string{"owner"},
		TerminalSubject:  ONR("user", "owner", "..."),
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
		ResourceRelation: RR("document", "view"),
		SubjectRelation:  RR("user", "..."),
		SubjectIds:       []string{"owner"},
		TerminalSubject:  ONR("user", "owner", "..."),
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
	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatcher := NewLocalOnlyDispatcher(10, 100)
	defer dispatcher.Close()

	ctx := datastoremw.ContextWithHandle(context.Background())
	require.NoError(datastoremw.SetInContext(ctx, ds))
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)

	err = dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
		ResourceRelation: RR("document", "view"),
		SubjectRelation:  RR("user", "..."),
		SubjectIds:       []string{"legal"},
		TerminalSubject:  ONR("user", "legal", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 0,
		},
	}, stream)

	require.Error(err)
}

func TestLookupResources2OverSchemaWithCursors(t *testing.T) {
	testCases := []struct {
		name                string
		schema              string
		relationships       []*core.RelationTuple
		permission          *core.RelationReference
		subject             *core.ObjectAndRelation
		expectedResourceIDs []string
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
				genTuples("document", "viewer", "user", "tom", 1510),
				genTuples("document", "editor", "user", "tom", 1510),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 1510),
		},
		{
			"basic exclusion",
			`definition user {}
		
		 	 definition document {
				relation banned: user
				relation viewer: user
				permission view = viewer - banned
  			 }`,
			genTuples("document", "viewer", "user", "tom", 1010),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 1010),
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
				genTuples("document", "viewer", "user", "tom", 510),
				genTuples("document", "editor", "user", "tom", 510),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 510),
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
				genTuples("document", "viewer", "user", "tom", 1310),
				genTuplesWithOffset("document", "editor", "user", "tom", 1250, 1200),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 2450),
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
			genTuplesWithCaveat("document", "viewer", "user", "tom", "somecaveat", map[string]any{"somecondition": 42}, 0, 2450),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 2450),
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
				genTuples("document", "viewer", "user", "tom", 1310),
				genTuplesWithOffset("document", "banned", "user", "tom", 1210, 100),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 1210),
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
			genTuplesWithCaveat("document", "viewer", "user", "tom", "somecaveat", map[string]any{}, 0, 2450),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 2450),
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
				genTuples("folder", "viewer", "user", "tom", 150),
				genSubjectTuples("document", "folder", "folder", "...", 150),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 150),
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
				genTuples("document", "viewer", "user", "tom", 15100),
				genTuples("document", "editor", "user", "tom", 15100),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 15100),
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
				genTuples("document", "viewer", "user", "tom", 510),
				joinTuples(
					genTuples("document", "org", "organization", "someorg", 510),
					[]*core.RelationTuple{
						tuple.MustParse("organization:someorg#member@user:tom"),
					},
				),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 510),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, pageSize := range []int{0, 104, 1023} {
				pageSize := pageSize
				t.Run(fmt.Sprintf("ps-%d_", pageSize), func(t *testing.T) {
					require := require.New(t)

					dispatcher := NewLocalOnlyDispatcher(10, 100)

					ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
					require.NoError(err)

					ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, require)

					ctx := datastoremw.ContextWithHandle(context.Background())
					require.NoError(datastoremw.SetInContext(ctx, ds))

					var currentCursor *v1.Cursor
					foundResourceIDs := mapz.NewSet[string]()
					for {
						stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)
						err = dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
							ResourceRelation: tc.permission,
							SubjectRelation:  RR(tc.subject.Namespace, "..."),
							SubjectIds:       []string{tc.subject.ObjectId},
							TerminalSubject:  tc.subject,
							Metadata: &v1.ResolverMeta{
								AtRevision:     revision.String(),
								DepthRemaining: 50,
							},
							OptionalLimit:  uint32(pageSize),
							OptionalCursor: currentCursor,
						}, stream)
						require.NoError(err)

						if pageSize > 0 {
							require.LessOrEqual(len(stream.Results()), pageSize)
						}

						for _, result := range stream.Results() {
							foundResourceIDs.Insert(result.Resource.ResourceId)
							currentCursor = result.AfterResponseCursor
						}

						if pageSize == 0 || len(stream.Results()) < pageSize {
							break
						}
					}

					foundResourceIDsSlice := foundResourceIDs.AsSlice()
					slices.Sort(foundResourceIDsSlice)
					slices.Sort(tc.expectedResourceIDs)

					require.Equal(tc.expectedResourceIDs, foundResourceIDsSlice)
				})
			}
		})
	}
}

func TestLookupResources2ImmediateTimeout(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
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
		ResourceRelation: RR("document", "view"),
		SubjectRelation:  RR("user", "..."),
		SubjectIds:       []string{"legal"},
		TerminalSubject:  ONR("user", "legal", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 10,
		},
	}, stream)

	require.ErrorIs(err, context.DeadlineExceeded)
	require.ErrorContains(err, "context deadline exceeded")
}

func TestLookupResources2WithError(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
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
		ResourceRelation: RR("document", "view"),
		SubjectRelation:  RR("user", "..."),
		SubjectIds:       []string{"legal"},
		TerminalSubject:  ONR("user", "legal", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 10,
		},
	}, stream)

	require.ErrorIs(err, context.DeadlineExceeded)
	require.ErrorContains(err, "context deadline exceeded")
}

func TestLookupResources2EnsureCheckHints(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	tcs := []struct {
		name          string
		schema        string
		relationships []*core.RelationTuple

		resourceRelation *core.RelationReference
		subject          *core.ObjectAndRelation

		disallowedQueries []*core.RelationReference
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
			relationships: []*core.RelationTuple{
				tuple.MustParse("document:masterplan#viewer@user:tom"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
				tuple.MustParse("document:anotherplan#viewer@user:tom"),
				tuple.MustParse("document:anotherplan#editor@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []*core.RelationReference{
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
			relationships: []*core.RelationTuple{
				tuple.MustParse("document:masterplan#org@organization:someorg"),
				tuple.MustParse("organization:someorg#member@user:tom"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []*core.RelationReference{
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
			relationships: []*core.RelationTuple{
				tuple.MustParse("document:masterplan#viewer@user:tom"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []*core.RelationReference{
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
			relationships: []*core.RelationTuple{
				tuple.MustParse("document:masterplan#viewer@user:tom"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []*core.RelationReference{
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
			relationships: []*core.RelationTuple{
				tuple.MustParse("document:masterplan#viewer@user:tom"),
				tuple.MustParse("document:masterplan#editor@user:tom"),
			},
			resourceRelation: RR("document", "view"),
			subject:          ONR("user", "tom", "..."),
			disallowedQueries: []*core.RelationReference{
				RR("document", "viewer"),
			},
			expectedResources: []string{"masterplan"},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
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
				ResourceRelation: tc.resourceRelation,
				SubjectRelation:  RR(tc.subject.Namespace, tc.subject.Relation),
				SubjectIds:       []string{tc.subject.ObjectId},
				TerminalSubject:  tc.subject,
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
	disallowedQueries []*core.RelationReference
}

func (dw disallowedWrapper) SnapshotReader(rev datastore.Revision) datastore.Reader {
	return disallowedReader{dw.Datastore.SnapshotReader(rev), dw.disallowedQueries}
}

type disallowedReader struct {
	datastore.Reader
	disallowedQueries []*core.RelationReference
}

func (dr disallowedReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	options ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	for _, disallowedQuery := range dr.disallowedQueries {
		if disallowedQuery.Namespace == filter.OptionalResourceType && disallowedQuery.Relation == filter.OptionalResourceRelation {
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
