package graph

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	itestutil "github.com/authzed/spicedb/internal/testutil"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	caveatexpr   = caveats.CaveatExprForTesting
	caveatAnd    = caveats.And
	caveatOr     = caveats.Or
	caveatInvert = caveats.Invert
)

func TestSimpleLookupSubjects(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	testCases := []struct {
		resourceType     string
		resourceID       string
		permission       string
		subjectType      string
		subjectRelation  string
		expectedSubjects []string
	}{
		{
			"document",
			"masterplan",
			"view",
			"user",
			"...",
			[]string{"auditor", "chief_financial_officer", "eng_lead", "legal", "owner", "product_manager", "vp_product"},
		},
		{
			"document",
			"masterplan",
			"edit",
			"user",
			"...",
			[]string{"product_manager"},
		},
		{
			"document",
			"masterplan",
			"view_and_edit",
			"user",
			"...",
			[]string{},
		},
		{
			"document",
			"specialplan",
			"view",
			"user",
			"...",
			[]string{"multiroleguy"},
		},
		{
			"document",
			"specialplan",
			"edit",
			"user",
			"...",
			[]string{"multiroleguy"},
		},
		{
			"document",
			"specialplan",
			"viewer_and_editor",
			"user",
			"...",
			[]string{"multiroleguy", "missingrolegal"},
		},
		{
			"document",
			"specialplan",
			"view_and_edit",
			"user",
			"...",
			[]string{"multiroleguy"},
		},
		{
			"folder",
			"company",
			"view",
			"user",
			"...",
			[]string{"auditor", "legal", "owner"},
		},
		{
			"folder",
			"strategy",
			"view",
			"user",
			"...",
			[]string{"auditor", "legal", "owner", "vp_product"},
		},
		{
			"document",
			"masterplan",
			"parent",
			"folder",
			"...",
			[]string{"plans", "strategy"},
		},
		{
			"document",
			"masterplan",
			"view",
			"folder",
			"...",
			[]string{},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("simple-lookup-subjects:%s:%s:%s:%s:%s", tc.resourceType, tc.resourceID, tc.permission, tc.subjectType, tc.subjectRelation), func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

			require := require.New(t)

			ctx, dis, revision := newLocalDispatcher(t)
			defer dis.Close()

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)

			err := dis.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: RR(tc.resourceType, tc.permission),
				ResourceIds:      []string{tc.resourceID},
				SubjectRelation:  RR(tc.subjectType, tc.subjectRelation),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
			}, stream)

			require.NoError(err)

			foundSubjectIds := []string{}
			for _, result := range stream.Results() {
				results, ok := result.FoundSubjectsByResourceId[tc.resourceID]
				if ok {
					for _, found := range results.FoundSubjects {
						if len(found.ExcludedSubjects) > 0 {
							continue
						}

						foundSubjectIds = append(foundSubjectIds, found.SubjectId)
					}
				}
			}

			sort.Strings(foundSubjectIds)
			sort.Strings(tc.expectedSubjects)
			require.Equal(tc.expectedSubjects, foundSubjectIds)

			// Ensure every subject found has access.
			for _, subjectID := range foundSubjectIds {
				checkResult, err := dis.DispatchCheck(ctx, &v1.DispatchCheckRequest{
					ResourceRelation: RR(tc.resourceType, tc.permission),
					ResourceIds:      []string{tc.resourceID},
					ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
					Subject:          ONR(tc.subjectType, subjectID, tc.subjectRelation),
					Metadata: &v1.ResolverMeta{
						AtRevision:     revision.String(),
						DepthRemaining: 50,
					},
				})

				require.NoError(err)
				require.Equal(v1.ResourceCheckResult_MEMBER, checkResult.ResultsByResourceId[tc.resourceID].Membership)
			}
			dis.Close()
		})
	}
}

func TestLookupSubjectsMaxDepth(t *testing.T) {
	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)

	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
	require.NoError(datastoremw.SetInContext(ctx, ds))

	tpl := tuple.Parse("folder:oops#parent@folder:oops")
	revision, err := common.WriteTuples(ctx, ds, corev1.RelationTupleUpdate_CREATE, tpl)
	require.NoError(err)

	dis := NewLocalOnlyDispatcher(10)
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)

	err = dis.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
		ResourceRelation: RR("folder", "view"),
		ResourceIds:      []string{"oops"},
		SubjectRelation:  RR("user", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
	}, stream)
	require.Error(err)
}

func TestLookupSubjectsDispatchCount(t *testing.T) {
	testCases := []struct {
		resourceType          string
		resourceID            string
		permission            string
		subjectType           string
		subjectRelation       string
		expectedDispatchCount int
	}{
		{
			"document",
			"masterplan",
			"view",
			"user",
			"...",
			13,
		},
		{
			"document",
			"masterplan",
			"view_and_edit",
			"user",
			"...",
			5,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("dispatch-count-lookup-subjects:%s:%s:%s:%s:%s", tc.resourceType, tc.resourceID, tc.permission, tc.subjectType, tc.subjectRelation), func(t *testing.T) {
			require := require.New(t)

			ctx, dis, revision := newLocalDispatcher(t)
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)

			err := dis.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: RR(tc.resourceType, tc.permission),
				ResourceIds:      []string{tc.resourceID},
				SubjectRelation:  RR(tc.subjectType, tc.subjectRelation),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
			}, stream)

			require.NoError(err)
			for _, result := range stream.Results() {
				require.LessOrEqual(int(result.Metadata.DispatchCount), tc.expectedDispatchCount, "Found dispatch count greater than expected")
			}
		})
	}
}

func TestCaveatedLookupSubjects(t *testing.T) {
	testCases := []struct {
		name          string
		schema        string
		relationships []*corev1.RelationTuple
		start         *corev1.ObjectAndRelation
		target        *corev1.RelationReference
		expected      []*v1.FoundSubject
	}{
		{
			"basic caveated",
			`definition user {}
		
			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }

		 	 definition document {
				relation viewer: user | user with somecaveat
				permission view = viewer
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:tom"), "somecaveat"),
				tuple.MustParse("document:first#viewer@user:sarah"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "sarah",
				},
				{
					SubjectId:        "tom",
					CaveatExpression: caveatexpr("somecaveat"),
				},
			},
		},
		{
			"union caveated",
			`definition user {}
		
			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }

		 	 definition document {
				relation viewer: user | user with somecaveat
				relation editor: user | user with somecaveat
				permission view = viewer + editor
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:tom"), "somecaveat"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#editor@user:tom"), "somecaveat"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId:        "tom",
					CaveatExpression: caveatexpr("somecaveat"),
				},
			},
		},
		{
			"union short-circuited caveated",
			`definition user {}
		
			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }

		 	 definition document {
				relation viewer: user | user with somecaveat
				relation editor: user | user with somecaveat
				permission view = viewer + editor
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#editor@user:tom"), "somecaveat"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "tom",
				},
			},
		},
		{
			"intersection caveated",
			`definition user {}
		
			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }

			 caveat anothercaveat(somecondition int) {
				somecondition == 42
			 }

		 	 definition document {
				relation viewer: user | user with somecaveat
				relation editor: user | user with anothercaveat
				permission view = viewer & editor
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:tom"), "somecaveat"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#editor@user:tom"), "anothercaveat"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:sarah"), "somecaveat"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "tom",
					CaveatExpression: caveatAnd(
						caveatexpr("somecaveat"),
						caveatexpr("anothercaveat"),
					),
				},
			},
		},
		{
			"exclusion caveated",
			`definition user {}
		
			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }

			 caveat anothercaveat(somecondition int) {
				somecondition == 42
			 }

		 	 definition document {
				relation viewer: user | user with somecaveat
				relation banned: user | user with anothercaveat
				permission view = viewer - banned
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:tom"), "somecaveat"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#banned@user:tom"), "anothercaveat"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:sarah"), "somecaveat"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId:        "sarah",
					CaveatExpression: caveatexpr("somecaveat"),
				},
				{
					SubjectId: "tom",
					CaveatExpression: caveatAnd(
						caveatexpr("somecaveat"),
						caveatInvert(caveatexpr("anothercaveat")),
					),
				},
			},
		},
		{
			"arrow caveated",
			`definition user {}
		
			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }

			 definition org {
				relation viewer: user								
			 }

		 	 definition document {
				relation org: org with somecaveat
				permission view = org->viewer
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustWithCaveat(tuple.MustParse("document:first#org@org:someorg"), "somecaveat"),
				tuple.MustParse("org:someorg#viewer@user:tom"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId:        "tom",
					CaveatExpression: caveatexpr("somecaveat"),
				},
			},
		},
		{
			"arrow and relation caveated",
			`definition user {}
		
			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }

			 caveat anothercaveat(somecondition int) {
				somecondition == 42
			 }

			 definition org {
				relation viewer: user with anothercaveat					
			 }

		 	 definition document {
				relation org: org with somecaveat
				permission view = org->viewer
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustWithCaveat(tuple.MustParse("document:first#org@org:someorg"), "somecaveat"),
				tuple.MustWithCaveat(tuple.MustParse("org:someorg#viewer@user:tom"), "anothercaveat"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "tom",
					CaveatExpression: caveatAnd(
						caveatexpr("somecaveat"),
						caveatexpr("anothercaveat"),
					),
				},
			},
		},
		{
			"caveated wildcard with exclusions caveated",
			`definition user {}
		
			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }

			 caveat anothercaveat(somecondition int) {
				somecondition == 42
			 }

		 	 definition document {
				relation viewer: user:* with somecaveat
				relation banned: user with anothercaveat
				permission view = viewer - banned
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:*"), "somecaveat"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#banned@user:tom"), "anothercaveat"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "*",
					ExcludedSubjects: []*v1.FoundSubject{
						{
							SubjectId:        "tom",
							CaveatExpression: caveatexpr("anothercaveat"),
						},
					},
					CaveatExpression: caveatexpr("somecaveat"),
				},
			},
		},
		{
			"caveated wildcard with exclusions caveated",
			`definition user {}
		
			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }

			 caveat anothercaveat(somecondition int) {
				somecondition == 42
			 }

			 caveat thirdcaveat(somecondition int) {
				somecondition == 42
			 }

		 	 definition document {
				relation viewer: user:* with somecaveat
				relation banned: user with anothercaveat
				relation explicitly_allowed: user with thirdcaveat
				permission view = (viewer - banned) + explicitly_allowed
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:*"), "somecaveat"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#banned@user:tom"), "anothercaveat"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#explicitly_allowed@user:tom"), "thirdcaveat"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId:        "tom",
					CaveatExpression: caveatexpr("thirdcaveat"),
				},
				{
					SubjectId: "*",
					ExcludedSubjects: []*v1.FoundSubject{
						{
							SubjectId: "tom",
							CaveatExpression: caveatAnd(
								caveatexpr("anothercaveat"),
								caveatInvert(caveatexpr("thirdcaveat")),
							),
						},
					},
					CaveatExpression: caveatexpr("somecaveat"),
				},
			},
		},
		{
			"multiple via arrows",
			`definition user {}
		
			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }

			 caveat anothercaveat(somecondition int) {
				somecondition == 42
			 }

			 caveat thirdcaveat(somecondition int) {
				somecondition == 42
			 }

			 definition org {
				relation viewer: user | user with anothercaveat				
			 }

		 	 definition document {
				relation org: org with somecaveat | org with thirdcaveat
				permission view = org->viewer
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustWithCaveat(tuple.MustParse("document:first#org@org:someorg"), "somecaveat"),
				tuple.MustWithCaveat(tuple.MustParse("org:someorg#viewer@user:tom"), "anothercaveat"),
				tuple.MustParse("org:someorg#viewer@user:sarah"),

				tuple.MustWithCaveat(tuple.MustParse("document:first#org@org:anotherorg"), "thirdcaveat"),
				tuple.MustWithCaveat(tuple.MustParse("org:anotherorg#viewer@user:amy"), "anothercaveat"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "tom",
					CaveatExpression: caveatAnd(
						caveatexpr("somecaveat"),
						caveatexpr("anothercaveat"),
					),
				},
				{
					SubjectId:        "sarah",
					CaveatExpression: caveatexpr("somecaveat"),
				},
				{
					SubjectId: "amy",
					CaveatExpression: caveatAnd(
						caveatexpr("thirdcaveat"),
						caveatexpr("anothercaveat"),
					),
				},
			},
		},
		{
			"arrow over different relations of the same subject",
			`definition user {}
	
			 definition folder {
				relation parent: folder
				relation viewer: user
				permission view = viewer
			 }

		 	 definition document {
				relation folder: folder | folder#parent
				permission view = folder->view
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("folder:folder1#viewer@user:tom"),
				tuple.MustParse("folder:folder2#viewer@user:fred"),
				tuple.MustParse("document:somedoc#folder@folder:folder1"),
				tuple.MustParse("document:somedoc#folder@folder:folder2#parent"),
			},
			ONR("document", "somedoc", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "tom",
				},
				{
					SubjectId: "fred",
				},
			},
		},
		{
			"caveated arrow over different relations of the same subject",
			`definition user {}
	
			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }

			 definition folder {
				relation parent: folder
				relation viewer: user
				permission view = viewer
			 }

		 	 definition document {
				relation folder: folder | folder#parent with somecaveat
				permission view = folder->view
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("folder:folder1#viewer@user:tom"),
				tuple.MustParse("folder:folder2#viewer@user:fred"),
				tuple.MustParse("document:somedoc#folder@folder:folder1"),
				tuple.MustWithCaveat(tuple.MustParse("document:somedoc#folder@folder:folder2#parent"), "somecaveat"),
			},
			ONR("document", "somedoc", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "tom",
				},
				{
					SubjectId:        "fred",
					CaveatExpression: caveatexpr("somecaveat"),
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			dispatcher := NewLocalOnlyDispatcher(10)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(err)

			ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, require)

			ctx := datastoremw.ContextWithHandle(context.Background())
			require.NoError(datastoremw.SetInContext(ctx, ds))

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
			err = dispatcher.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: &corev1.RelationReference{
					Namespace: tc.start.Namespace,
					Relation:  tc.start.Relation,
				},
				ResourceIds:     []string{tc.start.ObjectId},
				SubjectRelation: tc.target,
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
			}, stream)
			require.NoError(err)

			results := []*v1.FoundSubject{}
			for _, streamResult := range stream.Results() {
				for _, foundSubjects := range streamResult.FoundSubjectsByResourceId {
					results = append(results, foundSubjects.FoundSubjects...)
				}
			}

			itestutil.RequireEquivalentSets(t, tc.expected, results)
		})
	}
}

func TestCursoredLookupSubjects(t *testing.T) {
	testCases := []struct {
		name          string
		pageSizes     []int
		schema        string
		relationships []*corev1.RelationTuple
		start         *corev1.ObjectAndRelation
		target        *corev1.RelationReference
		expected      []*v1.FoundSubject
	}{
		{
			"simple",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

		 	 definition document {
				relation viewer: user
				permission view = viewer
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#viewer@user:sarah"),
				tuple.MustParse("document:first#viewer@user:fred"),
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustParse("document:first#viewer@user:andria"),
				tuple.MustParse("document:first#viewer@user:victor"),
				tuple.MustParse("document:first#viewer@user:chuck"),
				tuple.MustParse("document:first#viewer@user:ben"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{SubjectId: "sarah"},
				{SubjectId: "fred"},
				{SubjectId: "tom"},
				{SubjectId: "andria"},
				{SubjectId: "victor"},
				{SubjectId: "chuck"},
				{SubjectId: "ben"},
			},
		},
		{
			"basic union",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

		 	 definition document {
				relation viewer1: user
				relation viewer2: user
				permission view = viewer1 + viewer2
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#viewer1@user:sarah"),
				tuple.MustParse("document:first#viewer1@user:fred"),
				tuple.MustParse("document:first#viewer1@user:tom"),
				tuple.MustParse("document:first#viewer2@user:andria"),
				tuple.MustParse("document:first#viewer2@user:victor"),
				tuple.MustParse("document:first#viewer2@user:chuck"),
				tuple.MustParse("document:first#viewer2@user:ben"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{SubjectId: "sarah"},
				{SubjectId: "fred"},
				{SubjectId: "tom"},
				{SubjectId: "andria"},
				{SubjectId: "victor"},
				{SubjectId: "chuck"},
				{SubjectId: "ben"},
			},
		},
		{
			"basic intersection",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

		 	 definition document {
				relation viewer1: user
				relation viewer2: user
				permission view = viewer1 & viewer2
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#viewer1@user:sarah"),
				tuple.MustParse("document:first#viewer1@user:fred"),
				tuple.MustParse("document:first#viewer1@user:tom"),
				tuple.MustParse("document:first#viewer1@user:andria"),
				tuple.MustParse("document:first#viewer1@user:victor"),
				tuple.MustParse("document:first#viewer2@user:victor"),
				tuple.MustParse("document:first#viewer2@user:chuck"),
				tuple.MustParse("document:first#viewer2@user:ben"),
				tuple.MustParse("document:first#viewer2@user:andria"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{SubjectId: "andria"},
				{SubjectId: "victor"},
			},
		},
		{
			"basic exclusion",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

		 	 definition document {
				relation viewer1: user
				relation viewer2: user
				permission view = viewer1 - viewer2
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#viewer1@user:sarah"),
				tuple.MustParse("document:first#viewer1@user:fred"),
				tuple.MustParse("document:first#viewer1@user:tom"),
				tuple.MustParse("document:first#viewer1@user:andria"),
				tuple.MustParse("document:first#viewer1@user:victor"),
				tuple.MustParse("document:first#viewer2@user:victor"),
				tuple.MustParse("document:first#viewer2@user:chuck"),
				tuple.MustParse("document:first#viewer2@user:ben"),
				tuple.MustParse("document:first#viewer2@user:andria"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{SubjectId: "sarah"},
				{SubjectId: "fred"},
				{SubjectId: "tom"},
			},
		},
		{
			"union over exclusion",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

		 	 definition document {
				relation viewer: user
				relation editor: user
				relation banned: user

				permission edit = editor - banned
				permission view = viewer + edit
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#viewer@user:sarah"),
				tuple.MustParse("document:first#viewer@user:fred"),

				tuple.MustParse("document:first#editor@user:sarah"),
				tuple.MustParse("document:first#editor@user:george"),
				tuple.MustParse("document:first#editor@user:victor"),

				tuple.MustParse("document:first#banned@user:victor"),
				tuple.MustParse("document:first#banned@user:bannedguy"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{SubjectId: "sarah"},
				{SubjectId: "fred"},
				{SubjectId: "george"},
			},
		},
		{
			"basic caveated",
			[]int{0, 1, 2, 5, 100},
			`definition user {}
		
			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }

		 	 definition document {
				relation viewer: user | user with somecaveat
				permission view = viewer
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:tom"), "somecaveat"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:fred"), "somecaveat"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:sarah"), "somecaveat"),
				tuple.MustParse("document:first#viewer@user:tracy"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "tracy",
				},
				{
					SubjectId:        "tom",
					CaveatExpression: caveatexpr("somecaveat"),
				},
				{
					SubjectId:        "fred",
					CaveatExpression: caveatexpr("somecaveat"),
				},
				{
					SubjectId:        "sarah",
					CaveatExpression: caveatexpr("somecaveat"),
				},
			},
		},
		{
			"union short-circuited caveated",
			[]int{0, 1, 2, 5, 100},
			`definition user {}
		
			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }

		 	 definition document {
				relation viewer: user | user with somecaveat
				relation editor: user | user with somecaveat
				permission view = viewer + editor
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#editor@user:tom"), "somecaveat"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "tom",
				},
			},
		},
		{
			"intersection caveated",
			[]int{0, 1, 2, 5, 100},
			`definition user {}
		
			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }

			 caveat anothercaveat(somecondition int) {
				somecondition == 42
			 }

		 	 definition document {
				relation viewer: user | user with somecaveat
				relation editor: user | user with anothercaveat
				permission view = viewer & editor
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:tom"), "somecaveat"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#editor@user:tom"), "anothercaveat"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:sarah"), "somecaveat"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "tom",
					CaveatExpression: caveatAnd(
						caveatexpr("somecaveat"),
						caveatexpr("anothercaveat"),
					),
				},
			},
		},
		{
			"simple wildcard",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

		 	 definition document {
				relation viewer: user | user:*
				permission view = viewer
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#viewer@user:sarah"),
				tuple.MustParse("document:first#viewer@user:fred"),
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustParse("document:first#viewer@user:andria"),
				tuple.MustParse("document:first#viewer@user:victor"),
				tuple.MustParse("document:first#viewer@user:chuck"),
				tuple.MustParse("document:first#viewer@user:ben"),
				tuple.MustParse("document:first#viewer@user:*"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{SubjectId: "sarah"},
				{SubjectId: "fred"},
				{SubjectId: "tom"},
				{SubjectId: "andria"},
				{SubjectId: "victor"},
				{SubjectId: "chuck"},
				{SubjectId: "ben"},
				{SubjectId: "*"},
			},
		},
		{
			"intersection with wildcard",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

		 	 definition document {
				relation viewer1: user
				relation viewer2: user:*
				permission view = viewer1 & viewer2
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#viewer1@user:sarah"),
				tuple.MustParse("document:first#viewer1@user:fred"),
				tuple.MustParse("document:first#viewer1@user:tom"),
				tuple.MustParse("document:first#viewer1@user:andria"),
				tuple.MustParse("document:first#viewer1@user:victor"),
				tuple.MustParse("document:first#viewer1@user:chuck"),
				tuple.MustParse("document:first#viewer1@user:ben"),
				tuple.MustParse("document:first#viewer2@user:*"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{SubjectId: "sarah"},
				{SubjectId: "fred"},
				{SubjectId: "tom"},
				{SubjectId: "andria"},
				{SubjectId: "victor"},
				{SubjectId: "chuck"},
				{SubjectId: "ben"},
			},
		},
		{
			"wildcard with exclusions",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

		 	 definition document {
				relation viewer: user:*
				relation banned: user
				permission view = viewer - banned
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#banned@user:sarah"),
				tuple.MustParse("document:first#banned@user:fred"),
				tuple.MustParse("document:first#banned@user:tom"),
				tuple.MustParse("document:first#banned@user:andria"),
				tuple.MustParse("document:first#banned@user:victor"),
				tuple.MustParse("document:first#banned@user:chuck"),
				tuple.MustParse("document:first#banned@user:ben"),
				tuple.MustParse("document:first#viewer@user:*"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "*",
					ExcludedSubjects: []*v1.FoundSubject{
						{SubjectId: "sarah"},
						{SubjectId: "fred"},
						{SubjectId: "tom"},
						{SubjectId: "andria"},
						{SubjectId: "victor"},
						{SubjectId: "chuck"},
						{SubjectId: "ben"},
					},
				},
			},
		},
		{
			"canceling exclusions on wildcards",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

		 	 definition document {
				relation viewer: user
				relation banned: user:*
				relation banned2: user
				permission view = viewer - (banned - banned2)
  		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#viewer@user:sarah"),
				tuple.MustParse("document:first#viewer@user:fred"),
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustParse("document:first#viewer@user:andria"),
				tuple.MustParse("document:first#viewer@user:victor"),
				tuple.MustParse("document:first#viewer@user:chuck"),
				tuple.MustParse("document:first#viewer@user:ben"),

				tuple.MustParse("document:first#banned@user:*"),

				tuple.MustParse("document:first#banned2@user:andria"),
				tuple.MustParse("document:first#banned2@user:tom"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "andria",
				},
				{
					SubjectId: "tom",
				},
			},
		},
		{
			"wildcard with many, many exclusions",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

		 	 definition document {
				relation viewer: user:*
				relation banned: user
				permission view = viewer - banned
  		 }`,
			(func() []*corev1.RelationTuple {
				tuples := make([]*corev1.RelationTuple, 0, 201)
				tuples = append(tuples, tuple.MustParse("document:first#viewer@user:*"))
				for i := 0; i < 200; i++ {
					tuples = append(tuples, tuple.MustParse(fmt.Sprintf("document:first#banned@user:u%03d", i)))
				}
				return tuples
			})(),
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "*",
					ExcludedSubjects: (func() []*v1.FoundSubject {
						fs := make([]*v1.FoundSubject, 0, 200)
						for i := 0; i < 200; i++ {
							fs = append(fs, &v1.FoundSubject{SubjectId: fmt.Sprintf("u%03d", i)})
						}
						return fs
					})(),
				},
			},
		},
		{
			"simple arrow",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

	 		definition folder {
				relation parent: folder
				relation viewer: user
				permission view = viewer + parent->view	
			}

		 	 definition document {
				relation parent: folder
				relation viewer: user
				permission view = viewer + parent->view
  		    }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#viewer@user:sarah"),
				tuple.MustParse("document:first#viewer@user:fred"),

				tuple.MustParse("document:first#parent@folder:somefolder"),
				tuple.MustParse("folder:somefolder#viewer@user:victoria"),
				tuple.MustParse("folder:somefolder#viewer@user:tommy"),

				tuple.MustParse("folder:somefolder#parent@folder:another"),
				tuple.MustParse("folder:another#viewer@user:diana"),

				tuple.MustParse("folder:another#parent@folder:root"),
				tuple.MustParse("folder:root#viewer@user:zeus"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{SubjectId: "sarah"},
				{SubjectId: "fred"},
				{SubjectId: "victoria"},
				{SubjectId: "diana"},
				{SubjectId: "tommy"},
				{SubjectId: "zeus"},
			},
		},
		{
			"simple indirect",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

		 	 definition document {
				relation viewer: user | document#viewer
				permission view = viewer
     		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#viewer@user:sarah"),
				tuple.MustParse("document:first#viewer@user:fred"),

				tuple.MustParse("document:second#viewer@user:tom"),
				tuple.MustParse("document:second#viewer@user:mark"),

				tuple.MustParse("document:first#viewer@document:second#viewer"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{SubjectId: "sarah"},
				{SubjectId: "fred"},
				{SubjectId: "tom"},
				{SubjectId: "mark"},
			},
		},
		{
			"indirect with combined caveat",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

			 caveat somecaveat(some int) {
				some == 42
			 }

			 caveat anothercaveat(some int) {
				some == 43
			 }

			 definition otherresource {
		 	 	relation viewer: user with anothercaveat
		 	 }

		 	 definition document {
				relation viewer: user with somecaveat | otherresource#viewer
				permission view = viewer
     		 }`,
			[]*corev1.RelationTuple{
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:tom"), "somecaveat"),

				tuple.MustWithCaveat(tuple.MustParse("otherresource:second#viewer@user:tom"), "anothercaveat"),

				tuple.MustParse("document:first#viewer@otherresource:second#viewer"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "tom",
					CaveatExpression: caveatOr(
						caveatexpr("somecaveat"),
						caveatexpr("anothercaveat"),
					),
				},
			},
		},
		{
			"indirect with combined caveat direct",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

			 caveat somecaveat(some int) {
				some == 42
			 }

			 caveat anothercaveat(some int) {
				some == 43
			 }

			 definition otherresource {
		 	 	relation viewer: user with anothercaveat
		 	 }

		 	 definition document {
				relation viewer: user with somecaveat | otherresource#viewer
				permission view = viewer
     		 }`,
			[]*corev1.RelationTuple{
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:tom"), "somecaveat"),

				tuple.MustWithCaveat(tuple.MustParse("otherresource:second#viewer@user:tom"), "anothercaveat"),

				tuple.MustParse("document:first#viewer@otherresource:second#viewer"),
			},
			ONR("document", "first", "viewer"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "tom",
					CaveatExpression: caveatOr(
						caveatexpr("somecaveat"),
						caveatexpr("anothercaveat"),
					),
				},
			},
		},
		{
			"non-terminal subject",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

		 	 definition document {
				relation viewer: user | document#viewer
				permission view = viewer
     		 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#viewer@user:sarah"),
				tuple.MustParse("document:first#viewer@user:fred"),

				tuple.MustParse("document:second#viewer@user:tom"),
				tuple.MustParse("document:second#viewer@user:mark"),

				tuple.MustParse("document:first#viewer@document:second#viewer"),
			},
			ONR("document", "first", "view"),
			RR("document", "viewer"),
			[]*v1.FoundSubject{
				{SubjectId: "first"},
				{SubjectId: "second"},
			},
		},
		{
			"indirect non-terminal subject",
			[]int{0, 1, 2, 5, 100},
			`definition user {}

   		     definition folder {
				relation parent_view: folder#view
				relation viewer: user
				permission view = viewer + parent_view
			 }

			 definition document {
			   relation parent_view: folder#view
			   relation viewer: user
			   permission view = viewer + parent_view
			 }`,
			[]*corev1.RelationTuple{
				tuple.MustParse("document:first#parent_view@folder:somefolder#view"),
				tuple.MustParse("folder:somefolder#parent_view@folder:anotherfolder#view"),
			},
			ONR("document", "first", "view"),
			RR("folder", "view"),
			[]*v1.FoundSubject{
				{SubjectId: "anotherfolder"},
				{SubjectId: "somefolder"},
			},
		},
		{
			"large direct",
			[]int{0, 100, 104, 503, 1012, 10056},
			`definition user {}

		 	 definition document {
				relation viewer: user
				permission view = viewer
  		 }`,
			(func() []*corev1.RelationTuple {
				tuples := make([]*corev1.RelationTuple, 0, 20000)
				for i := 0; i < 20000; i++ {
					tuples = append(tuples, tuple.MustParse(fmt.Sprintf("document:first#viewer@user:u%03d", i)))
				}
				return tuples
			})(),
			ONR("document", "first", "view"),
			RR("user", "..."),
			(func() []*v1.FoundSubject {
				fs := make([]*v1.FoundSubject, 0, 20000)
				for i := 0; i < 20000; i++ {
					fs = append(fs, &v1.FoundSubject{SubjectId: fmt.Sprintf("u%03d", i)})
				}
				return fs
			})(),
		},
		{
			"large with intersection",
			[]int{0, 100, 104, 503, 1012, 10056},
			`definition user {}

		 	 definition document {
				relation viewer1: user
				relation viewer2: user
				permission view = viewer1 & viewer2
  		 }`,
			(func() []*corev1.RelationTuple {
				tuples := make([]*corev1.RelationTuple, 0, 20000)
				for i := 0; i < 20000; i++ {
					tuples = append(tuples, tuple.MustParse(fmt.Sprintf("document:first#viewer1@user:u%03d", i)))
					tuples = append(tuples, tuple.MustParse(fmt.Sprintf("document:first#viewer2@user:u%03d", i)))
				}
				return tuples
			})(),
			ONR("document", "first", "view"),
			RR("user", "..."),
			(func() []*v1.FoundSubject {
				fs := make([]*v1.FoundSubject, 0, 20000)
				for i := 0; i < 20000; i++ {
					fs = append(fs, &v1.FoundSubject{SubjectId: fmt.Sprintf("u%03d", i)})
				}
				return fs
			})(),
		},
		{
			"large with partial intersection",
			[]int{0, 100, 104, 503, 1012, 10056},
			`definition user {}

		 	 definition document {
				relation viewer1: user
				relation viewer2: user
				permission view = viewer1 & viewer2
  		 }`,
			(func() []*corev1.RelationTuple {
				tuples := make([]*corev1.RelationTuple, 0, 20000)
				for i := 0; i < 20000; i++ {
					tuples = append(tuples, tuple.MustParse(fmt.Sprintf("document:first#viewer1@user:u%03d", i)))

					if i >= 10000 {
						tuples = append(tuples, tuple.MustParse(fmt.Sprintf("document:first#viewer2@user:u%03d", i)))
					}
				}
				return tuples
			})(),
			ONR("document", "first", "view"),
			RR("user", "..."),
			(func() []*v1.FoundSubject {
				fs := make([]*v1.FoundSubject, 0, 10000)
				for i := 10000; i < 20000; i++ {
					fs = append(fs, &v1.FoundSubject{SubjectId: fmt.Sprintf("u%03d", i)})
				}
				return fs
			})(),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, limit := range tc.pageSizes {
				t.Run(fmt.Sprintf("limit-%d_", limit), func(t *testing.T) {
					require := require.New(t)

					dispatcher := NewLocalOnlyDispatcher(10)

					ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
					require.NoError(err)

					ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, require)

					ctx := datastoremw.ContextWithHandle(context.Background())
					require.NoError(datastoremw.SetInContext(ctx, ds))

					var cursor *v1.Cursor
					overallResults := []*v1.FoundSubject{}

					iterCount := 1
					if limit > 0 {
						iterCount = (len(tc.expected) / limit) + 1
					}

					for i := 0; i < iterCount; i++ {
						stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
						err = dispatcher.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
							ResourceRelation: &corev1.RelationReference{
								Namespace: tc.start.Namespace,
								Relation:  tc.start.Relation,
							},
							ResourceIds:     []string{tc.start.ObjectId},
							SubjectRelation: tc.target,
							Metadata: &v1.ResolverMeta{
								AtRevision:     revision.String(),
								DepthRemaining: 50,
							},
							OptionalLimit:  uint32(limit),
							OptionalCursor: cursor,
						}, stream)
						require.NoError(err)

						results := []*v1.FoundSubject{}
						hasWildcard := false

						for _, streamResult := range stream.Results() {
							for _, foundSubjects := range streamResult.FoundSubjectsByResourceId {
								results = append(results, foundSubjects.FoundSubjects...)
								for _, fs := range foundSubjects.FoundSubjects {
									if fs.SubjectId == tuple.PublicWildcard {
										hasWildcard = true
									}
								}
							}
							cursor = streamResult.AfterResponseCursor
						}

						if limit > 0 {
							// If there is a wildcard, its allowed to bypass the limit.
							if hasWildcard {
								require.LessOrEqual(len(results), limit+1)
							} else {
								require.LessOrEqual(len(results), limit)
							}
						}

						overallResults = append(overallResults, results...)
					}

					// NOTE: since cursored LS now can return a wildcard multiple times, we need to combine
					// them here before comparison.
					normalizedResults := combineWildcards(overallResults)
					itestutil.RequireEquivalentSets(t, tc.expected, normalizedResults)
				})
			}
		})
	}
}

func combineWildcards(results []*v1.FoundSubject) []*v1.FoundSubject {
	combined := make([]*v1.FoundSubject, 0, len(results))
	var wildcardResult *v1.FoundSubject
	for _, result := range results {
		if result.SubjectId != tuple.PublicWildcard {
			combined = append(combined, result)
			continue
		}

		if wildcardResult == nil {
			wildcardResult = result
			combined = append(combined, result)
			continue
		}

		wildcardResult.ExcludedSubjects = append(wildcardResult.ExcludedSubjects, result.ExcludedSubjects...)
	}
	return combined
}
