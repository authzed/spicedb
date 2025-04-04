package graph

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	itestutil "github.com/authzed/spicedb/internal/testutil"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	caveatexpr   = caveats.CaveatExprForTesting
	caveatAndCtx = caveats.MustCaveatExprForTestingWithContext
	caveatAnd    = caveats.And
	caveatInvert = caveats.Invert
	caveatOr     = caveats.Or
)

func TestSimpleLookupSubjects(t *testing.T) {
	t.Parallel()

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
			t.Parallel()

			require := require.New(t)

			ctx, dis, revision := newLocalDispatcher(t)
			defer dis.Close()

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)

			err := dis.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: RR(tc.resourceType, tc.permission).ToCoreRR(),
				ResourceIds:      []string{tc.resourceID},
				SubjectRelation:  RR(tc.subjectType, tc.subjectRelation).ToCoreRR(),
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
					ResourceRelation: RR(tc.resourceType, tc.permission).ToCoreRR(),
					ResourceIds:      []string{tc.resourceID},
					ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
					Subject:          ONR(tc.subjectType, subjectID, tc.subjectRelation).ToCoreONR(),
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
	t.Parallel()
	require := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)

	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
	require.NoError(datastoremw.SetInContext(ctx, ds))

	tpl := tuple.MustParse("folder:oops#owner@folder:oops#owner")
	revision, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, tpl)
	require.NoError(err)

	dis := NewLocalOnlyDispatcher(10, 100)
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)

	err = dis.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
		ResourceRelation: RR("folder", "owner").ToCoreRR(),
		ResourceIds:      []string{"oops"},
		SubjectRelation:  RR("user", "...").ToCoreRR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
	}, stream)
	require.Error(err)
}

func TestLookupSubjectsDispatchCount(t *testing.T) {
	t.Parallel()
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
			t.Parallel()

			require := require.New(t)

			ctx, dis, revision := newLocalDispatcher(t)
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)

			err := dis.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: RR(tc.resourceType, tc.permission).ToCoreRR(),
				ResourceIds:      []string{tc.resourceID},
				SubjectRelation:  RR(tc.subjectType, tc.subjectRelation).ToCoreRR(),
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

func TestLookupSubjectsOverSchema(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name          string
		schema        string
		relationships []tuple.Relationship
		start         tuple.ObjectAndRelation
		target        tuple.RelationReference
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
		{
			"simple arrow",
			`definition user {}
	
			 definition folder {
				relation viewer: user
				permission view = viewer
			 }

		 	 definition document {
				relation folder: folder
				permission view = folder->view
  		 }`,
			[]tuple.Relationship{
				tuple.MustParse("folder:folder1#viewer@user:tom"),
				tuple.MustParse("folder:folder1#viewer@user:fred"),
				tuple.MustParse("document:somedoc#folder@folder:folder1"),
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
			"simple any arrow",
			`definition user {}
	
			 definition folder {
				relation viewer: user
				permission view = viewer
			 }

		 	 definition document {
				relation folder: folder
				permission view = folder.any(view)
  		 }`,
			[]tuple.Relationship{
				tuple.MustParse("folder:folder1#viewer@user:tom"),
				tuple.MustParse("folder:folder1#viewer@user:fred"),
				tuple.MustParse("document:somedoc#folder@folder:folder1"),
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
			"simple all arrow",
			`definition user {}
	
			 definition folder {
				relation viewer: user
				permission view = viewer
			 }

		 	 definition document {
				relation folder: folder
				permission view = folder.all(view)
  		 }`,
			[]tuple.Relationship{
				tuple.MustParse("folder:folder1#viewer@user:tom"),
				tuple.MustParse("folder:folder1#viewer@user:fred"),
				tuple.MustParse("document:somedoc#folder@folder:folder1"),
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
			"all arrow multiple",
			`definition user {}
	
			 definition folder {
				relation viewer: user
				permission view = viewer
			 }

		 	 definition document {
				relation folder: folder
				permission view = folder.all(view)
  		 }`,
			[]tuple.Relationship{
				tuple.MustParse("folder:folder1#viewer@user:tom"),
				tuple.MustParse("folder:folder1#viewer@user:fred"),
				tuple.MustParse("folder:folder2#viewer@user:fred"),
				tuple.MustParse("document:somedoc#folder@folder:folder1"),
				tuple.MustParse("document:somedoc#folder@folder:folder2"),
			},
			ONR("document", "somedoc", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "fred",
				},
			},
		},
		{
			"all arrow over multiple resource IDs",
			`definition user {}
	
			 definition organization {
			 	relation member: user
			 }

			 definition folder {
   			    relation parent: organization
				permission view = parent.all(member)
			 }

		 	 definition document {
				relation folder: folder
				permission view = folder->view
  		 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:somedoc#folder@folder:folder1"),
				tuple.MustParse("document:somedoc#folder@folder:folder2"),
				tuple.MustParse("folder:folder1#parent@organization:org1"),
				tuple.MustParse("folder:folder2#parent@organization:org2"),
				tuple.MustParse("folder:folder2#parent@organization:org3"),
				tuple.MustParse("organization:org1#member@user:fred"),
				tuple.MustParse("organization:org2#member@user:tom"),
				tuple.MustParse("organization:org3#member@user:tom"),
				tuple.MustParse("organization:org2#member@user:sarah"),
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
			"intersection arrow over caveated teams",
			`definition user {}
	
			 definition team {
			 	relation member: user | user with anothercaveat
			 }

			 caveat caveat1(someparam1 int) {
				someparam1 == 42
			 }

 			 caveat caveat2(someparam2 int) {
				someparam2 == 42
			 }

			 caveat anothercaveat(anotherparam int) {
			 	anotherparam == 43
			 }

		 	 definition document {
				relation team: team with caveat1 | team with caveat2
				permission view = team.all(member)
  		 }`,
			[]tuple.Relationship{
				tuple.MustParse(`document:somedoc#team@team:team1[caveat1:{":someparam1":42}]`),
				tuple.MustParse(`document:somedoc#team@team:team2[caveat2:{":someparam2":43}]`),
				tuple.MustParse(`team:team1#member@user:tom`),
				tuple.MustParse(`team:team2#member@user:tom`),
				tuple.MustParse(`team:team1#member@user:fred`),
				tuple.MustParse(`team:team2#member@user:fred[anothercaveat:{":anotherparam":43}]`),
				tuple.MustParse(`team:team1#member@user:sarah`),
			},
			ONR("document", "somedoc", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "tom",
					CaveatExpression: caveatAnd(
						caveatAndCtx("caveat1", map[string]interface{}{"someparam1": 42}),
						caveatAndCtx("caveat2", map[string]interface{}{"someparam2": 43}),
					),
				},
				{
					SubjectId: "fred",
					CaveatExpression: caveatAnd(
						caveatAnd(
							caveatAndCtx("caveat1", map[string]interface{}{"someparam1": 42}),
							caveatAndCtx("caveat2", map[string]interface{}{"someparam2": 43}),
						),
						caveatAndCtx("anothercaveat", map[string]interface{}{"anotherparam": 43}),
					),
				},
			},
		},
		{
			"all arrow minus banned",
			`definition user {}
	
			 definition folder {
				relation viewer: user
				permission view = viewer
			 }

		 	 definition document {
			 	relation banned: user
				relation folder: folder
				permission view = folder.all(view) - banned
  		 }`,
			[]tuple.Relationship{
				tuple.MustParse("folder:folder1#viewer@user:tom"),
				tuple.MustParse("folder:folder1#viewer@user:fred"),
				tuple.MustParse("document:somedoc#folder@folder:folder1"),
				tuple.MustParse("document:somedoc#banned@user:fred"),
				tuple.MustParse("document:somedoc#banned@user:sarah"),
			},
			ONR("document", "somedoc", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "tom",
				},
			},
		},
		{
			"recursive all arrow ",
			`definition user {}

			definition folder {
				relation parent: folder
				relation owner: user

				permission view = parent.all(owner)
			}

			definition document {
				relation folder: folder
				permission view = folder.all(view)
			}`,
			[]tuple.Relationship{
				tuple.MustParse("folder:root1#owner@user:tom"),
				tuple.MustParse("folder:root1#owner@user:fred"),
				tuple.MustParse("folder:root1#owner@user:sarah"),
				tuple.MustParse("folder:root2#owner@user:fred"),
				tuple.MustParse("folder:root2#owner@user:sarah"),

				tuple.MustParse("folder:child1#parent@folder:root1"),
				tuple.MustParse("folder:child1#parent@folder:root2"),

				tuple.MustParse("folder:child2#parent@folder:root1"),
				tuple.MustParse("folder:child2#parent@folder:root2"),

				tuple.MustParse("document:doc1#folder@folder:child1"),
				tuple.MustParse("document:doc1#folder@folder:child2"),
			},
			ONR("document", "doc1", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "fred",
				},
				{
					SubjectId: "sarah",
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require := require.New(t)

			dispatcher := NewLocalOnlyDispatcher(10, 100)

			ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			require.NoError(err)

			ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, require)

			ctx := datastoremw.ContextWithHandle(context.Background())
			require.NoError(datastoremw.SetInContext(ctx, ds))

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
			err = dispatcher.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: tc.start.RelationReference().ToCoreRR(),
				ResourceIds:      []string{tc.start.ObjectID},
				SubjectRelation:  tc.target.ToCoreRR(),
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
