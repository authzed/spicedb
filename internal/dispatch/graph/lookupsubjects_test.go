package graph

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
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
		t.Run(fmt.Sprintf("simple-lookup-subjects:%s:%s:%s:%s:%s", tc.resourceType, tc.resourceID, tc.permission, tc.subjectType, tc.subjectRelation), func(t *testing.T) {
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

	tpl := tuple.Parse("folder:oops#owner@folder:oops#owner")
	revision, err := common.WriteTuples(ctx, ds, corev1.RelationTupleUpdate_CREATE, tpl)
	require.NoError(err)
	require.True(revision.GreaterThan(datastore.NoRevision))

	dis := NewLocalOnlyDispatcher(10)
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)

	err = dis.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
		ResourceRelation: RR("folder", "owner"),
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
				tuple.WithCaveat(tuple.MustParse("document:first#viewer@user:tom"), "somecaveat"),
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
				tuple.WithCaveat(tuple.MustParse("document:first#viewer@user:tom"), "somecaveat"),
				tuple.WithCaveat(tuple.MustParse("document:first#editor@user:tom"), "somecaveat"),
			},
			ONR("document", "first", "view"),
			RR("user", "..."),
			[]*v1.FoundSubject{
				{
					SubjectId: "tom",
					CaveatExpression: caveatOr(
						caveatexpr("somecaveat"),
						caveatexpr("somecaveat"),
					),
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
				tuple.WithCaveat(tuple.MustParse("document:first#editor@user:tom"), "somecaveat"),
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
				tuple.WithCaveat(tuple.MustParse("document:first#viewer@user:tom"), "somecaveat"),
				tuple.WithCaveat(tuple.MustParse("document:first#editor@user:tom"), "anothercaveat"),
				tuple.WithCaveat(tuple.MustParse("document:first#viewer@user:sarah"), "somecaveat"),
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
				tuple.WithCaveat(tuple.MustParse("document:first#viewer@user:tom"), "somecaveat"),
				tuple.WithCaveat(tuple.MustParse("document:first#banned@user:tom"), "anothercaveat"),
				tuple.WithCaveat(tuple.MustParse("document:first#viewer@user:sarah"), "somecaveat"),
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
				tuple.WithCaveat(tuple.MustParse("document:first#org@org:someorg"), "somecaveat"),
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
				tuple.WithCaveat(tuple.MustParse("document:first#org@org:someorg"), "somecaveat"),
				tuple.WithCaveat(tuple.MustParse("org:someorg#viewer@user:tom"), "anothercaveat"),
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
				tuple.WithCaveat(tuple.MustParse("document:first#viewer@user:*"), "somecaveat"),
				tuple.WithCaveat(tuple.MustParse("document:first#banned@user:tom"), "anothercaveat"),
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
				tuple.WithCaveat(tuple.MustParse("document:first#viewer@user:*"), "somecaveat"),
				tuple.WithCaveat(tuple.MustParse("document:first#banned@user:tom"), "anothercaveat"),
				tuple.WithCaveat(tuple.MustParse("document:first#explicitly_allowed@user:tom"), "thirdcaveat"),
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
				tuple.WithCaveat(tuple.MustParse("document:first#org@org:someorg"), "somecaveat"),
				tuple.WithCaveat(tuple.MustParse("org:someorg#viewer@user:tom"), "anothercaveat"),
				tuple.MustParse("org:someorg#viewer@user:sarah"),

				tuple.WithCaveat(tuple.MustParse("document:first#org@org:anotherorg"), "thirdcaveat"),
				tuple.WithCaveat(tuple.MustParse("org:anotherorg#viewer@user:amy"), "anothercaveat"),
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
	}

	for _, tc := range testCases {
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

			results := []*v1.FoundSubject{}
			for _, streamResult := range stream.Results() {
				for _, foundSubjects := range streamResult.FoundSubjectsByResourceId {
					results = append(results, foundSubjects.FoundSubjects...)
				}
			}
			sort.Sort(byFoundSubjectAndCaveat(results))
			sort.Sort(byFoundSubjectAndCaveat(tc.expected))

			require.NoError(err)
			require.Equal(len(tc.expected), len(results), "Found: %v, Expected: %v", results, tc.expected)
			for index := range tc.expected {
				require.True(proto.Equal(tc.expected[index], results[index]), "Found: %v, Expected: %v", results[index], tc.expected[index])
			}
		})
	}
}

type byFoundSubjectAndCaveat []*v1.FoundSubject

func (a byFoundSubjectAndCaveat) Len() int { return len(a) }
func (a byFoundSubjectAndCaveat) Less(i, j int) bool {
	return strings.Compare(
		fmt.Sprintf("%s:%v", a[i].SubjectId, a[i].CaveatExpression),
		fmt.Sprintf("%s:%v", a[j].SubjectId, a[j].CaveatExpression),
	) < 0
}
func (a byFoundSubjectAndCaveat) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func caveatexpr(name string) *v1.CaveatExpression {
	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Caveat{
			Caveat: caveat(name),
		},
	}
}

func caveat(name string) *corev1.ContextualizedCaveat {
	return &corev1.ContextualizedCaveat{
		CaveatName: name,
		Context:    &structpb.Struct{},
	}
}

func caveatOr(first *v1.CaveatExpression, second *v1.CaveatExpression) *v1.CaveatExpression {
	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Operation{
			Operation: &v1.CaveatOperation{
				Op:       v1.CaveatOperation_OR,
				Children: []*v1.CaveatExpression{first, second},
			},
		},
	}
}

func caveatAnd(first *v1.CaveatExpression, second *v1.CaveatExpression) *v1.CaveatExpression {
	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Operation{
			Operation: &v1.CaveatOperation{
				Op:       v1.CaveatOperation_AND,
				Children: []*v1.CaveatExpression{first, second},
			},
		},
	}
}

func caveatInvert(ce *v1.CaveatExpression) *v1.CaveatExpression {
	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Operation{
			Operation: &v1.CaveatOperation{
				Op:       v1.CaveatOperation_NOT,
				Children: []*v1.CaveatExpression{ce},
			},
		},
	}
}
