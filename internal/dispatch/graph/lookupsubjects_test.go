package graph

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSimpleLookupSubjects(t *testing.T) {
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

			ctx, dis, revision := newLocalDispatcher(require)
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
				for _, found := range result.FoundSubjects {
					if len(found.ExcludedSubjectIds) > 0 {
						continue
					}

					foundSubjectIds = append(foundSubjectIds, found.SubjectId)
				}
			}

			sort.Strings(foundSubjectIds)
			sort.Strings(tc.expectedSubjects)
			require.Equal(tc.expectedSubjects, foundSubjectIds)

			// Ensure every subject found has access.
			for _, subjectID := range foundSubjectIds {
				checkResult, err := dis.DispatchCheck(ctx, &v1.DispatchCheckRequest{
					ResourceAndRelation: ONR(tc.resourceType, tc.resourceID, tc.permission),
					Subject:             ONR(tc.subjectType, subjectID, tc.subjectRelation),
					Metadata: &v1.ResolverMeta{
						AtRevision:     revision.String(),
						DepthRemaining: 50,
					},
				})

				require.NoError(err)
				require.Equal(v1.DispatchCheckResponse_MEMBER, checkResult.Membership)
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
	require.True(revision.GreaterThan(decimal.Zero))

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

			ctx, dis, revision := newLocalDispatcher(require)
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
