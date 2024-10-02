package graph

import (
	"fmt"
	"math"
	"testing"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/stretchr/testify/require"
)

func TestDispatchChunking(t *testing.T) {
	t.Parallel()
	schema := `
		definition user {
			relation self: user
		}

		definition res {
			relation owner : user
			permission view = owner->self
		}`

	resources := make([]*core.RelationTuple, 0, math.MaxUint16+1)
	enabled := make([]*core.RelationTuple, 0, math.MaxUint16+1)
	for i := 0; i < math.MaxUint16+1; i++ {
		resources = append(resources, tuple.Parse(fmt.Sprintf("res:res1#owner@user:user%d", i)))
		enabled = append(enabled, tuple.Parse(fmt.Sprintf("user:user%d#self@user:user%d", i, i)))
	}

	ctx, dispatcher, revision := newLocalDispatcherWithSchemaAndRels(t, schema, append(enabled, resources...))

	t.Run("check", func(t *testing.T) {
		t.Parallel()
		for _, tpl := range resources[:1] {
			checkResult, err := dispatcher.DispatchCheck(ctx, &v1.DispatchCheckRequest{
				ResourceRelation: RR(tpl.ResourceAndRelation.Namespace, "view"),
				ResourceIds:      []string{tpl.ResourceAndRelation.ObjectId},
				ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
				Subject:          ONR(tpl.Subject.Namespace, tpl.Subject.ObjectId, graph.Ellipsis),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
			})

			require.NoError(t, err)
			require.NotNil(t, checkResult)
			require.NotEmpty(t, checkResult.ResultsByResourceId, "expected membership for resource %s", tpl.ResourceAndRelation.ObjectId)
			require.Equal(t, v1.ResourceCheckResult_MEMBER, checkResult.ResultsByResourceId[tpl.ResourceAndRelation.ObjectId].Membership)
		}
	})

	t.Run("lookup-resources", func(t *testing.T) {
		t.Parallel()

		for _, tpl := range resources[:1] {
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)
			err := dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
				ObjectRelation: RR(tpl.ResourceAndRelation.Namespace, "view"),
				Subject:        ONR(tpl.Subject.Namespace, tpl.Subject.ObjectId, graph.Ellipsis),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				OptionalLimit: veryLargeLimit,
			}, stream)

			require.NoError(t, err)

			foundResources, _, _, _ := processResults(stream)
			require.Len(t, foundResources, 1)
		}
	})

	t.Run("lookup-subjects", func(t *testing.T) {
		t.Parallel()

		for _, tpl := range resources[:1] {
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)

			err := dispatcher.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: RR(tpl.ResourceAndRelation.Namespace, "view"),
				ResourceIds:      []string{tpl.ResourceAndRelation.ObjectId},
				SubjectRelation:  RR(tpl.Subject.Namespace, graph.Ellipsis),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
			}, stream)

			require.NoError(t, err)
			res := stream.Results()
			require.Len(t, res, 1)
			require.Len(t, res[0].FoundSubjectsByResourceId, 1)
			require.NotNil(t, res[0].FoundSubjectsByResourceId["res1"])
			require.Len(t, res[0].FoundSubjectsByResourceId["res1"].FoundSubjects, math.MaxUint16+1)
		}
	})
}
