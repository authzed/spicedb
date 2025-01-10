package graph

import (
	"fmt"
	"math"
	"testing"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/stretchr/testify/require"
)

const veryLargeLimit = 1000000000

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

	resources := make([]tuple.Relationship, 0, math.MaxUint16+1)
	enabled := make([]tuple.Relationship, 0, math.MaxUint16+1)
	for i := 0; i < math.MaxUint16+1; i++ {
		resources = append(resources, tuple.MustParse(fmt.Sprintf("res:res1#owner@user:user%d", i)))
		enabled = append(enabled, tuple.MustParse(fmt.Sprintf("user:user%d#self@user:user%d", i, i)))
	}

	ctx, dispatcher, revision := newLocalDispatcherWithSchemaAndRels(t, schema, append(enabled, resources...))

	t.Run("check", func(t *testing.T) {
		t.Parallel()
		for _, tpl := range resources[:1] {
			checkResult, err := dispatcher.DispatchCheck(ctx, &v1.DispatchCheckRequest{
				ResourceRelation: RR(tpl.Resource.ObjectType, "view").ToCoreRR(),
				ResourceIds:      []string{tpl.Resource.ObjectID},
				ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
				Subject:          tuple.CoreONR(tpl.Subject.ObjectType, tpl.Subject.ObjectID, graph.Ellipsis),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
			})

			require.NoError(t, err)
			require.NotNil(t, checkResult)
			require.NotEmpty(t, checkResult.ResultsByResourceId, "expected membership for resource %s", tpl.Resource.ObjectID)
			require.Equal(t, v1.ResourceCheckResult_MEMBER, checkResult.ResultsByResourceId[tpl.Resource.ObjectID].Membership)
		}
	})

	t.Run("lookup-resources2", func(t *testing.T) {
		t.Parallel()

		for _, tpl := range resources[:1] {
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)
			err := dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
				ResourceRelation: RR(tpl.Resource.ObjectType, "view").ToCoreRR(),
				SubjectRelation:  RR(tpl.Subject.ObjectType, graph.Ellipsis).ToCoreRR(),
				SubjectIds:       []string{tpl.Subject.ObjectID},
				TerminalSubject:  tuple.CoreONR(tpl.Subject.ObjectType, tpl.Subject.ObjectID, graph.Ellipsis),
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
				ResourceRelation: RR(tpl.Resource.ObjectType, "view").ToCoreRR(),
				ResourceIds:      []string{tpl.Resource.ObjectID},
				SubjectRelation:  RR(tpl.Subject.ObjectType, graph.Ellipsis).ToCoreRR(),
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
