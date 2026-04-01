package graph

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/pkg/datalayer"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const veryLargeLimit = 1000000000

func TestDispatchChunking(t *testing.T) {
	schema := `
		definition user {
			relation its_a_me: user
		}

		definition res {
			relation owner : user
			permission view = owner->its_a_me
		}`

	resources := make([]tuple.Relationship, 0, math.MaxUint16+1)
	enabled := make([]tuple.Relationship, 0, math.MaxUint16+1)
	for i := range math.MaxUint16 + 1 {
		resources = append(resources, tuple.MustParse(fmt.Sprintf("res:res1#owner@user:user%d", i)))
		enabled = append(enabled, tuple.MustParse(fmt.Sprintf("user:user%d#its_a_me@user:user%d", i, i)))
	}

	ctx, dispatcher, revision := newLocalDispatcherWithSchemaAndRels(t, schema, append(enabled, resources...))

	t.Run("check", func(t *testing.T) {
		for _, tpl := range resources[:1] {
			checkResult, err := dispatcher.DispatchCheck(ctx, &v1.DispatchCheckRequest{
				ResourceRelation: RR(tpl.Resource.ObjectType, "view").ToCoreRR(),
				ResourceIds:      []string{tpl.Resource.ObjectID},
				ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
				Subject:          tuple.CoreONR(tpl.Subject.ObjectType, tpl.Subject.ObjectID, graph.Ellipsis),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
					SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
				},
			})

			require.NoError(t, err)
			require.NotNil(t, checkResult)
			require.NotEmpty(t, checkResult.ResultsByResourceId, "expected membership for resource %s", tpl.Resource.ObjectID)
			require.Equal(t, v1.ResourceCheckResult_MEMBER, checkResult.ResultsByResourceId[tpl.Resource.ObjectID].Membership)
		}
	})

	t.Run("lookup-resources2", func(t *testing.T) {
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
					SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
				},
				OptionalLimit: veryLargeLimit,
			}, stream)

			require.NoError(t, err)

			foundResources, _, _, _ := processResults(stream)
			require.Len(t, foundResources, 1)
		}
	})

	t.Run("lookup-subjects", func(t *testing.T) {
		for _, tpl := range resources[:1] {
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)

			err := dispatcher.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: RR(tpl.Resource.ObjectType, "view").ToCoreRR(),
				ResourceIds:      []string{tpl.Resource.ObjectID},
				SubjectRelation:  RR(tpl.Subject.ObjectType, graph.Ellipsis).ToCoreRR(),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
					SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
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
