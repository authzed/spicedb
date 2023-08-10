package v1

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var MaxBulkCheckDispatchChunkSize = datastore.FilterMaximumIDCount

type groupedCheckParameters struct {
	params      computed.CheckParameters
	resourceIDs []string
}

type groupingParameters struct {
	atRevision           datastore.Revision
	maximumAPIDepth      uint32
	maxCaveatContextSize int
}

// groupItems takes a slice of BulkCheckPermissionRequestItem and groups them based
// on using the same permission, subject type, subject id, and caveat.
func groupItems(ctx context.Context, params groupingParameters, items []*v1.BulkCheckPermissionRequestItem) ([]groupedCheckParameters, error) {
	groups := mapz.NewMultiMap[string, string]()
	groupParams := make(map[string]computed.CheckParameters, len(items))

	for _, item := range items {
		hash, err := computeBulkCheckPermissionItemHashWithoutResourceID(item)
		if err != nil {
			return nil, err
		}

		if _, ok := groupParams[hash]; !ok {
			caveatContext, err := GetCaveatContext(ctx, item.Context, params.maxCaveatContextSize)
			if err != nil {
				return nil, err
			}

			groupParams[hash] = computed.CheckParameters{
				ResourceType: &core.RelationReference{
					Namespace: item.Resource.ObjectType,
					Relation:  item.Permission,
				},
				Subject: &core.ObjectAndRelation{
					Namespace: item.Subject.Object.ObjectType,
					ObjectId:  item.Subject.Object.ObjectId,
					Relation:  normalizeSubjectRelation(item.Subject),
				},
				CaveatContext: caveatContext,
				AtRevision:    params.atRevision,
				MaximumDepth:  params.maximumAPIDepth,
				DebugOption:   computed.NoDebugging,
			}
		}

		groups.Add(hash, item.Resource.ObjectId)
	}

	grouped := make([]groupedCheckParameters, 0, groups.Len())
	for _, key := range groups.Keys() {
		resourceIDs, _ := groups.Get(key)
		grouped = append(grouped, groupedCheckParameters{
			params:      groupParams[key],
			resourceIDs: resourceIDs,
		})
	}

	return grouped, nil
}
