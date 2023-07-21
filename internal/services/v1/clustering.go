package v1

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"golang.org/x/exp/maps"

	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var MaxBulkCheckDispatchChunkSize = datastore.FilterMaximumIDCount

type clusteredCheckParameters struct {
	params             computed.CheckParameters
	chunkedResourceIDs [][]string
}

type clusteringParameters struct {
	atRevision           datastore.Revision
	maximumAPIDepth      uint32
	maxCaveatContextSize int
}

// clusterItems takes a slice of BulkCheckPermissionRequestItem and cluster them based
// on using the same resource type, subject type, subject id, and caveat.
func clusterItems(ctx context.Context, params clusteringParameters, items []*v1.BulkCheckPermissionRequestItem) ([]clusteredCheckParameters, error) {
	clustered := make(map[string]clusteredCheckParameters, len(items))
	for _, item := range items {
		hash := bulkItemHash(item)
		cluster, ok := clustered[hash]
		if ok {
			lastChunkIdx := len(cluster.chunkedResourceIDs) - 1
			lastChunk := cluster.chunkedResourceIDs[lastChunkIdx]
			if len(lastChunk) == int(MaxBulkCheckDispatchChunkSize) {
				cluster.chunkedResourceIDs = append(cluster.chunkedResourceIDs, []string{item.Resource.ObjectId})
				clustered[hash] = cluster
			} else {
				cluster.chunkedResourceIDs[lastChunkIdx] = append(lastChunk, item.Resource.ObjectId)
			}
		} else {
			caveatContext, err := GetCaveatContext(ctx, item.Context, params.maxCaveatContextSize)
			if err != nil {
				return nil, err
			}
			clustered[hash] = clusteredCheckParameters{
				params: computed.CheckParameters{
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
				},
				chunkedResourceIDs: [][]string{{item.Resource.ObjectId}},
			}
		}
	}

	return maps.Values(clustered), nil
}

func bulkItemHash(item *v1.BulkCheckPermissionRequestItem) string {
	contextHash := caveats.StableContextStringForHashing(item.Context)
	key := item.Resource.ObjectType + item.Permission + item.Subject.Object.ObjectType + item.Subject.Object.ObjectId + normalizeSubjectRelation(item.Subject) + contextHash
	return key
}
