package v1

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"golang.org/x/exp/maps"

	"github.com/authzed/spicedb/internal/graph/computed"
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

// clusterItems takes a slice of BulkCheckPermissionRequestItem and clusters them based
// on using the same permission, subject type, subject id, and caveat.
func clusterItems(ctx context.Context, params clusteringParameters, items []*v1.BulkCheckPermissionRequestItem, maxBatchSize uint16) ([]clusteredCheckParameters, error) {
	clustered := make(map[string]clusteredCheckParameters, len(items))
	for _, item := range items {
		hash, err := computeBulkCheckPermissionItemHashWithoutResourceID(item)
		if err != nil {
			return nil, err
		}

		cluster, ok := clustered[hash]
		if ok {
			lastChunkIdx := len(cluster.chunkedResourceIDs) - 1
			lastChunk := cluster.chunkedResourceIDs[lastChunkIdx]
			if len(lastChunk) == int(maxBatchSize) {
				resourceIDs := make([]string, 0, maxBatchSize)
				resourceIDs = append(resourceIDs, item.Resource.ObjectId)

				cluster.chunkedResourceIDs = append(cluster.chunkedResourceIDs, resourceIDs)
				clustered[hash] = cluster
			} else {
				cluster.chunkedResourceIDs[lastChunkIdx] = append(lastChunk, item.Resource.ObjectId)
			}
		} else {
			caveatContext, err := GetCaveatContext(ctx, item.Context, params.maxCaveatContextSize)
			if err != nil {
				return nil, err
			}

			resourceIDs := make([]string, 0, maxBatchSize)
			resourceIDs = append(resourceIDs, item.Resource.ObjectId)

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
				chunkedResourceIDs: [][]string{resourceIDs},
			}
		}
	}

	return maps.Values(clustered), nil
}
