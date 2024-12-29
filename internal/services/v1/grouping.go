package v1

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

type groupedCheckParameters struct {
	params      *computed.CheckParameters
	resourceIDs []string
}

type groupingParameters struct {
	atRevision           datastore.Revision
	maximumAPIDepth      uint32
	maxCaveatContextSize int
	withTracing          bool
}

// groupItems takes a slice of CheckBulkPermissionsRequestItem and groups them based
// on using the same permission, subject type, subject id, and caveat.
func groupItems(ctx context.Context, params groupingParameters, items []*v1.CheckBulkPermissionsRequestItem) (map[string]*groupedCheckParameters, error) {
	res := make(map[string]*groupedCheckParameters)

	for _, item := range items {
		hash, err := computeCheckBulkPermissionsItemHashWithoutResourceID(item)
		if err != nil {
			return nil, err
		}

		if _, ok := res[hash]; !ok {
			caveatContext, err := GetCaveatContext(ctx, item.Context, params.maxCaveatContextSize)
			if err != nil {
				return nil, err
			}

			res[hash] = &groupedCheckParameters{
				params:      checkParametersFromCheckBulkPermissionsRequestItem(item, params, caveatContext),
				resourceIDs: []string{item.Resource.ObjectId},
			}
		} else {
			res[hash].resourceIDs = append(res[hash].resourceIDs, item.Resource.ObjectId)
		}
	}

	return res, nil
}

func checkParametersFromCheckBulkPermissionsRequestItem(
	bc *v1.CheckBulkPermissionsRequestItem,
	params groupingParameters,
	caveatContext map[string]any,
) *computed.CheckParameters {
	debugOption := computed.NoDebugging
	if params.withTracing {
		debugOption = computed.BasicDebuggingEnabled
	}

	return &computed.CheckParameters{
		ResourceType:  tuple.RR(bc.Resource.ObjectType, bc.Permission),
		Subject:       tuple.ONR(bc.Subject.Object.ObjectType, bc.Subject.Object.ObjectId, normalizeSubjectRelation(bc.Subject)),
		CaveatContext: caveatContext,
		AtRevision:    params.atRevision,
		MaximumDepth:  params.maximumAPIDepth,
		DebugOption:   debugOption,
	}
}
