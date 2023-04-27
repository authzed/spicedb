package graph

import (
	"errors"

	"github.com/authzed/spicedb/internal/graph/computed"

	"github.com/authzed/spicedb/pkg/util"

	"github.com/authzed/spicedb/pkg/spiceerrors"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// NewCursoredLookupResources creates and instance of CursoredLookupResources.
func NewCursoredLookupResources(c dispatch.Check, r dispatch.ReachableResources, concurrencyLimit uint16) *CursoredLookupResources {
	return &CursoredLookupResources{c, r, concurrencyLimit}
}

// CursoredLookupResources exposes a method to perform LookupResources requests, and delegates subproblems to the
// provided dispatch.Lookup instance.
type CursoredLookupResources struct {
	c                dispatch.Check
	r                dispatch.ReachableResources
	concurrencyLimit uint16
}

// ValidatedLookupResourcesRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedLookupResourcesRequest struct {
	*v1.DispatchLookupResourcesRequest
	Revision datastore.Revision
}

func (cl *CursoredLookupResources) LookupResources(
	req ValidatedLookupResourcesRequest,
	stream dispatch.LookupResourcesStream,
) error {
	if req.Subject.ObjectId == tuple.PublicWildcard {
		return NewErrInvalidArgument(errors.New("cannot perform lookup resources on wildcard"))
	}

	rrStream := dispatch.NewHandlingDispatchStream(stream.Context(), func(result *v1.DispatchReachableResourcesResponse) error {
		toCheck := util.NewSet[string]()
		for _, reachableResource := range result.Resources {
			if reachableResource.ResultStatus == v1.ReachableResource_REQUIRES_CHECK {
				toCheck.Add(reachableResource.ResourceId)
			}
		}

		var checkResults map[string]*v1.ResourceCheckResult
		checkMeta := emptyMetadata
		if !toCheck.IsEmpty() {
			results, resultsMeta, err := computed.ComputeBulkCheck(
				stream.Context(),
				cl.c,
				computed.CheckParameters{
					ResourceType:  req.ObjectRelation,
					Subject:       req.Subject,
					CaveatContext: req.Context.AsMap(),
					AtRevision:    req.Revision,
					MaximumDepth:  req.Metadata.DepthRemaining,
					DebugOption:   computed.NoDebugging,
				},
				toCheck.AsSlice(),
			)
			if err != nil {
				return err
			}
			checkResults = results
			checkMeta = addCallToResponseMetadata(resultsMeta)
		}

		for _, reachableResource := range result.Resources {
			switch reachableResource.ResultStatus {
			case v1.ReachableResource_HAS_PERMISSION:
				stream.Publish(&v1.DispatchLookupResourcesResponse{
					ResolvedResources: []*v1.ResolvedResource{
						{
							ResourceId:     reachableResource.ResourceId,
							Permissionship: v1.ResolvedResource_HAS_PERMISSION,
						},
					},
					Metadata: addCallToResponseMetadata(result.Metadata),
				})
				continue

			case v1.ReachableResource_REQUIRES_CHECK:
				checkResult, ok := checkResults[reachableResource.ResourceId]
				if !ok {
					return spiceerrors.MustBugf("missing checked result")
				}
				switch checkResult.Membership {
				case v1.ResourceCheckResult_MEMBER:
					stream.Publish(&v1.DispatchLookupResourcesResponse{
						ResolvedResources: []*v1.ResolvedResource{
							{
								ResourceId:     reachableResource.ResourceId,
								Permissionship: v1.ResolvedResource_HAS_PERMISSION,
							},
						},
						Metadata: combineResponseMetadata(result.Metadata, checkMeta),
					})
					continue

				case v1.ResourceCheckResult_CAVEATED_MEMBER:
					stream.Publish(&v1.DispatchLookupResourcesResponse{
						ResolvedResources: []*v1.ResolvedResource{
							{
								ResourceId:             reachableResource.ResourceId,
								Permissionship:         v1.ResolvedResource_CONDITIONALLY_HAS_PERMISSION,
								MissingRequiredContext: checkResult.MissingExprFields,
							},
						},
						Metadata: combineResponseMetadata(result.Metadata, checkMeta),
					})
					continue

				case v1.ResourceCheckResult_NOT_MEMBER:
					// Skip.
					continue

				default:
					return spiceerrors.MustBugf("unknown check result status for reachable resources")
				}

			default:
				return spiceerrors.MustBugf("unknown result status for reachable resources")
			}
		}

		return nil
	})

	return cl.r.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
		ResourceRelation: req.ObjectRelation,
		SubjectRelation: &core.RelationReference{
			Namespace: req.Subject.Namespace,
			Relation:  req.Subject.Relation,
		},
		SubjectIds: []string{req.Subject.ObjectId},
		Metadata:   req.Metadata,
	}, rrStream)
}
