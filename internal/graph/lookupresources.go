package graph

import (
	"context"
	"errors"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
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

// reachableResourcesLimit is a limit set on the reachable resources calls to ensure caching
// stores smaller chunks.
const reachableResourcesLimit = 1000

func (cl *CursoredLookupResources) LookupResources(
	req ValidatedLookupResourcesRequest,
	stream dispatch.LookupResourcesStream,
) error {
	if req.Subject.ObjectId == tuple.PublicWildcard {
		return NewErrInvalidArgument(errors.New("cannot perform lookup resources on wildcard"))
	}

	withCancel, cancelReachable := context.WithCancel(stream.Context())
	defer cancelReachable()

	limits, ctx := newLimitTracker(withCancel, req.OptionalLimit)
	reachableResourcesCursor := req.OptionalCursor

	// Loop until the limit has been exhausted or no additional reachable resources are found (see below)
	for !limits.hasExhaustedLimit() {
		reachableResourcesFound := 0

		// Create a new handling stream that consumes the reachable resources results and publishes them
		// as found resources if they are properly checked.
		rrStream := dispatch.NewHandlingDispatchStream(withCancel, func(result *v1.DispatchReachableResourcesResponse) error {
			reachableResourcesCursor = result.AfterResponseCursor
			reachableResourcesFound++

			if limits.hasExhaustedLimit() {
				return nil
			}

			reachableResource := result.Resource
			metadata := addCallToResponseMetadata(result.Metadata)

			// See if a check is required.
			var checkResult *v1.ResourceCheckResult
			if reachableResource.ResultStatus == v1.ReachableResource_REQUIRES_CHECK {
				results, resultsMeta, err := computed.ComputeBulkCheck(
					ctx,
					cl.c,
					computed.CheckParameters{
						ResourceType:  req.ObjectRelation,
						Subject:       req.Subject,
						CaveatContext: req.Context.AsMap(),
						AtRevision:    req.Revision,
						MaximumDepth:  req.Metadata.DepthRemaining,
						DebugOption:   computed.NoDebugging,
					},
					[]string{reachableResource.ResourceId},
				)
				if err != nil {
					return err
				}

				checkResult = results[reachableResource.ResourceId]
				metadata = combineResponseMetadata(resultsMeta, metadata)
			}

			// Publish the resource, if applicable.
			var permissionship v1.ResolvedResource_Permissionship
			var missingFields []string

			switch {
			case reachableResource.ResultStatus == v1.ReachableResource_HAS_PERMISSION:
				permissionship = v1.ResolvedResource_HAS_PERMISSION

			case checkResult == nil || checkResult.Membership == v1.ResourceCheckResult_NOT_MEMBER:
				return nil

			case checkResult != nil && checkResult.Membership == v1.ResourceCheckResult_MEMBER:
				permissionship = v1.ResolvedResource_HAS_PERMISSION

			case checkResult != nil && checkResult.Membership == v1.ResourceCheckResult_CAVEATED_MEMBER:
				permissionship = v1.ResolvedResource_CONDITIONALLY_HAS_PERMISSION
				missingFields = checkResult.MissingExprFields

			default:
				return spiceerrors.MustBugf("unknown check result status for reachable resources")
			}

			okay, done := limits.prepareForPublishing()
			defer done()

			if !okay {
				return nil
			}

			return stream.Publish(&v1.DispatchLookupResourcesResponse{
				ResolvedResource: &v1.ResolvedResource{
					ResourceId:             reachableResource.ResourceId,
					Permissionship:         permissionship,
					MissingRequiredContext: missingFields,
				},
				Metadata:            metadata,
				AfterResponseCursor: result.AfterResponseCursor,
			})
		})

		err := cl.r.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
			ResourceRelation: req.ObjectRelation,
			SubjectRelation: &core.RelationReference{
				Namespace: req.Subject.Namespace,
				Relation:  req.Subject.Relation,
			},
			SubjectIds:     []string{req.Subject.ObjectId},
			Metadata:       req.Metadata,
			OptionalCursor: reachableResourcesCursor,
			OptionalLimit:  reachableResourcesLimit,
		}, rrStream)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			return err
		}

		if reachableResourcesFound < reachableResourcesLimit {
			return nil
		}
	}

	return nil
}
