package v1

import (
	"context"
	"slices"
	"sync"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/graph/computed"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/taskrunner"
	"github.com/authzed/spicedb/pkg/genutil"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// bulkChecker contains the logic to allow ExperimentalService/BulkCheckPermission and
// PermissionsService/CheckBulkPermissions to share the same implementation.
type bulkChecker struct {
	maxAPIDepth          uint32
	maxCaveatContextSize int
	maxConcurrency       uint16

	dispatch          dispatch.Dispatcher
	dispatchChunkSize uint16
}

const maxBulkCheckCount = 10000

func (bc *bulkChecker) checkBulkPermissions(ctx context.Context, req *v1.CheckBulkPermissionsRequest) (*v1.CheckBulkPermissionsResponse, error) {
	atRevision, checkedAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	if len(req.Items) > maxBulkCheckCount {
		return nil, NewExceedsMaximumChecksErr(uint64(len(req.Items)), maxBulkCheckCount)
	}

	// Compute a hash for each requested item and record its index(es) for the items, to be used for sorting of results.
	itemCount, err := genutil.EnsureUInt32(len(req.Items))
	if err != nil {
		return nil, err
	}

	itemIndexByHash := mapz.NewMultiMapWithCap[string, int](itemCount)
	for index, item := range req.Items {
		itemHash, err := computeCheckBulkPermissionsItemHash(item)
		if err != nil {
			return nil, err
		}

		itemIndexByHash.Add(itemHash, index)
	}

	// Identify checks with same permission+subject over different resources and group them. This is doable because
	// the dispatching system already internally supports this kind of batching for performance.
	groupedItems, err := groupItems(ctx, groupingParameters{
		atRevision:           atRevision,
		maxCaveatContextSize: bc.maxCaveatContextSize,
		maximumAPIDepth:      bc.maxAPIDepth,
		withTracing:          req.WithTracing,
	}, req.Items)
	if err != nil {
		return nil, err
	}

	bulkResponseMutex := sync.Mutex{}

	spiceerrors.DebugAssert(func() bool {
		return bc.maxConcurrency > 0
	}, "max concurrency must be greater than 0 in bulk check")

	tr := taskrunner.NewPreloadedTaskRunner(ctx, bc.maxConcurrency, len(groupedItems))

	respMetadata := &dispatchv1.ResponseMeta{
		DispatchCount:       1,
		CachedDispatchCount: 0,
		DepthRequired:       1,
		DebugInfo:           nil,
	}
	usagemetrics.SetInContext(ctx, respMetadata)

	orderedPairs := make([]*v1.CheckBulkPermissionsPair, len(req.Items))

	addPair := func(pair *v1.CheckBulkPermissionsPair) error {
		pairItemHash, err := computeCheckBulkPermissionsItemHash(pair.Request)
		if err != nil {
			return err
		}

		found, ok := itemIndexByHash.Get(pairItemHash)
		if !ok {
			return spiceerrors.MustBugf("missing expected item hash")
		}

		for _, index := range found {
			orderedPairs[index] = pair
		}

		return nil
	}

	appendResultsForError := func(params *computed.CheckParameters, resourceIDs []string, err error) error {
		rewritten := shared.RewriteError(ctx, err, &shared.ConfigForErrors{
			MaximumAPIDepth: bc.maxAPIDepth,
		})
		statusResp, ok := status.FromError(rewritten)
		if !ok {
			// If error is not a gRPC Status, fail the entire bulk check request.
			return err
		}

		bulkResponseMutex.Lock()
		defer bulkResponseMutex.Unlock()

		for _, resourceID := range resourceIDs {
			reqItem, err := requestItemFromResourceAndParameters(params, resourceID)
			if err != nil {
				return err
			}

			if err := addPair(&v1.CheckBulkPermissionsPair{
				Request: reqItem,
				Response: &v1.CheckBulkPermissionsPair_Error{
					Error: statusResp.Proto(),
				},
			}); err != nil {
				return err
			}
		}

		return nil
	}

	appendResultsForCheck := func(
		params *computed.CheckParameters,
		resourceIDs []string,
		metadata *dispatchv1.ResponseMeta,
		debugInfos []*dispatchv1.DebugInformation,
		results map[string]*dispatchv1.ResourceCheckResult,
	) error {
		bulkResponseMutex.Lock()
		defer bulkResponseMutex.Unlock()

		ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

		schemaText := ""
		if len(debugInfos) > 0 {
			schema, err := getFullSchema(ctx, ds)
			if err != nil {
				return err
			}
			schemaText = schema
		}

		for _, resourceID := range resourceIDs {
			var debugTrace *v1.DebugInformation
			if len(debugInfos) > 0 {
				// Find the debug info that matches the resource ID.
				var debugInfo *dispatchv1.DebugInformation
				for _, di := range debugInfos {
					if slices.Contains(di.Check.Request.ResourceIds, resourceID) {
						debugInfo = di
						break
					}
				}

				if debugInfo != nil {
					// Synthesize a new debug information with a trace "wrapping" the (potentially batched)
					// trace.
					localResults := make(map[string]*dispatchv1.ResourceCheckResult, 1)
					if result, ok := results[resourceID]; ok {
						localResults[resourceID] = result
					}
					wrappedDebugInfo := &dispatchv1.DebugInformation{
						Check: &dispatchv1.CheckDebugTrace{
							Request: &dispatchv1.DispatchCheckRequest{
								ResourceRelation: debugInfo.Check.Request.ResourceRelation,
								ResourceIds:      []string{resourceID},
								Subject:          debugInfo.Check.Request.Subject,
								ResultsSetting:   debugInfo.Check.Request.ResultsSetting,
								Debug:            debugInfo.Check.Request.Debug,
							},
							ResourceRelationType: debugInfo.Check.ResourceRelationType,
							IsCachedResult:       false,
							SubProblems: []*dispatchv1.CheckDebugTrace{
								debugInfo.Check,
							},
							Results:  localResults,
							Duration: durationpb.New(time.Duration(0)),
							TraceId:  graph.NewTraceID(),
							SourceId: debugInfo.Check.SourceId,
						},
					}

					// Convert to debug information.
					dt, err := convertCheckDispatchDebugInformationWithSchema(ctx, params.CaveatContext, wrappedDebugInfo, ds, schemaText)
					if err != nil {
						return err
					}
					debugTrace = dt
				}
			}

			reqItem, err := requestItemFromResourceAndParameters(params, resourceID)
			if err != nil {
				return err
			}

			if err := addPair(&v1.CheckBulkPermissionsPair{
				Request:  reqItem,
				Response: pairItemFromCheckResult(results[resourceID], debugTrace),
			}); err != nil {
				return err
			}
		}

		respMetadata.DispatchCount += metadata.DispatchCount
		respMetadata.CachedDispatchCount += metadata.CachedDispatchCount
		return nil
	}

	for _, group := range groupedItems {
		group := group

		slicez.ForEachChunk(group.resourceIDs, bc.dispatchChunkSize, func(resourceIDs []string) {
			tr.Add(func(ctx context.Context) error {
				ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

				// Ensure the check namespaces and relations are valid.
				err := namespace.CheckNamespaceAndRelations(ctx,
					[]namespace.TypeAndRelationToCheck{
						{
							NamespaceName: group.params.ResourceType.ObjectType,
							RelationName:  group.params.ResourceType.Relation,
							AllowEllipsis: false,
						},
						{
							NamespaceName: group.params.Subject.ObjectType,
							RelationName:  stringz.DefaultEmpty(group.params.Subject.Relation, graph.Ellipsis),
							AllowEllipsis: true,
						},
					}, ds)
				if err != nil {
					return appendResultsForError(group.params, resourceIDs, err)
				}

				// Call bulk check to compute the check result(s) for the resource ID(s).
				rcr, metadata, debugInfos, err := computed.ComputeBulkCheck(ctx, bc.dispatch, *group.params, resourceIDs, bc.dispatchChunkSize)
				if err != nil {
					return appendResultsForError(group.params, resourceIDs, err)
				}

				return appendResultsForCheck(group.params, resourceIDs, metadata, debugInfos, rcr)
			})
		})
	}

	// Run the checks in parallel.
	if err := tr.StartAndWait(); err != nil {
		return nil, err
	}

	return &v1.CheckBulkPermissionsResponse{CheckedAt: checkedAt, Pairs: orderedPairs}, nil
}

func toCheckBulkPermissionsRequest(req *v1.BulkCheckPermissionRequest) *v1.CheckBulkPermissionsRequest {
	items := make([]*v1.CheckBulkPermissionsRequestItem, len(req.Items))
	for i, item := range req.Items {
		items[i] = &v1.CheckBulkPermissionsRequestItem{
			Resource:   item.Resource,
			Permission: item.Permission,
			Subject:    item.Subject,
			Context:    item.Context,
		}
	}

	return &v1.CheckBulkPermissionsRequest{Items: items}
}

func toBulkCheckPermissionResponse(resp *v1.CheckBulkPermissionsResponse) *v1.BulkCheckPermissionResponse {
	pairs := make([]*v1.BulkCheckPermissionPair, len(resp.Pairs))
	for i, pair := range resp.Pairs {
		pairs[i] = &v1.BulkCheckPermissionPair{}
		pairs[i].Request = &v1.BulkCheckPermissionRequestItem{
			Resource:   pair.Request.Resource,
			Permission: pair.Request.Permission,
			Subject:    pair.Request.Subject,
			Context:    pair.Request.Context,
		}

		switch t := pair.Response.(type) {
		case *v1.CheckBulkPermissionsPair_Item:
			pairs[i].Response = &v1.BulkCheckPermissionPair_Item{
				Item: &v1.BulkCheckPermissionResponseItem{
					Permissionship:    t.Item.Permissionship,
					PartialCaveatInfo: t.Item.PartialCaveatInfo,
				},
			}
		case *v1.CheckBulkPermissionsPair_Error:
			pairs[i].Response = &v1.BulkCheckPermissionPair_Error{
				Error: t.Error,
			}
		default:
			panic("unknown CheckBulkPermissionResponse pair response type")
		}
	}

	return &v1.BulkCheckPermissionResponse{
		CheckedAt: resp.CheckedAt,
		Pairs:     pairs,
	}
}
