package computed

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/samber/lo"

	cexpr "github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/taskrunner"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// DebugOption defines the various debug level options for Checks.
type DebugOption int

const (
	// NoDebugging indicates that debug information should be retained
	// while performing the Check.
	NoDebugging DebugOption = 0

	// BasicDebuggingEnabled indicates that basic debug information, such
	// as which steps were taken, should be retained while performing the
	// Check and returned to the caller.
	//
	// NOTE: This has a minor performance impact.
	BasicDebuggingEnabled DebugOption = 1

	// TraceDebuggingEnabled indicates that the Check is being issued for
	// tracing the exact calls made for debugging, which means that not only
	// should debug information be recorded and returned, but that optimizations
	// such as batching should be disabled.
	//
	// WARNING: This has a fairly significant performance impact and should only
	// be used in tooling!
	TraceDebuggingEnabled DebugOption = 2
)

// CheckParameters are the parameters for the ComputeCheck call. *All* are required.
type CheckParameters struct {
	ResourceType  *core.RelationReference
	Subject       *core.ObjectAndRelation
	CaveatContext map[string]any
	AtRevision    datastore.Revision
	MaximumDepth  uint32
	DebugOption   DebugOption
	CheckHints    []*v1.CheckHint
}

// ComputeCheck computes a check result for the given resource and subject, computing any
// caveat expressions found.
func ComputeCheck(
	ctx context.Context,
	d dispatch.Check,
	params CheckParameters,
	resourceID string,
	dispatchChunkSize uint16,
	concurrencyLimit uint16,
) (*v1.ResourceCheckResult, *v1.ResponseMeta, error) {
	resultsMap, meta, err := computeCheck(ctx, d, params, []string{resourceID}, dispatchChunkSize, concurrencyLimit)
	if err != nil {
		return nil, meta, err
	}
	return resultsMap[resourceID], meta, err
}

// ComputeBulkCheck computes a check result for the given resources and subject, computing any
// caveat expressions found.
func ComputeBulkCheck(
	ctx context.Context,
	d dispatch.Check,
	params CheckParameters,
	resourceIDs []string,
	dispatchChunkSize uint16,
	concurrencyLimit uint16,
) (map[string]*v1.ResourceCheckResult, *v1.ResponseMeta, error) {
	return computeCheck(ctx, d, params, resourceIDs, dispatchChunkSize, concurrencyLimit)
}

func computeCheck(ctx context.Context,
	d dispatch.Check,
	params CheckParameters,
	resourceIDs []string,
	dispatchChunkSize uint16,
	concurrencyLimit uint16,
) (map[string]*v1.ResourceCheckResult, *v1.ResponseMeta, error) {
	debugging := v1.DispatchCheckRequest_NO_DEBUG
	if params.DebugOption == BasicDebuggingEnabled {
		debugging = v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING
		if len(resourceIDs) > 1 {
			return nil, nil, spiceerrors.MustBugf("debugging can only be enabled for a single resource ID")
		}
	} else if params.DebugOption == TraceDebuggingEnabled {
		debugging = v1.DispatchCheckRequest_ENABLE_TRACE_DEBUGGING
		if len(resourceIDs) > 1 {
			return nil, nil, spiceerrors.MustBugf("debugging can only be enabled for a single resource ID")
		}
	}

	setting := v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS
	if len(resourceIDs) == 1 {
		setting = v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT
	}

	// Ensure that the number of resources IDs given to each dispatch call is not in excess of the maximum.
	var mapLock sync.Mutex
	results := make(map[string]*v1.ResourceCheckResult, len(resourceIDs))

	bf, err := v1.NewTraversalBloomFilter(uint(params.MaximumDepth))
	if err != nil {
		return nil, nil, spiceerrors.MustBugf("failed to create new traversal bloom filter")
	}

	taskNumber := len(resourceIDs) / int(dispatchChunkSize)
	if len(resourceIDs)%int(dispatchChunkSize) != 0 {
		taskNumber++
	}
	if taskNumber > 1 {
		var totalDispatchCount atomic.Uint32
		var depthMutex sync.Mutex
		allDepths := make([]uint32, 0, (len(resourceIDs)/int(dispatchChunkSize))+1)
		var totalCachedDispatchCount atomic.Uint32
		tr := taskrunner.NewPreloadedTaskRunner(ctx, concurrencyLimit, taskNumber)
		slicez.ForEachChunk(resourceIDs, dispatchChunkSize, func(resourceIDsToCheck []string) {
			tr.Add(func(ctx context.Context) error {
				checkResult, err := d.DispatchCheck(ctx, &v1.DispatchCheckRequest{
					ResourceRelation: params.ResourceType,
					ResourceIds:      resourceIDsToCheck,
					ResultsSetting:   setting,
					Subject:          params.Subject,
					Metadata: &v1.ResolverMeta{
						AtRevision:     params.AtRevision.String(),
						DepthRemaining: params.MaximumDepth,
						TraversalBloom: bf,
					},
					Debug:      debugging,
					CheckHints: params.CheckHints,
				})

				totalDispatchCount.Add(checkResult.Metadata.DispatchCount)
				totalCachedDispatchCount.Add(checkResult.Metadata.CachedDispatchCount)
				depthMutex.Lock()
				allDepths = append(allDepths, checkResult.Metadata.DepthRequired)
				depthMutex.Unlock()
				if err != nil {
					return err
				}

				for _, resourceID := range resourceIDsToCheck {
					computed, err := computeCaveatedCheckResult(ctx, params, resourceID, checkResult)
					if err != nil {
						return err
					}

					mapLock.Lock()
					results[resourceID] = computed
					mapLock.Unlock()
				}

				return nil
			})
		})

		// Run the checks in parallel.
		err = tr.StartAndWait()
		metadata := &v1.ResponseMeta{
			DispatchCount:       totalDispatchCount.Load(),
			DepthRequired:       lo.Max(allDepths),
			CachedDispatchCount: totalCachedDispatchCount.Load(),
		}
		return results, metadata, err
	}

	checkResult, err := d.DispatchCheck(ctx, &v1.DispatchCheckRequest{
		ResourceRelation: params.ResourceType,
		ResourceIds:      resourceIDs,
		ResultsSetting:   setting,
		Subject:          params.Subject,
		Metadata: &v1.ResolverMeta{
			AtRevision:     params.AtRevision.String(),
			DepthRemaining: params.MaximumDepth,
			TraversalBloom: bf,
		},
		Debug:      debugging,
		CheckHints: params.CheckHints,
	})
	metadata := &v1.ResponseMeta{
		DispatchCount:       checkResult.Metadata.DispatchCount,
		DepthRequired:       checkResult.Metadata.DepthRequired,
		CachedDispatchCount: checkResult.Metadata.CachedDispatchCount,
	}
	if err != nil {
		return nil, metadata, err
	}

	for _, resourceID := range resourceIDs {
		computed, err := computeCaveatedCheckResult(ctx, params, resourceID, checkResult)
		if err != nil {
			return nil, metadata, err
		}
		results[resourceID] = computed
	}

	return results, metadata, nil
}

func computeCaveatedCheckResult(ctx context.Context, params CheckParameters, resourceID string, checkResult *v1.DispatchCheckResponse) (*v1.ResourceCheckResult, error) {
	result, ok := checkResult.ResultsByResourceId[resourceID]
	if !ok {
		return &v1.ResourceCheckResult{
			Membership: v1.ResourceCheckResult_NOT_MEMBER,
		}, nil
	}

	if result.Membership == v1.ResourceCheckResult_MEMBER {
		return result, nil
	}

	ds := datastoremw.MustFromContext(ctx)
	reader := ds.SnapshotReader(params.AtRevision)

	caveatResult, err := cexpr.RunCaveatExpression(ctx, result.Expression, params.CaveatContext, reader, cexpr.RunCaveatExpressionNoDebugging)
	if err != nil {
		return nil, err
	}

	if caveatResult.IsPartial() {
		missingFields, _ := caveatResult.MissingVarNames()
		return &v1.ResourceCheckResult{
			Membership:        v1.ResourceCheckResult_CAVEATED_MEMBER,
			MissingExprFields: missingFields,
		}, nil
	}

	if caveatResult.Value() {
		return &v1.ResourceCheckResult{
			Membership: v1.ResourceCheckResult_MEMBER,
		}, nil
	}

	return &v1.ResourceCheckResult{
		Membership: v1.ResourceCheckResult_NOT_MEMBER,
	}, nil
}
