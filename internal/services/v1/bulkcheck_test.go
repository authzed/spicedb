package v1

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/testfixtures"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// debugInfoDispatcher is a dispatcher that returns a successful check result whose
// metadata carries debug information with a nil Check.Request. This mirrors the
// shape produced by combineResponseMetadata for combined trace nodes, which can
// reach a non-tracing CheckBulkPermissions caller when an identical tracing-enabled
// request shares its dispatch via singleflight.
type debugInfoDispatcher struct {
	dispatch.Dispatcher
}

func (d debugInfoDispatcher) DispatchCheck(_ context.Context, req *dispatchv1.DispatchCheckRequest) (*dispatchv1.DispatchCheckResponse, error) {
	return &dispatchv1.DispatchCheckResponse{
		ResultsByResourceId: memberResults(req.ResourceIds),
		Metadata: &dispatchv1.ResponseMeta{
			DispatchCount: 1,
			DebugInfo: &dispatchv1.DebugInformation{
				Check: &dispatchv1.CheckDebugTrace{
					SubProblems: []*dispatchv1.CheckDebugTrace{{IsCachedResult: true}},
				},
			},
		},
	}, nil
}

// matchingDebugInfoDispatcher returns a successful check result whose debug
// information carries a Check.Request matching the dispatched resource IDs. This
// exercises the normal tracing path where CheckBulkPermissions synthesizes a
// debug trace for each resource.
type matchingDebugInfoDispatcher struct {
	dispatch.Dispatcher
}

func (d matchingDebugInfoDispatcher) DispatchCheck(_ context.Context, req *dispatchv1.DispatchCheckRequest) (*dispatchv1.DispatchCheckResponse, error) {
	return &dispatchv1.DispatchCheckResponse{
		ResultsByResourceId: memberResults(req.ResourceIds),
		Metadata: &dispatchv1.ResponseMeta{
			DispatchCount: 1,
			DebugInfo: &dispatchv1.DebugInformation{
				Check: &dispatchv1.CheckDebugTrace{
					Request: &dispatchv1.DispatchCheckRequest{
						ResourceRelation: req.ResourceRelation,
						ResourceIds:      req.ResourceIds,
						Subject:          req.Subject,
						ResultsSetting:   req.ResultsSetting,
						Debug:            req.Debug,
					},
					Results: memberResults(req.ResourceIds),
				},
			},
		},
	}, nil
}

func memberResults(resourceIDs []string) map[string]*dispatchv1.ResourceCheckResult {
	results := make(map[string]*dispatchv1.ResourceCheckResult, len(resourceIDs))
	for _, resourceID := range resourceIDs {
		results[resourceID] = &dispatchv1.ResourceCheckResult{
			Membership: dispatchv1.ResourceCheckResult_MEMBER,
		}
	}
	return results
}

func runBulkCheck(t *testing.T, d dispatch.Dispatcher, withTracing bool) *v1.CheckBulkPermissionsResponse {
	t.Helper()
	require := require.New(t)

	uninitialized, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(t, uninitialized)
	dl := datalayer.NewDataLayer(ds)

	ctx := datalayer.ContextWithDataLayer(consistency.ContextWithHandle(t.Context()), dl)

	req := &v1.CheckBulkPermissionsRequest{
		Items: []*v1.CheckBulkPermissionsRequestItem{
			{
				Resource: &v1.ObjectReference{
					ObjectType: "document",
					ObjectId:   "masterplan",
				},
				Permission: "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "eng_lead",
					},
				},
			},
		},
		WithTracing: withTracing,
	}

	require.NoError(consistency.AddRevisionToContext(ctx, req, dl, "", consistency.TreatMismatchingTokensAsError))

	bc := &bulkChecker{
		maxAPIDepth:          50,
		maxCaveatContextSize: 4096,
		maxConcurrency:       1,
		caveatTypeSet:        caveattypes.Default.TypeSet,
		dispatch:             d,
		dispatchChunkSize:    100,
	}

	resp, err := bc.checkBulkPermissions(ctx, req, NewMetrics(nil))
	require.NoError(err)
	return resp
}

// TestCheckBulkPermissionsWithNilDebugRequest ensures that a debug information entry
// with a nil Check.Request does not panic CheckBulkPermissions when tracing was not
// requested. See https://github.com/authzed/spicedb/issues/3159.
func TestCheckBulkPermissionsWithNilDebugRequest(t *testing.T) {
	require := require.New(t)

	resp := runBulkCheck(t, debugInfoDispatcher{}, false)
	require.Len(resp.Pairs, 1)

	item := resp.Pairs[0].GetItem()
	require.NotNil(item)
	require.Equal(v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, item.Permissionship)
	require.Nil(item.DebugTrace)
}

// TestCheckBulkPermissionsWithMatchingDebugRequest ensures that a debug information
// entry whose Check.Request matches the resource is surfaced as a debug trace on the
// result, exercising the positive branch of the resource-ID matching loop.
func TestCheckBulkPermissionsWithMatchingDebugRequest(t *testing.T) {
	require := require.New(t)

	resp := runBulkCheck(t, matchingDebugInfoDispatcher{}, true)
	require.Len(resp.Pairs, 1)

	item := resp.Pairs[0].GetItem()
	require.NotNil(item)
	require.Equal(v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, item.Permissionship)
	require.NotNil(item.DebugTrace)
	require.NotNil(item.DebugTrace.Check)
	require.Equal("masterplan", item.DebugTrace.Check.Resource.GetObjectId())
	require.Equal("view", item.DebugTrace.Check.Permission)
}
