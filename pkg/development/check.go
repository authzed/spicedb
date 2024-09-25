package development

import (
	v1api "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/graph/computed"
	v1 "github.com/authzed/spicedb/internal/services/v1"
	v1dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const defaultWasmDispatchChunkSize = 100

// CheckResult is the result of a RunCheck operation.
type CheckResult struct {
	Permissionship      v1dispatch.ResourceCheckResult_Membership
	MissingCaveatFields []string
	DispatchDebugInfo   *v1dispatch.DebugInformation
	V1DebugInfo         *v1api.DebugInformation
}

// RunCheck performs a check against the data in the development context.
//
// Note that it is up to the caller to call DistinguishGraphError on the error
// if they want to distinguish between user errors and internal errors.
func RunCheck(devContext *DevContext, resource tuple.ObjectAndRelation, subject tuple.ObjectAndRelation, caveatContext map[string]any) (CheckResult, error) {
	ctx := devContext.Ctx
	cr, meta, err := computed.ComputeCheck(ctx, devContext.Dispatcher,
		computed.CheckParameters{
			ResourceType:  resource.RelationReference(),
			Subject:       subject,
			CaveatContext: caveatContext,
			AtRevision:    devContext.Revision,
			MaximumDepth:  maxDispatchDepth,
			DebugOption:   computed.TraceDebuggingEnabled,
		},
		resource.ObjectID,
		defaultWasmDispatchChunkSize,
	)
	if err != nil {
		return CheckResult{v1dispatch.ResourceCheckResult_NOT_MEMBER, nil, nil, nil}, err
	}

	reader := devContext.Datastore.SnapshotReader(devContext.Revision)
	converted, err := v1.ConvertCheckDispatchDebugInformation(ctx, caveatContext, meta.DebugInfo, reader)
	if err != nil {
		return CheckResult{v1dispatch.ResourceCheckResult_NOT_MEMBER, nil, nil, nil}, err
	}

	return CheckResult{cr.Membership, cr.MissingExprFields, meta.DebugInfo, converted}, nil
}
