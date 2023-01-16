package development

import (
	"github.com/authzed/spicedb/internal/graph/computed"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// CheckResult is the result of a RunCheck operation.
type CheckResult struct {
	Permissionship      v1.ResourceCheckResult_Membership
	MissingCaveatFields []string
	DebugInfo           *v1.DebugInformation
}

// RunCheck performs a check against the data in the development context.
//
// Note that it is up to the caller to call DistinguishGraphError on the error
// if they want to distinguish between user errors and internal errors.
func RunCheck(devContext *DevContext, resource *core.ObjectAndRelation, subject *core.ObjectAndRelation, caveatContext map[string]any) (CheckResult, error) {
	ctx := devContext.Ctx
	cr, meta, err := computed.ComputeCheck(ctx, devContext.Dispatcher,
		computed.CheckParameters{
			ResourceType: &core.RelationReference{
				Namespace: resource.Namespace,
				Relation:  resource.Relation,
			},
			Subject:       subject,
			CaveatContext: caveatContext,
			AtRevision:    devContext.Revision,
			MaximumDepth:  maxDispatchDepth,
			DebugOption:   computed.TraceDebuggingEnabled,
		},
		resource.ObjectId,
	)
	if err != nil {
		return CheckResult{v1.ResourceCheckResult_NOT_MEMBER, nil, nil}, err
	}

	return CheckResult{cr.Membership, cr.MissingExprFields, meta.DebugInfo}, nil
}
