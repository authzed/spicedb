package development

import (
	"github.com/authzed/spicedb/internal/graph/computed"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// RunCheck performs a check against the data in the development context.
//
// Note that it is up to the caller to call DistinguishGraphError on the error
// if they want to distinguish between user errors and internal errors.
func RunCheck(devContext *DevContext, resource *core.ObjectAndRelation, subject *core.ObjectAndRelation) (v1.ResourceCheckResult_Membership, error) {
	ctx := devContext.Ctx
	cr, _, err := computed.ComputeCheck(ctx, devContext.Dispatcher,
		computed.CheckParameters{
			ResourceType: &core.RelationReference{
				Namespace: resource.Namespace,
				Relation:  resource.Relation,
			},
			Subject:            subject,
			CaveatContext:      nil, // TODO(jschorr): get from the dev context?
			AtRevision:         devContext.Revision,
			MaximumDepth:       maxDispatchDepth,
			IsDebuggingEnabled: false,
		},
		resource.ObjectId,
	)
	if err != nil {
		return v1.ResourceCheckResult_NOT_MEMBER, err
	}

	return cr.Membership, nil
}
