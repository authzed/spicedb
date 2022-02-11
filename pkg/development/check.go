package development

import (
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// RunCheck performs a check against the data in the development context.
//
// Note that it is up to the caller to call DistinguishGraphError on the error
// if they want to distinguish between user errors and internal errors.
func RunCheck(devContext *DevContext, resource *v0.ObjectAndRelation, subject *v0.ObjectAndRelation) (v1.DispatchCheckResponse_Membership, error) {
	ctx := devContext.Ctx
	cr, err := devContext.Dispatcher.DispatchCheck(ctx, &v1.DispatchCheckRequest{
		ObjectAndRelation: resource,
		Subject:           subject,
		Metadata: &v1.ResolverMeta{
			AtRevision:     devContext.Revision.String(),
			DepthRemaining: maxDispatchDepth,
		},
	})
	if err != nil {
		return v1.DispatchCheckResponse_NOT_MEMBER, err
	}
	return cr.Membership, nil
}
