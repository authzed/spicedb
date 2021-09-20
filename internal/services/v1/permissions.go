package v1

import (
	"context"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	dispatch "github.com/authzed/spicedb/internal/proto/dispatch/v1"
)

func (ps *permissionServer) CheckPermission(ctx context.Context, req *v1.CheckPermissionRequest) (*v1.CheckPermissionResponse, error) {
	atRevision, checkedAt := consistency.MustRevisionFromContext(ctx)

	err := ps.nsm.CheckNamespaceAndRelation(ctx, req.Resource.ObjectType, req.Permission, false)
	if err != nil {
		return nil, rewritePermissionsError(err)
	}

	err = ps.nsm.CheckNamespaceAndRelation(ctx,
		req.Subject.Object.ObjectType,
		normalizeSubjectRelation(req.Subject),
		true)
	if err != nil {
		return nil, rewritePermissionsError(err)
	}

	cr, err := ps.dispatch.DispatchCheck(ctx, &dispatch.DispatchCheckRequest{
		Metadata: &dispatch.ResolverMeta{
			AtRevision:     atRevision.String(),
			DepthRemaining: ps.defaultDepth,
		},
		ObjectAndRelation: &v0.ObjectAndRelation{
			Namespace: req.Resource.ObjectType,
			ObjectId:  req.Resource.ObjectId,
			Relation:  req.Permission,
		},
		Subject: &v0.ObjectAndRelation{
			Namespace: req.Subject.Object.ObjectType,
			ObjectId:  req.Subject.Object.ObjectId,
			Relation:  normalizeSubjectRelation(req.Subject),
		},
	})
	if err != nil {
		return nil, rewritePermissionsError(err)
	}

	var permissionship v1.CheckPermissionResponse_Permissionship
	switch cr.Membership {
	case dispatch.DispatchCheckResponse_MEMBER:
		permissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION
	case dispatch.DispatchCheckResponse_NOT_MEMBER:
		permissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION
	default:
		permissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED
	}

	return &v1.CheckPermissionResponse{
		CheckedAt:      checkedAt,
		Permissionship: permissionship,
	}, nil
}

func normalizeSubjectRelation(sub *v1.SubjectReference) string {
	if sub.OptionalRelation == "" {
		return graph.Ellipsis
	}
	return sub.OptionalRelation
}
