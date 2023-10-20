package v1

import (
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/tuple"
)

func computeBulkCheckPermissionItemHashWithoutResourceID(req *v1.BulkCheckPermissionRequestItem) (string, error) {
	return shared.ComputeCallHash("v1.bulkcheckpermissionrequestitem", nil, map[string]any{
		"resource-type":    req.Resource.ObjectType,
		"permission":       req.Permission,
		"subject-type":     req.Subject.Object.ObjectType,
		"subject-id":       req.Subject.Object.ObjectId,
		"subject-relation": req.Subject.OptionalRelation,
		"context":          req.Context,
	})
}

func computeBulkCheckPermissionItemHash(req *v1.BulkCheckPermissionRequestItem) (string, error) {
	return shared.ComputeCallHash("v1.bulkcheckpermissionrequestitem", nil, map[string]any{
		"resource-type":    req.Resource.ObjectType,
		"resource-id":      req.Resource.ObjectId,
		"permission":       req.Permission,
		"subject-type":     req.Subject.Object.ObjectType,
		"subject-id":       req.Subject.Object.ObjectId,
		"subject-relation": req.Subject.OptionalRelation,
		"context":          req.Context,
	})
}

func computeReadRelationshipsRequestHash(req *v1.ReadRelationshipsRequest) (string, error) {
	osf := req.RelationshipFilter.OptionalSubjectFilter
	if osf == nil {
		osf = &v1.SubjectFilter{}
	}

	srf := "(none)"
	if osf.OptionalRelation != nil {
		srf = osf.OptionalRelation.Relation
	}

	return shared.ComputeCallHash("v1.readrelationships", req.Consistency, map[string]any{
		"filter-resource-type": req.RelationshipFilter.ResourceType,
		"filter-relation":      req.RelationshipFilter.OptionalRelation,
		"filter-resource-id":   req.RelationshipFilter.OptionalResourceId,
		"subject-type":         osf.SubjectType,
		"subject-relation":     srf,
		"subject-resource-id":  osf.OptionalSubjectId,
		"limit":                req.OptionalLimit,
	})
}

func computeLRRequestHash(req *v1.LookupResourcesRequest) (string, error) {
	return shared.ComputeCallHash("v1.lookupresources", req.Consistency, map[string]any{
		"resource-type": req.ResourceObjectType,
		"permission":    req.Permission,
		"subject":       tuple.StringSubjectRef(req.Subject),
		"limit":         req.OptionalLimit,
		"context":       req.Context,
	})
}
