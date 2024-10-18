package v1

import (
	"strconv"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

func computeCheckBulkPermissionsItemHashWithoutResourceID(req *v1.CheckBulkPermissionsRequestItem) (string, error) {
	return computeCallHash("v1.checkbulkpermissionsrequestitem", nil, map[string]any{
		"resource-type":    req.Resource.ObjectType,
		"permission":       req.Permission,
		"subject-type":     req.Subject.Object.ObjectType,
		"subject-id":       req.Subject.Object.ObjectId,
		"subject-relation": req.Subject.OptionalRelation,
		"context":          req.Context,
	})
}

func computeCheckBulkPermissionsItemHash(req *v1.CheckBulkPermissionsRequestItem) (string, error) {
	return computeCallHash("v1.checkbulkpermissionsrequestitem", nil, map[string]any{
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

	return computeCallHash("v1.readrelationships", req.Consistency, map[string]any{
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
	return computeCallHash("v1.lookupresources", req.Consistency, map[string]any{
		"resource-type": req.ResourceObjectType,
		"permission":    req.Permission,
		"subject":       tuple.V1StringSubjectRef(req.Subject),
		"limit":         req.OptionalLimit,
		"context":       req.Context,
	})
}

func computeCallHash(apiName string, consistency *v1.Consistency, arguments map[string]any) (string, error) {
	stringArguments := make(map[string]string, len(arguments)+1)

	if consistency == nil {
		consistency = &v1.Consistency{
			Requirement: &v1.Consistency_MinimizeLatency{
				MinimizeLatency: true,
			},
		}
	}

	consistencyBytes, err := consistency.MarshalVT()
	if err != nil {
		return "", err
	}

	stringArguments["consistency"] = string(consistencyBytes)

	for argName, argValue := range arguments {
		if argName == "consistency" {
			return "", spiceerrors.MustBugf("cannot specify consistency in the arguments")
		}

		switch v := argValue.(type) {
		case string:
			stringArguments[argName] = v

		case int:
			stringArguments[argName] = strconv.Itoa(v)

		case uint32:
			stringArguments[argName] = strconv.Itoa(int(v))

		case *structpb.Struct:
			stringArguments[argName] = caveats.StableContextStringForHashing(v)

		default:
			return "", spiceerrors.MustBugf("unknown argument type in compute call hash")
		}
	}
	return computeAPICallHash(apiName, stringArguments)
}
