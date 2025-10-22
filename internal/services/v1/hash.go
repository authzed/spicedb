package v1

import (
	"strconv"

	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

func computeCheckBulkPermissionsItemHashWithoutResourceID(req *v1.CheckBulkPermissionsRequestItem) (string, error) {
	return computeCallHash("v1.checkbulkpermissionsrequestitem", nil, map[string]any{
		"resource-type":    req.GetResource().GetObjectType(),
		"permission":       req.GetPermission(),
		"subject-type":     req.GetSubject().GetObject().GetObjectType(),
		"subject-id":       req.GetSubject().GetObject().GetObjectId(),
		"subject-relation": req.GetSubject().GetOptionalRelation(),
		"context":          req.GetContext(),
	})
}

func computeCheckBulkPermissionsItemHash(req *v1.CheckBulkPermissionsRequestItem) (string, error) {
	return computeCallHash("v1.checkbulkpermissionsrequestitem", nil, map[string]any{
		"resource-type":    req.GetResource().GetObjectType(),
		"resource-id":      req.GetResource().GetObjectId(),
		"permission":       req.GetPermission(),
		"subject-type":     req.GetSubject().GetObject().GetObjectType(),
		"subject-id":       req.GetSubject().GetObject().GetObjectId(),
		"subject-relation": req.GetSubject().GetOptionalRelation(),
		"context":          req.GetContext(),
	})
}

func computeReadRelationshipsRequestHash(req *v1.ReadRelationshipsRequest) (string, error) {
	osf := req.GetRelationshipFilter().GetOptionalSubjectFilter()
	if osf == nil {
		osf = &v1.SubjectFilter{}
	}

	srf := "(none)"
	if osf.GetOptionalRelation() != nil {
		srf = osf.GetOptionalRelation().GetRelation()
	}

	return computeCallHash("v1.readrelationships", req.GetConsistency(), map[string]any{
		"filter-resource-type": req.GetRelationshipFilter().GetResourceType(),
		"filter-relation":      req.GetRelationshipFilter().GetOptionalRelation(),
		"filter-resource-id":   req.GetRelationshipFilter().GetOptionalResourceId(),
		"subject-type":         osf.GetSubjectType(),
		"subject-relation":     srf,
		"subject-resource-id":  osf.GetOptionalSubjectId(),
		"limit":                req.GetOptionalLimit(),
	})
}

func computeLRRequestHash(req *v1.LookupResourcesRequest) (string, error) {
	return computeCallHash("v1.lookupresources", req.GetConsistency(), map[string]any{
		"resource-type": req.GetResourceObjectType(),
		"permission":    req.GetPermission(),
		"subject":       tuple.V1StringSubjectRef(req.GetSubject()),
		"limit":         req.GetOptionalLimit(),
		"context":       req.GetContext(),
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
