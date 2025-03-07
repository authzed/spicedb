package hints

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// CheckHintForComputedUserset creates a CheckHint for a relation and a subject.
func CheckHintForComputedUserset(resourceType string, resourceID string, relation string, subject tuple.ObjectAndRelation, result *v1.ResourceCheckResult) *v1.CheckHint {
	return &v1.CheckHint{
		Resource: &core.ObjectAndRelation{
			Namespace: resourceType,
			ObjectId:  resourceID,
			Relation:  relation,
		},
		Subject: subject.ToCoreONR(),
		Result:  result,
	}
}

// CheckHintForArrow creates a CheckHint for an arrow and a subject.
func CheckHintForArrow(resourceType string, resourceID string, tuplesetRelation string, computedUsersetRelation string, subject tuple.ObjectAndRelation, result *v1.ResourceCheckResult) *v1.CheckHint {
	return &v1.CheckHint{
		Resource: &core.ObjectAndRelation{
			Namespace: resourceType,
			ObjectId:  resourceID,
			Relation:  tuplesetRelation,
		},
		TtuComputedUsersetRelation: computedUsersetRelation,
		Subject:                    subject.ToCoreONR(),
		Result:                     result,
	}
}

// AsCheckHintForComputedUserset returns the resourceID if the checkHint is for the given relation and subject.
func AsCheckHintForComputedUserset(checkHint *v1.CheckHint, resourceType string, relationName string, subject tuple.ObjectAndRelation) (string, bool) {
	if checkHint.TtuComputedUsersetRelation != "" {
		return "", false
	}

	if checkHint.Resource.Namespace == resourceType && checkHint.Resource.Relation == relationName && checkHint.Subject.EqualVT(subject.ToCoreONR()) {
		return checkHint.Resource.ObjectId, true
	}

	return "", false
}

// AsCheckHintForArrow returns the resourceID if the checkHint is for the given arrow and subject.
func AsCheckHintForArrow(checkHint *v1.CheckHint, resourceType string, tuplesetRelation string, computedUsersetRelation string, subject tuple.ObjectAndRelation) (string, bool) {
	if checkHint.TtuComputedUsersetRelation != computedUsersetRelation {
		return "", false
	}

	if checkHint.Resource.Namespace == resourceType && checkHint.Resource.Relation == tuplesetRelation && checkHint.Subject.EqualVT(subject.ToCoreONR()) {
		return checkHint.Resource.ObjectId, true
	}

	return "", false
}

// HintForEntrypoint returns a CheckHint for the given reachability graph entrypoint and associated subject and result.
func HintForEntrypoint(re schema.ReachabilityEntrypoint, resourceID string, subject tuple.ObjectAndRelation, result *v1.ResourceCheckResult) (*v1.CheckHint, error) {
	switch re.EntrypointKind() {
	case core.ReachabilityEntrypoint_RELATION_ENTRYPOINT:
		return nil, spiceerrors.MustBugf("cannot call CheckHintForResource for kind %v", re.EntrypointKind())

	case core.ReachabilityEntrypoint_TUPLESET_TO_USERSET_ENTRYPOINT:
		namespace := re.TargetNamespace()
		tuplesetRelation, err := re.TuplesetRelation()
		if err != nil {
			return nil, err
		}

		computedUsersetRelation, err := re.ComputedUsersetRelation()
		if err != nil {
			return nil, err
		}

		return CheckHintForArrow(namespace, resourceID, tuplesetRelation, computedUsersetRelation, subject, result), nil

	case core.ReachabilityEntrypoint_COMPUTED_USERSET_ENTRYPOINT:
		namespace := re.TargetNamespace()
		relation, err := re.ComputedUsersetRelation()
		if err != nil {
			return nil, err
		}

		return CheckHintForComputedUserset(namespace, resourceID, relation, subject, result), nil

	default:
		return nil, spiceerrors.MustBugf("unknown relation entrypoint kind")
	}
}
