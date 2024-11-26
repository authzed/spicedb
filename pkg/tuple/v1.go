package tuple

import (
	"fmt"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
	"google.golang.org/protobuf/types/known/timestamppb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// ParseV1Rel parses a string representation of a relationship into a v1.Relationship object.
func ParseV1Rel(relString string) (*v1.Relationship, error) {
	parsed, err := Parse(relString)
	if err != nil {
		return nil, err
	}

	return ToV1Relationship(parsed), nil
}

// Same as above, but panics if it cannot be parsed. Should only be used in tests.
func MustParseV1Rel(relString string) *v1.Relationship {
	parsed, err := Parse(relString)
	if err != nil {
		panic(fmt.Sprintf("could not parse relationship string: %s %s", relString, err))
	}

	return ToV1Relationship(parsed)
}

// MustV1RelString converts a relationship into a string.  Will panic if
// the Relationship does not validate.
func MustV1RelString(rel *v1.Relationship) string {
	if err := rel.Validate(); err != nil {
		panic(fmt.Sprintf("invalid relationship: %#v %s", rel, err))
	}
	return MustV1StringRelationship(rel)
}

// StringObjectRef marshals a *v1.ObjectReference into a string.
//
// This function assumes that the provided values have already been validated.
func V1StringObjectRef(ref *v1.ObjectReference) string {
	return JoinObjectRef(ref.ObjectType, ref.ObjectId)
}

// StringSubjectRef marshals a *v1.SubjectReference into a string.
//
// This function assumes that the provided values have already been validated.
func V1StringSubjectRef(ref *v1.SubjectReference) string {
	if ref.OptionalRelation == "" {
		return V1StringObjectRef(ref.Object)
	}
	return JoinRelRef(V1StringObjectRef(ref.Object), ref.OptionalRelation)
}

// MustV1StringRelationship converts a v1.Relationship to a string.
func MustV1StringRelationship(rel *v1.Relationship) string {
	relString, err := V1StringRelationship(rel)
	if err != nil {
		panic(err)
	}
	return relString
}

// V1StringRelationship converts a v1.Relationship to a string.
func V1StringRelationship(rel *v1.Relationship) (string, error) {
	if rel == nil || rel.Resource == nil || rel.Subject == nil {
		return "", nil
	}

	caveatString, err := V1StringCaveatRef(rel.OptionalCaveat)
	if err != nil {
		return "", err
	}

	expirationString, err := V1StringExpiration(rel.OptionalExpiresAt)
	if err != nil {
		return "", err
	}

	return V1StringRelationshipWithoutCaveatOrExpiration(rel) + caveatString + expirationString, nil
}

func V1StringExpiration(expiration *timestamppb.Timestamp) (string, error) {
	if expiration == nil {
		return "", nil
	}

	return "[expiration:" + expiration.AsTime().Format(expirationFormat) + "]", nil
}

// V1StringRelationshipWithoutCaveatOrExpiration converts a v1.Relationship to a string, excluding any caveat.
func V1StringRelationshipWithoutCaveatOrExpiration(rel *v1.Relationship) string {
	if rel == nil || rel.Resource == nil || rel.Subject == nil {
		return ""
	}

	return V1StringObjectRef(rel.Resource) + "#" + rel.Relation + "@" + V1StringSubjectRef(rel.Subject)
}

// V1StringCaveatRef converts a v1.ContextualizedCaveat to a string.
func V1StringCaveatRef(caveat *v1.ContextualizedCaveat) (string, error) {
	if caveat == nil || caveat.CaveatName == "" {
		return "", nil
	}

	contextString, err := StringCaveatContext(caveat.Context)
	if err != nil {
		return "", err
	}

	if len(contextString) > 0 {
		contextString = ":" + contextString
	}

	return "[" + caveat.CaveatName + contextString + "]", nil
}

// UpdateToV1RelationshipUpdate converts a RelationshipUpdate into a
// v1.RelationshipUpdate.
func UpdateToV1RelationshipUpdate(update RelationshipUpdate) (*v1.RelationshipUpdate, error) {
	var op v1.RelationshipUpdate_Operation
	switch update.Operation {
	case UpdateOperationCreate:
		op = v1.RelationshipUpdate_OPERATION_CREATE
	case UpdateOperationDelete:
		op = v1.RelationshipUpdate_OPERATION_DELETE
	case UpdateOperationTouch:
		op = v1.RelationshipUpdate_OPERATION_TOUCH
	default:
		return nil, spiceerrors.MustBugf("unknown update operation: %v", update.Operation)
	}

	return &v1.RelationshipUpdate{
		Operation:    op,
		Relationship: ToV1Relationship(update.Relationship),
	}, nil
}

// MustUpdateToV1RelationshipUpdate converts a RelationshipUpdate into a
// v1.RelationshipUpdate. Panics on error.
func MustUpdateToV1RelationshipUpdate(update RelationshipUpdate) *v1.RelationshipUpdate {
	v1rel, err := UpdateToV1RelationshipUpdate(update)
	if err != nil {
		panic(err)
	}

	return v1rel
}

// UpdateFromV1RelationshipUpdate converts a RelationshipUpdate into a
// RelationTupleUpdate.
func UpdateFromV1RelationshipUpdate(update *v1.RelationshipUpdate) (RelationshipUpdate, error) {
	var op UpdateOperation
	switch update.Operation {
	case v1.RelationshipUpdate_OPERATION_CREATE:
		op = UpdateOperationCreate
	case v1.RelationshipUpdate_OPERATION_DELETE:
		op = UpdateOperationDelete
	case v1.RelationshipUpdate_OPERATION_TOUCH:
		op = UpdateOperationTouch
	default:
		return RelationshipUpdate{}, spiceerrors.MustBugf("unknown update operation: %v", update.Operation)
	}

	return RelationshipUpdate{
		Operation:    op,
		Relationship: FromV1Relationship(update.Relationship),
	}, nil
}

// FromV1Relationship converts a v1.Relationship into a Relationship.
func FromV1Relationship(rel *v1.Relationship) Relationship {
	var caveat *core.ContextualizedCaveat
	if rel.OptionalCaveat != nil {
		caveat = &core.ContextualizedCaveat{
			CaveatName: rel.OptionalCaveat.CaveatName,
			Context:    rel.OptionalCaveat.Context,
		}
	}

	var expiration *time.Time
	if rel.OptionalExpiresAt != nil {
		t := rel.OptionalExpiresAt.AsTime()
		expiration = &t
	}

	return Relationship{
		RelationshipReference: RelationshipReference{
			Resource: ObjectAndRelation{
				ObjectID:   rel.Resource.ObjectId,
				ObjectType: rel.Resource.ObjectType,
				Relation:   rel.Relation,
			},
			Subject: ObjectAndRelation{
				ObjectID:   rel.Subject.Object.ObjectId,
				ObjectType: rel.Subject.Object.ObjectType,
				Relation:   stringz.Default(rel.Subject.OptionalRelation, Ellipsis, ""),
			},
		},
		OptionalCaveat:     caveat,
		OptionalExpiration: expiration,
	}
}

// ToV1Relationship converts a Relationship into a v1.Relationship.
func ToV1Relationship(rel Relationship) *v1.Relationship {
	var caveat *v1.ContextualizedCaveat
	if rel.OptionalCaveat != nil {
		caveat = &v1.ContextualizedCaveat{
			CaveatName: rel.OptionalCaveat.CaveatName,
			Context:    rel.OptionalCaveat.Context,
		}
	}

	var expiration *timestamppb.Timestamp
	if rel.OptionalExpiration != nil {
		expiration = timestamppb.New(*rel.OptionalExpiration)
	}

	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: rel.Resource.ObjectType,
			ObjectId:   rel.Resource.ObjectID,
		},
		Relation: rel.Resource.Relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: rel.Subject.ObjectType,
				ObjectId:   rel.Subject.ObjectID,
			},
			OptionalRelation: stringz.Default(rel.Subject.Relation, "", Ellipsis),
		},
		OptionalCaveat:    caveat,
		OptionalExpiresAt: expiration,
	}
}

// CopyToV1Relationship copies a Relationship into a v1.Relationship.
func CopyToV1Relationship(rel Relationship, v1rel *v1.Relationship) {
	v1rel.Resource.ObjectType = rel.Resource.ObjectType
	v1rel.Resource.ObjectId = rel.Resource.ObjectID
	v1rel.Relation = rel.Resource.Relation
	v1rel.Subject.Object.ObjectType = rel.Subject.ObjectType
	v1rel.Subject.Object.ObjectId = rel.Subject.ObjectID
	v1rel.Subject.OptionalRelation = stringz.Default(rel.Subject.Relation, "", Ellipsis)

	if rel.OptionalCaveat != nil {
		if v1rel.OptionalCaveat == nil {
			v1rel.OptionalCaveat = &v1.ContextualizedCaveat{}
		}

		v1rel.OptionalCaveat.CaveatName = rel.OptionalCaveat.CaveatName
		v1rel.OptionalCaveat.Context = rel.OptionalCaveat.Context
	} else {
		v1rel.OptionalCaveat = nil
	}

	if rel.OptionalExpiration != nil {
		v1rel.OptionalExpiresAt = timestamppb.New(*rel.OptionalExpiration)
	} else {
		v1rel.OptionalExpiresAt = nil
	}
}

// UpdatesToV1RelationshipUpdates converts a slice of RelationshipUpdate into a
// slice of v1.RelationshipUpdate.
func UpdatesToV1RelationshipUpdates(updates []RelationshipUpdate) ([]*v1.RelationshipUpdate, error) {
	relationshipUpdates := make([]*v1.RelationshipUpdate, 0, len(updates))

	for _, update := range updates {
		converted, err := UpdateToV1RelationshipUpdate(update)
		if err != nil {
			return nil, err
		}

		relationshipUpdates = append(relationshipUpdates, converted)
	}

	return relationshipUpdates, nil
}

// UpdatesFromV1RelationshipUpdates converts a slice of v1.RelationshipUpdate into a
// slice of RelationshipUpdate.
func UpdatesFromV1RelationshipUpdates(updates []*v1.RelationshipUpdate) ([]RelationshipUpdate, error) {
	relationshipUpdates := make([]RelationshipUpdate, 0, len(updates))

	for _, update := range updates {
		converted, err := UpdateFromV1RelationshipUpdate(update)
		if err != nil {
			return nil, err
		}

		relationshipUpdates = append(relationshipUpdates, converted)
	}

	return relationshipUpdates, nil
}

// ToV1Filter converts a RelationTuple into a RelationshipFilter.
func ToV1Filter(rel Relationship) *v1.RelationshipFilter {
	return &v1.RelationshipFilter{
		ResourceType:          rel.Resource.ObjectType,
		OptionalResourceId:    rel.Resource.ObjectID,
		OptionalRelation:      rel.Resource.Relation,
		OptionalSubjectFilter: SubjectONRToSubjectFilter(rel.Subject),
	}
}

// SubjectONRToSubjectFilter converts a userset to the equivalent exact SubjectFilter.
func SubjectONRToSubjectFilter(subject ObjectAndRelation) *v1.SubjectFilter {
	return &v1.SubjectFilter{
		SubjectType:       subject.ObjectType,
		OptionalSubjectId: subject.ObjectID,
		OptionalRelation: &v1.SubjectFilter_RelationFilter{
			Relation: stringz.Default(subject.Relation, "", Ellipsis),
		},
	}
}

// RelToFilter converts a Relationship into a RelationshipFilter.
func RelToFilter(rel *v1.Relationship) *v1.RelationshipFilter {
	return &v1.RelationshipFilter{
		ResourceType:       rel.Resource.ObjectType,
		OptionalResourceId: rel.Resource.ObjectId,
		OptionalRelation:   rel.Relation,
		OptionalSubjectFilter: &v1.SubjectFilter{
			SubjectType:       rel.Subject.Object.ObjectType,
			OptionalSubjectId: rel.Subject.Object.ObjectId,
			OptionalRelation: &v1.SubjectFilter_RelationFilter{
				Relation: rel.Subject.OptionalRelation,
			},
		},
	}
}
