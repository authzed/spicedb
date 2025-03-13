package relationships

import (
	"fmt"
	"maps"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/lithammer/fuzzysearch/fuzzy"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// InvalidSubjectTypeError indicates that a write was attempted with a subject type which is not
// allowed on relation.
type InvalidSubjectTypeError struct {
	error
	relationship      tuple.Relationship
	relationType      *core.AllowedRelation
	additionalDetails map[string]string
}

// NewInvalidSubjectTypeError constructs a new error for attempting to write an invalid subject type.
func NewInvalidSubjectTypeError(
	relationship tuple.Relationship,
	relationType *core.AllowedRelation,
	definition *schema.Definition,
) error {
	allowedTypes, err := definition.AllowedDirectRelationsAndWildcards(relationship.Resource.Relation)
	if err != nil {
		return err
	}

	// Special case: if the subject is uncaveated but only a caveated version is allowed, return
	// a more descriptive error.
	if relationship.OptionalCaveat == nil {
		allowedCaveatsForSubject := mapz.NewSet[string]()

		for _, allowedType := range allowedTypes {
			if allowedType.RequiredCaveat != nil &&
				allowedType.RequiredCaveat.CaveatName != "" &&
				allowedType.Namespace == relationship.Subject.ObjectType &&
				allowedType.GetRelation() == relationship.Subject.Relation &&
				(allowedType.RequiredExpiration != nil) == (relationship.OptionalExpiration != nil) {
				allowedCaveatsForSubject.Add(allowedType.RequiredCaveat.CaveatName)
			}
		}

		if !allowedCaveatsForSubject.IsEmpty() {
			return InvalidSubjectTypeError{
				error: fmt.Errorf(
					"subjects of type `%s` are not allowed on relation `%s#%s` without one of the following caveats: %s",
					schema.SourceForAllowedRelation(relationType),
					relationship.Resource.ObjectType,
					relationship.Resource.Relation,
					strings.Join(allowedCaveatsForSubject.AsSlice(), ","),
				),
				relationship: relationship,
				relationType: relationType,
				additionalDetails: map[string]string{
					"allowed_caveats": strings.Join(allowedCaveatsForSubject.AsSlice(), ","),
				},
			}
		}
	}

	allowedTypeStrings := make([]string, 0, len(allowedTypes))
	for _, allowedType := range allowedTypes {
		allowedTypeStrings = append(allowedTypeStrings, schema.SourceForAllowedRelation(allowedType))
	}

	matches := fuzzy.RankFind(schema.SourceForAllowedRelation(relationType), allowedTypeStrings)
	sort.Sort(matches)
	if len(matches) > 0 {
		return InvalidSubjectTypeError{
			error: fmt.Errorf(
				"subjects of type `%s` are not allowed on relation `%s#%s`; did you mean `%s`?",
				schema.SourceForAllowedRelation(relationType),
				relationship.Resource.ObjectType,
				relationship.Resource.Relation,
				matches[0].Target,
			),
			relationship:      relationship,
			relationType:      relationType,
			additionalDetails: nil,
		}
	}

	return InvalidSubjectTypeError{
		error: fmt.Errorf(
			"subjects of type `%s` are not allowed on relation `%s#%s`",
			schema.SourceForAllowedRelation(relationType),
			relationship.Resource.ObjectType,
			relationship.Resource.Relation,
		),
		relationship:      relationship,
		relationType:      relationType,
		additionalDetails: nil,
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err InvalidSubjectTypeError) GRPCStatus() *status.Status {
	details := map[string]string{
		"definition_name": err.relationship.Resource.ObjectType,
		"relation_name":   err.relationship.Resource.Relation,
		"subject_type":    schema.SourceForAllowedRelation(err.relationType),
	}

	if err.additionalDetails != nil {
		maps.Copy(details, err.additionalDetails)
	}

	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_INVALID_SUBJECT_TYPE,
			details,
		),
	)
}

// CannotWriteToPermissionError indicates that a write was attempted on a permission.
type CannotWriteToPermissionError struct {
	error
	rel tuple.Relationship
}

// NewCannotWriteToPermissionError constructs a new error for attempting to write to a permission.
func NewCannotWriteToPermissionError(rel tuple.Relationship) CannotWriteToPermissionError {
	return CannotWriteToPermissionError{
		error: fmt.Errorf(
			"cannot write a relationship to permission `%s` under definition `%s`",
			rel.Resource.Relation,
			rel.Resource.ObjectType,
		),
		rel: rel,
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err CannotWriteToPermissionError) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_CANNOT_UPDATE_PERMISSION,
			map[string]string{
				"definition_name": err.rel.Resource.ObjectType,
				"permission_name": err.rel.Resource.Relation,
			},
		),
	)
}

// CaveatNotFoundError indicates that a caveat referenced in a relationship update was not found.
type CaveatNotFoundError struct {
	error
	relationship tuple.Relationship
}

// NewCaveatNotFoundError constructs a new caveat not found error.
func NewCaveatNotFoundError(relationship tuple.Relationship) CaveatNotFoundError {
	return CaveatNotFoundError{
		error: fmt.Errorf(
			"the caveat `%s` was not found for relationship `%s`",
			relationship.OptionalCaveat.CaveatName,
			tuple.MustString(relationship),
		),
		relationship: relationship,
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err CaveatNotFoundError) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.FailedPrecondition,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_UNKNOWN_CAVEAT,
			map[string]string{
				"caveat_name": err.relationship.OptionalCaveat.CaveatName,
			},
		),
	)
}
