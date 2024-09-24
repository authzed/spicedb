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
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/typesystem"
)

// ErrInvalidSubjectType indicates that a write was attempted with a subject type which is not
// allowed on relation.
type ErrInvalidSubjectType struct {
	error
	tuple             *core.RelationTuple
	relationType      *core.AllowedRelation
	additionalDetails map[string]string
}

// NewInvalidSubjectTypeError constructs a new error for attempting to write an invalid subject type.
func NewInvalidSubjectTypeError(
	update *core.RelationTuple,
	relationType *core.AllowedRelation,
	typeSystem *typesystem.TypeSystem,
) error {
	allowedTypes, err := typeSystem.AllowedDirectRelationsAndWildcards(update.ResourceAndRelation.Relation)
	if err != nil {
		return err
	}

	// Special case: if the subject is uncaveated but only a caveated version is allowed, return
	// a more descriptive error.
	if update.Caveat == nil {
		allowedCaveatsForSubject := mapz.NewSet[string]()

		for _, allowedType := range allowedTypes {
			if allowedType.RequiredCaveat != nil &&
				allowedType.RequiredCaveat.CaveatName != "" &&
				allowedType.Namespace == update.Subject.Namespace &&
				allowedType.GetRelation() == update.Subject.Relation {
				allowedCaveatsForSubject.Add(allowedType.RequiredCaveat.CaveatName)
			}
		}

		if !allowedCaveatsForSubject.IsEmpty() {
			return ErrInvalidSubjectType{
				error: fmt.Errorf(
					"subjects of type `%s` are not allowed on relation `%s#%s` without one of the following caveats: %s",
					typesystem.SourceForAllowedRelation(relationType),
					update.ResourceAndRelation.Namespace,
					update.ResourceAndRelation.Relation,
					strings.Join(allowedCaveatsForSubject.AsSlice(), ","),
				),
				tuple:        update,
				relationType: relationType,
				additionalDetails: map[string]string{
					"allowed_caveats": strings.Join(allowedCaveatsForSubject.AsSlice(), ","),
				},
			}
		}
	}

	allowedTypeStrings := make([]string, 0, len(allowedTypes))
	for _, allowedType := range allowedTypes {
		allowedTypeStrings = append(allowedTypeStrings, typesystem.SourceForAllowedRelation(allowedType))
	}

	matches := fuzzy.RankFind(typesystem.SourceForAllowedRelation(relationType), allowedTypeStrings)
	sort.Sort(matches)
	if len(matches) > 0 {
		return ErrInvalidSubjectType{
			error: fmt.Errorf(
				"subjects of type `%s` are not allowed on relation `%s#%s`; did you mean `%s`?",
				typesystem.SourceForAllowedRelation(relationType),
				update.ResourceAndRelation.Namespace,
				update.ResourceAndRelation.Relation,
				matches[0].Target,
			),
			tuple:             update,
			relationType:      relationType,
			additionalDetails: nil,
		}
	}

	return ErrInvalidSubjectType{
		error: fmt.Errorf(
			"subjects of type `%s` are not allowed on relation `%s#%s`",
			typesystem.SourceForAllowedRelation(relationType),
			update.ResourceAndRelation.Namespace,
			update.ResourceAndRelation.Relation,
		),
		tuple:             update,
		relationType:      relationType,
		additionalDetails: nil,
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrInvalidSubjectType) GRPCStatus() *status.Status {
	details := map[string]string{
		"definition_name": err.tuple.ResourceAndRelation.Namespace,
		"relation_name":   err.tuple.ResourceAndRelation.Relation,
		"subject_type":    typesystem.SourceForAllowedRelation(err.relationType),
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

// ErrCannotWriteToPermission indicates that a write was attempted on a permission.
type ErrCannotWriteToPermission struct {
	error
	tuple *core.RelationTuple
}

// NewCannotWriteToPermissionError constructs a new error for attempting to write to a permission.
func NewCannotWriteToPermissionError(update *core.RelationTuple) ErrCannotWriteToPermission {
	return ErrCannotWriteToPermission{
		error: fmt.Errorf(
			"cannot write a relationship to permission `%s` under definition `%s`",
			update.ResourceAndRelation.Relation,
			update.ResourceAndRelation.Namespace,
		),
		tuple: update,
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrCannotWriteToPermission) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_CANNOT_UPDATE_PERMISSION,
			map[string]string{
				"definition_name": err.tuple.ResourceAndRelation.Namespace,
				"permission_name": err.tuple.ResourceAndRelation.Relation,
			},
		),
	)
}

// ErrCaveatNotFound indicates that a caveat referenced in a relationship update was not found.
type ErrCaveatNotFound struct {
	error
	tuple *core.RelationTuple
}

// NewCaveatNotFoundError constructs a new caveat not found error.
func NewCaveatNotFoundError(update *core.RelationTuple) ErrCaveatNotFound {
	return ErrCaveatNotFound{
		error: fmt.Errorf(
			"the caveat `%s` was not found for relationship `%s`",
			update.Caveat.CaveatName,
			tuple.MustString(update),
		),
		tuple: update,
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrCaveatNotFound) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.FailedPrecondition,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_UNKNOWN_CAVEAT,
			map[string]string{
				"caveat_name": err.tuple.Caveat.CaveatName,
			},
		),
	)
}
