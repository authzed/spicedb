package relationships

import (
	"fmt"

	"github.com/authzed/spicedb/internal/namespace"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ErrInvalidSubjectType indicates that a write was attempted with a subject type which is not
// allowed on relation.
type ErrInvalidSubjectType struct {
	error
	tuple        *core.RelationTuple
	relationType *core.AllowedRelation
}

// NewInvalidSubjectTypeError constructs a new error for attempting to write an invalid subject type.
func NewInvalidSubjectTypeError(update *core.RelationTuple, relationType *core.AllowedRelation) ErrInvalidSubjectType {
	return ErrInvalidSubjectType{
		error: fmt.Errorf(
			"subjects of type `%s` are not allowed on relation `%s#%s`",
			namespace.SourceForAllowedRelation(relationType),
			update.ResourceAndRelation.Namespace,
			update.ResourceAndRelation.Relation,
		),
		tuple:        update,
		relationType: relationType,
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrInvalidSubjectType) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_INVALID_SUBJECT_TYPE,
			map[string]string{
				"definition_name": err.tuple.ResourceAndRelation.Namespace,
				"relation_name":   err.tuple.ResourceAndRelation.Relation,
				"subject_type":    namespace.SourceForAllowedRelation(err.relationType),
			},
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
