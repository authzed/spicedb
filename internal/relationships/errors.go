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
	update       *core.RelationTupleUpdate
	relationType *core.AllowedRelation
}

// NewInvalidSubjectTypeError constructs a new error for attempting to write an invalid subject type.
func NewInvalidSubjectTypeError(update *core.RelationTupleUpdate, relationType *core.AllowedRelation) ErrInvalidSubjectType {
	return ErrInvalidSubjectType{
		error: fmt.Errorf(
			"subjects of type `%s` are not allowed on relation `%s#%s`",
			namespace.SourceForAllowedRelation(relationType),
			update.Tuple.ResourceAndRelation.Namespace,
			update.Tuple.ResourceAndRelation.Relation,
		),
		update:       update,
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
				"definition_name": err.update.Tuple.ResourceAndRelation.Namespace,
				"relation_name":   err.update.Tuple.ResourceAndRelation.Relation,
				"subject_type":    namespace.SourceForAllowedRelation(err.relationType),
			},
		),
	)
}

// ErrCannotWriteToPermission indicates that a write was attempted on a permission.
type ErrCannotWriteToPermission struct {
	error
	update *core.RelationTupleUpdate
}

// NewCannotWriteToPermissionError constructs a new error for attempting to write to a permission.
func NewCannotWriteToPermissionError(update *core.RelationTupleUpdate) ErrCannotWriteToPermission {
	return ErrCannotWriteToPermission{
		error: fmt.Errorf(
			"cannot write a relationship to permission `%s` under definition `%s`",
			update.Tuple.ResourceAndRelation.Relation,
			update.Tuple.ResourceAndRelation.Namespace,
		),
		update: update,
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
				"definition_name": err.update.Tuple.ResourceAndRelation.Namespace,
				"permission_name": err.update.Tuple.ResourceAndRelation.Relation,
			},
		),
	)
}

// ErrCaveatNotFound indicates that a caveat referenced in a relationship update was not found.
type ErrCaveatNotFound struct {
	error
	update *core.RelationTupleUpdate
}

// NewCaveatNotFoundError constructs a new caveat not found error.
func NewCaveatNotFoundError(update *core.RelationTupleUpdate) ErrCaveatNotFound {
	return ErrCaveatNotFound{
		error: fmt.Errorf(
			"the caveat `%s` was not found for relationship `%s`",
			update.Tuple.Caveat.CaveatName,
			tuple.MustString(update.Tuple),
		),
		update: update,
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
				"caveat_name": err.update.Tuple.Caveat.CaveatName,
			},
		),
	)
}
