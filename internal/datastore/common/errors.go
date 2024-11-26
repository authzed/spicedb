package common

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// SerializationError is returned when there's been a serialization
// error while performing a datastore operation
type SerializationError struct {
	error
}

func (err SerializationError) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.Aborted,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_SERIALIZATION_FAILURE,
			map[string]string{},
		),
	)
}

func (err SerializationError) Unwrap() error {
	return err.error
}

// NewSerializationError creates a new SerializationError
func NewSerializationError(err error) error {
	return SerializationError{err}
}

// ReadOnlyTransactionError is returned when an otherwise read-write
// transaction fails on writes with an error indicating that the datastore
// is currently in a read-only mode.
type ReadOnlyTransactionError struct {
	error
}

func (err ReadOnlyTransactionError) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.Aborted,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_SERVICE_READ_ONLY,
			map[string]string{},
		),
	)
}

// NewReadOnlyTransactionError creates a new ReadOnlyTransactionError.
func NewReadOnlyTransactionError(err error) error {
	return ReadOnlyTransactionError{
		fmt.Errorf("could not perform write operation, as the datastore is currently in read-only mode: %w. This may indicate that the datastore has been put into maintenance mode", err),
	}
}

// CreateRelationshipExistsError is an error returned when attempting to CREATE an already-existing
// relationship.
type CreateRelationshipExistsError struct {
	error

	// Relationship is the relationship that caused the error. May be nil, depending on the datastore.
	Relationship *tuple.Relationship
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err CreateRelationshipExistsError) GRPCStatus() *status.Status {
	if err.Relationship == nil {
		return spiceerrors.WithCodeAndDetails(
			err,
			codes.AlreadyExists,
			spiceerrors.ForReason(
				v1.ErrorReason_ERROR_REASON_ATTEMPT_TO_RECREATE_RELATIONSHIP,
				map[string]string{},
			),
		)
	}

	relationship := tuple.ToV1Relationship(*err.Relationship)
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.AlreadyExists,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_ATTEMPT_TO_RECREATE_RELATIONSHIP,
			map[string]string{
				"relationship":       tuple.V1StringRelationshipWithoutCaveatOrExpiration(relationship),
				"resource_type":      relationship.Resource.ObjectType,
				"resource_object_id": relationship.Resource.ObjectId,
				"resource_relation":  relationship.Relation,
				"subject_type":       relationship.Subject.Object.ObjectType,
				"subject_object_id":  relationship.Subject.Object.ObjectId,
				"subject_relation":   relationship.Subject.OptionalRelation,
			},
		),
	)
}

// NewCreateRelationshipExistsError creates a new CreateRelationshipExistsError.
func NewCreateRelationshipExistsError(relationship *tuple.Relationship) error {
	msg := "could not CREATE one or more relationships, as they already existed. If this is persistent, please switch to TOUCH operations or specify a precondition"
	if relationship != nil {
		msg = fmt.Sprintf("could not CREATE relationship `%s`, as it already existed. If this is persistent, please switch to TOUCH operations or specify a precondition", tuple.StringWithoutCaveatOrExpiration(*relationship))
	}

	return CreateRelationshipExistsError{
		errors.New(msg),
		relationship,
	}
}

var (
	portMatchRegex  = regexp.MustCompile("invalid port \\\"(.+)\\\" after host")
	parseMatchRegex = regexp.MustCompile("parse \\\"(.+)\\\":")
)

// RedactAndLogSensitiveConnString elides the given error, logging it only at trace
// level (after being redacted).
func RedactAndLogSensitiveConnString(ctx context.Context, baseErr string, err error, pgURL string) error {
	if err == nil {
		return errors.New(baseErr)
	}

	// See: https://github.com/jackc/pgx/issues/1271
	filtered := err.Error()
	filtered = strings.ReplaceAll(filtered, pgURL, "(redacted)")
	filtered = portMatchRegex.ReplaceAllString(filtered, "(redacted)")
	filtered = parseMatchRegex.ReplaceAllString(filtered, "(redacted)")
	log.Ctx(ctx).Trace().Msg(baseErr + ": " + filtered)
	return fmt.Errorf("%s. To view details of this error (that may contain sensitive information), please run with --log-level=trace", baseErr)
}

// RevisionUnavailableError is returned when a revision is not available on a replica.
type RevisionUnavailableError struct {
	error
}

// NewRevisionUnavailableError creates a new RevisionUnavailableError.
func NewRevisionUnavailableError(err error) error {
	return RevisionUnavailableError{err}
}
