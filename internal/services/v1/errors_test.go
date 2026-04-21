package v1

import (
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

func TestExceedsMaximumLimitError(t *testing.T) {
	err := NewExceedsMaximumLimitErr(100, 50)

	require.Contains(t, err.Error(), "100")
	require.Contains(t, err.Error(), "50")

	status := err.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.NotEmpty(t, status.Details())

	require.NotPanics(t, func() {
		err.MarshalZerologObject(zerolog.Dict())
	})
}

func TestExceedsMaximumChecksError(t *testing.T) {
	err := NewExceedsMaximumChecksErr(100, 50)

	require.Contains(t, err.Error(), "100")
	require.Contains(t, err.Error(), "50")

	status := err.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.NotEmpty(t, status.Details())

	require.NotPanics(t, func() {
		err.MarshalZerologObject(zerolog.Dict())
	})
}

func TestExceedsMaximumUpdatesError(t *testing.T) {
	err := NewExceedsMaximumUpdatesErr(1000, 500)

	require.Contains(t, err.Error(), "1000")
	require.Contains(t, err.Error(), "500")
	require.Contains(t, err.Error(), "ImportBulkRelationships")

	status := err.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.NotEmpty(t, status.Details())

	require.NotPanics(t, func() {
		err.MarshalZerologObject(zerolog.Dict())
	})
}

func TestExceedsMaximumPreconditionsError(t *testing.T) {
	err := NewExceedsMaximumPreconditionsErr(50, 10)

	require.Contains(t, err.Error(), "50")
	require.Contains(t, err.Error(), "10")

	status := err.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.NotEmpty(t, status.Details())

	require.NotPanics(t, func() {
		err.MarshalZerologObject(zerolog.Dict())
	})
}

func TestPreconditionFailedError_Minimal(t *testing.T) {
	precondition := &v1.Precondition{
		Operation: v1.Precondition_OPERATION_MUST_MATCH,
		Filter: &v1.RelationshipFilter{
			ResourceType: "document",
		},
	}
	err := func() PreconditionFailedError {
		var target PreconditionFailedError
		_ = errors.As(NewPreconditionFailedErr(precondition), &target)
		return target
	}()

	require.Contains(t, err.Error(), "precondition")

	status := err.GRPCStatus()
	require.Equal(t, codes.FailedPrecondition, status.Code())
	require.NotEmpty(t, status.Details())

	require.NotPanics(t, func() {
		err.MarshalZerologObject(zerolog.Dict())
	})
}

func TestPreconditionFailedError_FullFilter(t *testing.T) {
	precondition := &v1.Precondition{
		Operation: v1.Precondition_OPERATION_MUST_NOT_MATCH,
		Filter: &v1.RelationshipFilter{
			ResourceType:             "document",
			OptionalResourceId:       "first",
			OptionalResourceIdPrefix: "doc-",
			OptionalRelation:         "viewer",
			OptionalSubjectFilter: &v1.SubjectFilter{
				SubjectType:       "user",
				OptionalSubjectId: "alice",
				OptionalRelation: &v1.SubjectFilter_RelationFilter{
					Relation: "member",
				},
			},
		},
	}
	err := func() PreconditionFailedError {
		var target PreconditionFailedError
		_ = errors.As(NewPreconditionFailedErr(precondition), &target)
		return target
	}()

	status := err.GRPCStatus()
	require.Equal(t, codes.FailedPrecondition, status.Code())
	require.NotEmpty(t, status.Details())
}

func TestDuplicateRelationshipError(t *testing.T) {
	update := &v1.RelationshipUpdate{
		Operation: v1.RelationshipUpdate_OPERATION_CREATE,
		Relationship: &v1.Relationship{
			Resource: &v1.ObjectReference{
				ObjectType: "document",
				ObjectId:   "first",
			},
			Relation: "viewer",
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: "user",
					ObjectId:   "alice",
				},
			},
		},
	}
	err := NewDuplicateRelationshipErr(update)

	require.Contains(t, err.Error(), "document")
	require.Contains(t, err.Error(), "alice")

	status := err.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.NotEmpty(t, status.Details())
}

func TestErrMaxRelationshipContextError(t *testing.T) {
	update := &v1.RelationshipUpdate{
		Operation: v1.RelationshipUpdate_OPERATION_CREATE,
		Relationship: &v1.Relationship{
			Resource: &v1.ObjectReference{ObjectType: "document", ObjectId: "first"},
			Relation: "viewer",
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "alice"},
			},
		},
	}
	err := NewMaxRelationshipContextError(update, 1024)

	require.Contains(t, err.Error(), "1024")

	status := err.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.NotEmpty(t, status.Details())
}

func TestCouldNotTransactionallyDeleteError_Minimal(t *testing.T) {
	filter := &v1.RelationshipFilter{ResourceType: "document"}
	err := NewCouldNotTransactionallyDeleteErr(filter, 100)

	require.Contains(t, err.Error(), "100")

	status := err.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.NotEmpty(t, status.Details())
}

func TestCouldNotTransactionallyDeleteError_FullFilter(t *testing.T) {
	filter := &v1.RelationshipFilter{
		ResourceType:       "document",
		OptionalResourceId: "first",
		OptionalRelation:   "viewer",
		OptionalSubjectFilter: &v1.SubjectFilter{
			SubjectType:       "user",
			OptionalSubjectId: "alice",
			OptionalRelation: &v1.SubjectFilter_RelationFilter{
				Relation: "member",
			},
		},
	}
	err := NewCouldNotTransactionallyDeleteErr(filter, 100)

	status := err.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.NotEmpty(t, status.Details())
}

func TestInvalidCursorError(t *testing.T) {
	err := NewInvalidCursorErr("malformed")

	require.Contains(t, err.Error(), "malformed")

	status := err.GRPCStatus()
	require.Equal(t, codes.FailedPrecondition, status.Code())
	require.NotEmpty(t, status.Details())
}

func TestInvalidFilterError(t *testing.T) {
	err := NewInvalidFilterErr("bad filter", "filter-repr")

	require.Contains(t, err.Error(), "bad filter")

	status := err.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.NotEmpty(t, status.Details())
}

func TestEmptyPreconditionError(t *testing.T) {
	err := NewEmptyPreconditionErr()

	require.Equal(t, "one of the specified preconditions is empty", err.Error())

	status := err.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, status.Code())
}

func TestNotAPermissionError(t *testing.T) {
	err := NewNotAPermissionError("viewer")

	require.Contains(t, err.Error(), "viewer")

	status := err.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.NotEmpty(t, status.Details())
}

func TestTransactionMetadataTooLargeError(t *testing.T) {
	err := NewTransactionMetadataTooLargeErr(1024, 512)

	require.Contains(t, err.Error(), "1024")
	require.Contains(t, err.Error(), "512")

	status := err.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.NotEmpty(t, status.Details())

	require.NotPanics(t, func() {
		err.MarshalZerologObject(zerolog.Dict())
	})
}

func TestDefaultIfZero(t *testing.T) {
	require.Equal(t, "fallback", defaultIfZero("", "fallback"))
	require.Equal(t, "explicit", defaultIfZero("explicit", "fallback"))

	require.Equal(t, 5, defaultIfZero(0, 5))
	require.Equal(t, 7, defaultIfZero(7, 5))
}
