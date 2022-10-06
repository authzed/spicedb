package v1alpha1

import (
	"context"
	"errors"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func rewriteError(ctx context.Context, err error) error {
	// Check if the error can be directly used.
	if _, ok := status.FromError(err); ok {
		return err
	}

	// Otherwise, convert based on the error.
	var nsNotFoundError sharederrors.UnknownNamespaceError
	var errPreconditionFailure *writeSchemaPreconditionFailure
	var compilerError compiler.BaseCompilerError
	var sourceError spiceerrors.ErrorWithSource
	var typeError namespace.TypeError

	switch {
	case errors.As(err, &typeError):
		return spiceerrors.WithCodeAndReason(err, codes.InvalidArgument, v1.ErrorReason_ERROR_REASON_SCHEMA_TYPE_ERROR)
	case errors.As(err, &compilerError):
		return spiceerrors.WithCodeAndReason(err, codes.InvalidArgument, v1.ErrorReason_ERROR_REASON_SCHEMA_PARSE_ERROR)
	case errors.As(err, &sourceError):
		return spiceerrors.WithCodeAndReason(err, codes.InvalidArgument, v1.ErrorReason_ERROR_REASON_SCHEMA_PARSE_ERROR)

	case errors.As(err, &nsNotFoundError):
		return spiceerrors.WithCodeAndReason(err, codes.NotFound, v1.ErrorReason_ERROR_REASON_UNKNOWN_DEFINITION)

	case errors.As(err, &datastore.ErrReadOnly{}):
		return shared.ErrServiceReadOnly
	case errors.As(err, &errPreconditionFailure):
		return status.Errorf(codes.FailedPrecondition, "%s", err)
	default:
		log.Ctx(ctx).Err(err).Msg("received unexpected error")
		return err
	}
}
