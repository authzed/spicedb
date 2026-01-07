package common

import (
	"fmt"

	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewUnsupportedError wraps an error as a Postgres "feature not supported" error.
// This is used when a query or feature is not implemented by the FDW.
func NewUnsupportedError(baseErr error) error {
	err := fmt.Errorf("unsupported feature or query type: %w", baseErr)
	err = psqlerr.WithCode(err, codes.FeatureNotSupported)
	err = psqlerr.WithSeverity(err, psqlerr.LevelFatal)
	return err
}

// NewSemanticsError wraps an error as a Postgres semantic error with fatal severity.
// This is used when a query is syntactically valid but semantically incorrect.
func NewSemanticsError(baseErr error) error {
	err := fmt.Errorf("semantic error: %w", baseErr)
	err = psqlerr.WithCode(err, codes.InvalidParameterValue)
	err = psqlerr.WithSeverity(err, psqlerr.LevelFatal)
	return err
}

// NewQueryError wraps an error as a Postgres query error.
// This is used for runtime query execution errors.
func NewQueryError(baseErr error) error {
	err := fmt.Errorf("query error: %w", baseErr)
	err = psqlerr.WithCode(err, codes.InvalidParameterValue)
	err = psqlerr.WithSeverity(err, psqlerr.LevelError)
	return err
}

// ConvertError converts gRPC status errors to appropriate Postgres errors.
func ConvertError(err error) error {
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case grpccodes.InvalidArgument:
			fallthrough

		case grpccodes.FailedPrecondition:
			return NewQueryError(err)
		}
	}

	return err
}
