package caveats

import (
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// CaveatEvaluationErr is an error in evaluation of a caveat expression.
type CaveatEvaluationErr struct {
	error
	caveatExpr *core.CaveatExpression
	evalErr    caveats.EvaluationErr
}

// MarshalZerologObject implements zerolog.LogObjectMarshaler
func (err CaveatEvaluationErr) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("caveat_name", err.caveatExpr.GetCaveat().CaveatName).Interface("context", err.caveatExpr.GetCaveat().Context)
}

// DetailsMetadata returns the metadata for details for this error.
func (err CaveatEvaluationErr) DetailsMetadata() map[string]string {
	return spiceerrors.CombineMetadata(err.evalErr, map[string]string{
		"caveat_name": err.caveatExpr.GetCaveat().CaveatName,
	})
}

func (err CaveatEvaluationErr) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_CAVEAT_EVALUATION_ERROR,
			err.DetailsMetadata(),
		),
	)
}

func NewCaveatEvaluationErr(caveatExpr *core.CaveatExpression, err caveats.EvaluationErr) CaveatEvaluationErr {
	return CaveatEvaluationErr{
		fmt.Errorf("evaluation error for caveat %s: %w", caveatExpr.GetCaveat().CaveatName, err), caveatExpr, err,
	}
}

// CaveatParameterTypeError is a type error in constructing a parameter from a value.
type CaveatParameterTypeError struct {
	error
	caveatExpr      *core.CaveatExpression
	conversionError *caveats.ParameterConversionErr
}

// MarshalZerologObject implements zerolog.LogObjectMarshaler
func (err CaveatParameterTypeError) MarshalZerologObject(e *zerolog.Event) {
	evt := e.Err(err.error).
		Str("caveat_name", err.caveatExpr.GetCaveat().CaveatName).
		Interface("context", err.caveatExpr.GetCaveat().Context)

	if err.conversionError != nil {
		evt.Str("parameter_name", err.conversionError.ParameterName())
	}
}

// DetailsMetadata returns the metadata for details for this error.
func (err CaveatParameterTypeError) DetailsMetadata() map[string]string {
	if err.conversionError != nil {
		return spiceerrors.CombineMetadata(err.conversionError, map[string]string{
			"caveat_name": err.caveatExpr.GetCaveat().CaveatName,
		})
	}

	return map[string]string{
		"caveat_name": err.caveatExpr.GetCaveat().CaveatName,
	}
}

func (err CaveatParameterTypeError) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_CAVEAT_PARAMETER_TYPE_ERROR,
			err.DetailsMetadata(),
		),
	)
}

func NewCaveatParameterTypeError(caveatExpr *core.CaveatExpression, err error) CaveatParameterTypeError {
	conversionError := &caveats.ParameterConversionErr{}
	if !errors.As(err, conversionError) {
		conversionError = nil
	}

	return CaveatParameterTypeError{
		fmt.Errorf("type error for parameters for caveat `%s`: %w", caveatExpr.GetCaveat().CaveatName, err),
		caveatExpr,
		conversionError,
	}
}
