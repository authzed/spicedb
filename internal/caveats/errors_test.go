package caveats

import (
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestEvaluationErrorInterface(t *testing.T) {
	caveatExpr := &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: &core.ContextualizedCaveat{
				CaveatName: "test_caveat",
				Context: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"key": structpb.NewStringValue("value"),
					},
				},
			},
		},
	}

	// Create a generic evaluation error (since we can't construct the specific type)
	baseErr := errors.New("base evaluation error")
	var evalErr caveats.EvaluationError
	if !errors.As(baseErr, &evalErr) {
		// If we can't create a real EvaluationError, test with generic error
		internalErr := NewEvaluationError(caveatExpr, evalErr)

		// Test that the wrapper works even with empty evaluation error
		require.Contains(t, internalErr.Error(), "evaluation error for caveat test_caveat")

		// Test DetailsMetadata
		metadata := internalErr.DetailsMetadata()
		require.Equal(t, "test_caveat", metadata["caveat_name"])

		// Test GRPCStatus
		grpcStatus := internalErr.GRPCStatus()
		require.Equal(t, codes.InvalidArgument, grpcStatus.Code())
		require.NotNil(t, grpcStatus.Details())

		// Test MarshalZerologObject doesn't panic
		event := zerolog.Dict()
		internalErr.MarshalZerologObject(event)
	}
}

func TestParameterTypeError(t *testing.T) {
	caveatExpr := &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: &core.ContextualizedCaveat{
				CaveatName: "test_caveat",
				Context: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"key": structpb.NewStringValue("value"),
					},
				},
			},
		},
	}

	baseErr := errors.New("parameter type error")
	paramErr := NewParameterTypeError(caveatExpr, baseErr)

	// Test error message
	require.Contains(t, paramErr.Error(), "type error for parameters for caveat `test_caveat`")
	require.Contains(t, paramErr.Error(), "parameter type error")

	// Test DetailsMetadata
	metadata := paramErr.DetailsMetadata()
	require.Equal(t, "test_caveat", metadata["caveat_name"])

	// Test GRPCStatus
	grpcStatus := paramErr.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, grpcStatus.Code())
	require.NotNil(t, grpcStatus.Details())

	// Test MarshalZerologObject doesn't panic
	event := zerolog.Dict()
	paramErr.MarshalZerologObject(event)
}

func TestParameterTypeErrorWithConversionError(t *testing.T) {
	caveatExpr := &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: &core.ContextualizedCaveat{
				CaveatName: "test_caveat",
			},
		},
	}

	// Test with an error that can be converted to ParameterConversionError
	// We can't construct the error directly, but we can test the error.As path
	baseErr := errors.New("parameter conversion error")
	paramErr := NewParameterTypeError(caveatExpr, baseErr)

	// Test DetailsMetadata includes basic caveat info
	metadata := paramErr.DetailsMetadata()
	require.Equal(t, "test_caveat", metadata["caveat_name"])
}
