//go:build wasm
// +build wasm

package main

import (
	"fmt"
	"strings"

	"github.com/authzed/spicedb/pkg/development"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/tuple"
)

func runOperation(devContext *development.DevContext, operation *devinterface.Operation) (*devinterface.OperationResult, error) {
	switch {
	case operation.FormatSchemaParameters != nil:
		formatted, _, err := generator.GenerateSchema(devContext.CompiledSchema.OrderedDefinitions)
		if err != nil {
			return nil, err
		}

		trimmed := strings.TrimSpace(formatted)
		return &devinterface.OperationResult{
			FormatSchemaResult: &devinterface.FormatSchemaResult{
				FormattedSchema: trimmed,
			},
		}, nil

	case operation.CheckParameters != nil:
		result, debug, err := development.RunCheck(devContext, operation.CheckParameters.Resource, operation.CheckParameters.Subject)
		if err != nil {
			devErr, wireErr := development.DistinguishGraphError(
				devContext,
				err,
				devinterface.DeveloperError_CHECK_WATCH,
				0, 0,
				tuple.MustString(&core.RelationTuple{
					ResourceAndRelation: operation.CheckParameters.Resource,
					Subject:             operation.CheckParameters.Subject,
				}),
			)
			if wireErr != nil {
				return nil, wireErr
			}

			return &devinterface.OperationResult{
				CheckResult: &devinterface.CheckOperationsResult{
					CheckError: devErr,
				},
			}, nil
		}

		// TODO(jschorr): Support caveats here.
		membership := devinterface.CheckOperationsResult_NOT_MEMBER
		if result == v1.ResourceCheckResult_MEMBER {
			membership = devinterface.CheckOperationsResult_MEMBER
		}

		return &devinterface.OperationResult{
			CheckResult: &devinterface.CheckOperationsResult{
				Membership:       membership,
				DebugInformation: debug,
			},
		}, nil

	case operation.AssertionsParameters != nil:
		assertions, devErr := development.ParseAssertionsYAML(operation.AssertionsParameters.AssertionsYaml)
		if devErr != nil {
			return &devinterface.OperationResult{
				AssertionsResult: &devinterface.RunAssertionsResult{
					InputError: devErr,
				},
			}, nil
		}

		validationErrors, err := development.RunAllAssertions(devContext, assertions)
		if err != nil {
			return nil, err
		}

		return &devinterface.OperationResult{
			AssertionsResult: &devinterface.RunAssertionsResult{
				ValidationErrors: validationErrors,
			},
		}, nil

	case operation.ValidationParameters != nil:
		validation, devErr := development.ParseExpectedRelationsYAML(operation.ValidationParameters.ValidationYaml)
		if devErr != nil {
			return &devinterface.OperationResult{
				ValidationResult: &devinterface.RunValidationResult{
					InputError: devErr,
				},
			}, nil
		}

		membershipSet, validationErrors, err := development.RunValidation(devContext, validation)
		if err != nil {
			return nil, err
		}

		updatedValidationYaml := ""
		if membershipSet != nil {
			generatedValidationYaml, gerr := development.GenerateValidation(membershipSet)
			if gerr != nil {
				return nil, gerr
			}
			updatedValidationYaml = generatedValidationYaml
		}

		return &devinterface.OperationResult{
			ValidationResult: &devinterface.RunValidationResult{
				UpdatedValidationYaml: updatedValidationYaml,
				ValidationErrors:      validationErrors,
			},
		}, nil

	default:
		return nil, fmt.Errorf("unknown operation")
	}
}
