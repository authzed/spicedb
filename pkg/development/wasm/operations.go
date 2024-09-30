//go:build wasm
// +build wasm

package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/authzed/spicedb/pkg/development"
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
		var caveatContext map[string]any
		if operation.CheckParameters.CaveatContext != nil {
			caveatContext = operation.CheckParameters.CaveatContext.AsMap()
		}

		cr, err := development.RunCheck(
			devContext,
			tuple.FromCoreObjectAndRelation(operation.CheckParameters.Resource),
			tuple.FromCoreObjectAndRelation(operation.CheckParameters.Subject),
			caveatContext,
		)
		if err != nil {
			devErr, wireErr := development.DistinguishGraphError(
				devContext,
				err,
				devinterface.DeveloperError_CHECK_WATCH,
				0, 0,
				tuple.MustString(tuple.Relationship{
					RelationshipReference: tuple.RelationshipReference{
						Resource: tuple.FromCoreObjectAndRelation(operation.CheckParameters.Resource),
						Subject:  tuple.FromCoreObjectAndRelation(operation.CheckParameters.Subject),
					},
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

		membership := devinterface.CheckOperationsResult_NOT_MEMBER
		if cr.Permissionship == v1.ResourceCheckResult_MEMBER {
			membership = devinterface.CheckOperationsResult_MEMBER
		} else if cr.Permissionship == v1.ResourceCheckResult_CAVEATED_MEMBER {
			membership = devinterface.CheckOperationsResult_CAVEATED_MEMBER
		}

		return &devinterface.OperationResult{
			CheckResult: &devinterface.CheckOperationsResult{
				Membership:               membership,
				DebugInformation:         cr.DispatchDebugInfo,
				ResolvedDebugInformation: cr.V1DebugInfo,
				PartialCaveatInfo: &devinterface.PartialCaveatInfo{
					MissingRequiredContext: cr.MissingCaveatFields,
				},
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

	case operation.SchemaWarningsParameters != nil:
		warnings, err := development.GetWarnings(context.Background(), devContext)
		if err != nil {
			return nil, err
		}

		return &devinterface.OperationResult{
			SchemaWarningsResult: &devinterface.SchemaWarningsResult{
				Warnings: warnings,
			},
		}, nil

	default:
		return nil, fmt.Errorf("unknown operation")
	}
}
