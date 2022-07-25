//go:build wasm
// +build wasm

package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"syscall/js"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/authzed/spicedb/pkg/development"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
)

func runOperation(devContext *development.DevContext, op js.Value) error {
	operation := op.Get("operation").String()
	parameters := op.Get("parameters").String()
	callback := op.Get("callback")

	switch operation {
	case "formatSchema":
		formatted := ""
		for _, nsDef := range devContext.Namespaces {
			source, _ := generator.GenerateSource(nsDef)
			formatted += source
			formatted += "\n\n"
		}

		trimmed := strings.TrimSpace(formatted)
		callback.Invoke(trimmed)

	case "check":
		params := &devinterface.CheckOperationParameters{}
		err := protojson.Unmarshal([]byte(parameters), params)
		if err != nil {
			return err
		}

		result, err := development.RunCheck(devContext, params.Resource, params.Subject)
		if err != nil {
			callback.Invoke(nil, err.Error())
		} else {
			callback.Invoke(result == v1.DispatchCheckResponse_MEMBER, nil)
		}

	case "runAssertions":
		params := &devinterface.RunAssertionsParameters{}
		err := protojson.Unmarshal([]byte(parameters), params)
		if err != nil {
			return err
		}

		assertions, devErr := development.ParseAssertionsYAML(params.AssertionsYaml)
		if devErr != nil {
			encoded, err := encodeDevError(devErr)
			callback.Invoke(encoded, err)
			return nil
		}

		devErrs, err := development.RunAllAssertions(devContext, assertions)
		if err != nil {
			callback.Invoke(nil, err.Error())
			return nil
		} else if devErrs != nil {
			encoded, err := encodeDevErrors(devErrs)
			callback.Invoke(encoded, err)
			return nil
		}

		callback.Invoke(nil, nil)

	case "runValidation":
		params := &devinterface.RunValidationParameters{}
		err := protojson.Unmarshal([]byte(parameters), params)
		if err != nil {
			return err
		}

		validation, devErr := development.ParseExpectedRelationsYAML(params.ValidationYaml)
		if devErr != nil {
			encoded, err := encodeDevError(devErr)
			callback.Invoke(nil, encoded, err)
			return nil
		}

		membershipSet, devErrs, err := development.RunValidation(devContext, validation)

		updatedValidationYaml := ""
		if membershipSet != nil {
			generatedValidationYaml, gerr := development.GenerateValidation(membershipSet)
			if gerr != nil {
				return gerr
			}
			updatedValidationYaml = generatedValidationYaml
		}

		if err != nil {
			callback.Invoke(updatedValidationYaml, nil, err.Error())
			return nil
		} else if devErrs != nil {
			encoded, err := encodeDevErrors(devErrs)
			callback.Invoke(updatedValidationYaml, encoded, err)
			return nil
		}

		callback.Invoke(updatedValidationYaml, nil, nil)
	default:
		return fmt.Errorf("unknown operation: `%v`", operation)
	}

	return nil
}

func encodeDevErrors(devErrs *development.DeveloperErrors) (interface{}, error) {
	marshalled, err := json.Marshal(devErrs)
	if err != nil {
		return nil, err
	}

	return string(marshalled), nil
}

func encodeDevError(devErr *devinterface.DeveloperError) (interface{}, error) {
	return encodeDevErrors(&development.DeveloperErrors{
		ValidationErrors: []*devinterface.DeveloperError{devErr},
	})
}
