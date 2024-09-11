//go:build wasm
// +build wasm

package main

import (
	"context"
	"fmt"
	"syscall/js"

	"github.com/authzed/spicedb/pkg/development"

	"google.golang.org/protobuf/encoding/protojson"

	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
)

// runDeveloperRequest is the function exported into the WASM environment for invoking
// one or more development operations.
//
// The arguments are:
//
//  1. Message in the form of a DeveloperRequest containing the context and the operation(s) to
//     run, in JSON form.
//
// The function returns:
//
//		A single JSON-encoded string of messages of type DeveloperResponse representing the errors
//	 encountered (if any), or the responses for each operation.
//
// See example/wasm.html for an example.
func runDeveloperRequest(this js.Value, args []js.Value) any {
	if len(args) != 1 {
		return respErr(fmt.Errorf("invalid number of arguments specified"))
	}

	// Unmarshal the developer request.
	encoded := args[0].String()
	devRequest := &devinterface.DeveloperRequest{}
	err := protojson.Unmarshal([]byte(encoded), devRequest)
	if err != nil {
		return respErr(fmt.Errorf("could not decode developer request: %w", err))
	}

	if devRequest.Context == nil {
		return respErr(fmt.Errorf("missing required context"))
	}

	// Construct the developer context.
	devContext, devErrors, err := development.NewDevContext(context.Background(), devRequest.Context)
	if err != nil {
		return respErr(err)
	}

	if devErrors != nil && len(devErrors.InputErrors) > 0 {
		return respInputErr(devErrors.InputErrors)
	}

	// Run operations.
	results := make(map[uint64]*devinterface.OperationResult, len(devRequest.Operations))
	for index, op := range devRequest.Operations {
		result, err := runOperation(devContext, op)
		if err != nil {
			return respErr(err)
		}

		results[uint64(index)] = result
	}

	return encode(&devinterface.DeveloperResponse{
		OperationsResults: &devinterface.OperationsResults{
			Results: results,
		},
	})
}

func encode(response *devinterface.DeveloperResponse) js.Value {
	encoded, err := protojson.Marshal(response)
	if err != nil {
		panic(err)
	}

	return js.ValueOf(string(encoded))
}

func respErr(err error) js.Value {
	return encode(&devinterface.DeveloperResponse{
		InternalError: err.Error(),
	})
}

func respInputErr(inputErrors []*devinterface.DeveloperError) js.Value {
	return encode(&devinterface.DeveloperResponse{
		DeveloperErrors: &devinterface.DeveloperErrors{
			InputErrors: inputErrors,
		},
	})
}
