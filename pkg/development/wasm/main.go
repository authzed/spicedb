//go:build wasm
// +build wasm

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"syscall/js"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/authzed/spicedb/pkg/development"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
)

func rerr(message string) interface{} {
	return js.ValueOf([]interface{}{nil, message})
}

func derr(derrs *development.DeveloperErrors) interface{} {
	marshalled, err := json.Marshal(derrs)
	if err != nil {
		return rerr(err.Error())
	}

	return js.ValueOf([]interface{}{string(marshalled), nil})
}

// runDevelopmentOperations is the function exported into the WASM environment for invoking
// one or more development operations.
//
// The arguments are:
//
//  1. JSON-string form of the development context:
//     {
//     "schema": "definition user {}"
//     "relationships": [
//     {
//     "resource_and_relation": {"namespace": "ns", "object_id": "oid", "relation": "viewer"},
//     "subject": {... same fields as resource_and_relation ...}
//     }
//     ]
//     }
//
//  2. An array (*not* JSON encoded) of operations to be run by the package:
//     [
//     {
//     "operation": "check",
//     "parameters": "(json string with parameters for operation)",
//     "callback": callback: (result, err) => { ... },
//     }
//     ]
//
// See example/wasm.html for an example and the individual operations for their parameters
// and callback arguments.
func runDevelopmentOperations(this js.Value, args []js.Value) interface{} {
	if len(args) != 2 {
		return rerr("invalid number of arguments specified")
	}

	devContext, devErrors, err := newDevContextFromJSON(args[0].String())
	if err != nil {
		return rerr(err.Error())
	}

	if devErrors != nil && (len(devErrors.InputErrors) > 0 || len(devErrors.ValidationErrors) > 0) {
		return derr(devErrors)
	}

	for index := 0; index < args[1].Length(); index++ {
		err := runOperation(devContext, args[1].Get(strconv.Itoa(index)))
		if err != nil {
			return rerr(err.Error())
		}
	}

	return js.ValueOf([]interface{}{nil, nil})
}

func newDevContextFromJSON(encoded string) (*development.DevContext, *development.DeveloperErrors, error) {
	reqContext := &devinterface.RequestContext{}
	err := protojson.Unmarshal([]byte(encoded), reqContext)
	if err != nil {
		return nil, nil, err
	}
	return development.NewDevContext(context.Background(), reqContext)
}

func main() {
	c := make(chan struct{}, 0)
	js.Global().Set("runSpiceDBDevelopmentOperations", js.FuncOf(runDevelopmentOperations))
	fmt.Println("Development interface initialized")
	<-c
}
