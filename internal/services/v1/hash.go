package v1

import (
	"strconv"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	structpb "github.com/golang/protobuf/ptypes/struct"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func computeCallHash(apiName string, consistency *v1.Consistency, arguments map[string]any) (string, error) {
	stringArguments := make(map[string]string, len(arguments)+1)

	if consistency == nil {
		consistency = &v1.Consistency{
			Requirement: &v1.Consistency_MinimizeLatency{
				MinimizeLatency: true,
			},
		}
	}

	consistencyBytes, err := consistency.MarshalVT()
	if err != nil {
		return "", err
	}

	stringArguments["consistency"] = string(consistencyBytes)

	for argName, argValue := range arguments {
		if argName == "consistency" {
			return "", spiceerrors.MustBugf("cannot specify consistency in the arguments")
		}

		switch v := argValue.(type) {
		case string:
			stringArguments[argName] = v

		case int:
			stringArguments[argName] = strconv.Itoa(v)

		case uint32:
			stringArguments[argName] = strconv.Itoa(int(v))

		case *structpb.Struct:
			bytes, err := v.MarshalJSON()
			if err != nil {
				return "", err
			}
			stringArguments[argName] = string(bytes)

		default:
			return "", spiceerrors.MustBugf("unknown argument type in compute call hash")
		}
	}
	return computeAPICallHash(apiName, stringArguments)
}
