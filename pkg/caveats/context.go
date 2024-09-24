package caveats

import (
	"maps"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/caveats/types"
)

// ConvertContextToStruct converts the given context values into a context struct.
func ConvertContextToStruct(contextValues map[string]any) (*structpb.Struct, error) {
	cloned := maps.Clone(contextValues)
	cloned = convertCustomValues(cloned).(map[string]any)
	return structpb.NewStruct(cloned)
}

func convertCustomValues(value any) any {
	switch v := value.(type) {
	case map[string]any:
		for key, value := range v {
			v[key] = convertCustomValues(value)
		}
		return v

	case []any:
		for index, current := range v {
			v[index] = convertCustomValues(current)
		}
		return v

	case time.Time:
		return v.Format(time.RFC3339)

	case time.Duration:
		return v.String()

	case types.CustomType:
		return v.SerializedString()

	default:
		return v
	}
}
