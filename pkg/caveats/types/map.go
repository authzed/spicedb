package types

import (
	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/types"
	"github.com/authzed/cel-go/common/types/ref"
)

func init() {
	registerMethodOnDefinedType(cel.MapType(cel.StringType, cel.DynType),
		"isSubtreeOf",
		[]*cel.Type{cel.MapType(cel.StringType, cel.DynType)},
		cel.BoolType,
		func(arg ...ref.Val) ref.Val {
			map0 := arg[0].Value().(map[string]any)
			map1 := arg[1].Value().(map[string]any)
			return types.Bool(subtree(map0, map1))
		},
	)
}

func subtree(map0 map[string]any, map1 map[string]any) bool {
	for k, v := range map0 {
		val, ok := map1[k]
		if !ok {
			return false
		}
		nestedMap0, ok := v.(map[string]any)
		if ok {
			nestedMap1, ok := val.(map[string]any)
			if !ok {
				return false
			}
			nestedResult := subtree(nestedMap0, nestedMap1)
			if !nestedResult {
				return false
			}
		} else {
			if v != val {
				return false
			}
		}
	}
	return true
}
