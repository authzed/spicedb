package types

import "reflect"

// subtree reports whether every key in map0 is present in map1 with a matching
// value: nested maps match when map0's is a subtree of map1's, and all other
// values match when they are deeply equal.
//
// reflect.DeepEqual is used for the leaf comparison instead of the `!=`
// operator because caveat context values may be slices (CEL list values arrive
// as []any). Comparing uncomparable types such as a slice with `!=` panics at
// runtime, which previously turned a valid `map.isSubtreeOf(...)` evaluation
// into an internal error. DeepEqual handles slices, scalars and nested maps
// without panicking, and for scalars it matches the previous `==` semantics.
//
// Lists compare by exact value: element order is significant, and a map nested
// inside a list is compared element-by-element, NOT walked as a subtree. Only a
// map nested directly as a value is matched recursively as a subtree.
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
		} else if !reflect.DeepEqual(v, val) {
			return false
		}
	}
	return true
}
