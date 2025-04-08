package types

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
