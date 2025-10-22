package namespace

import (
	"sort"

	"github.com/authzed/spicedb/pkg/schema"
)

// computePermissionAliases computes a map of aliases between the various permissions in a
// namespace. A permission is considered an alias if it *directly* refers to another permission
// or relation without any other form of expression.
func computePermissionAliases(typeDefinition *schema.ValidatedDefinition) (map[string]string, error) {
	aliases := map[string]string{}
	done := map[string]struct{}{}
	unresolvedAliases := map[string]string{}

	for _, rel := range typeDefinition.Namespace().GetRelation() {
		// Ensure the relation has a rewrite...
		if rel.GetUsersetRewrite() == nil {
			done[rel.GetName()] = struct{}{}
			continue
		}

		// ... with a union ...
		union := rel.GetUsersetRewrite().GetUnion()
		if union == nil {
			done[rel.GetName()] = struct{}{}
			continue
		}

		// ... with a single child ...
		if len(union.GetChild()) != 1 {
			done[rel.GetName()] = struct{}{}
			continue
		}

		// ... that is a computed userset.
		computedUserset := union.GetChild()[0].GetComputedUserset()
		if computedUserset == nil {
			done[rel.GetName()] = struct{}{}
			continue
		}

		// If the aliased item is a relation, then we've found the alias target.
		aliasedPermOrRel := computedUserset.GetRelation()
		if !typeDefinition.IsPermission(aliasedPermOrRel) {
			done[rel.GetName()] = struct{}{}
			aliases[rel.GetName()] = aliasedPermOrRel
			continue
		}

		// Otherwise, add the permission to the working set.
		unresolvedAliases[rel.GetName()] = aliasedPermOrRel
	}

	for len(unresolvedAliases) > 0 {
		startingCount := len(unresolvedAliases)
		for relName, aliasedPermission := range unresolvedAliases {
			if _, ok := done[aliasedPermission]; ok {
				done[relName] = struct{}{}

				if alias, ok := aliases[aliasedPermission]; ok {
					aliases[relName] = alias
				} else {
					aliases[relName] = aliasedPermission
				}
				delete(unresolvedAliases, relName)
				continue
			}
		}
		if len(unresolvedAliases) == startingCount {
			keys := make([]string, 0, len(unresolvedAliases))
			for key := range unresolvedAliases {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			return nil, NewPermissionsCycleErr(typeDefinition.Namespace().GetName(), keys)
		}
	}

	return aliases, nil
}
