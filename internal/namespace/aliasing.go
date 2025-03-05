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

	for _, rel := range typeDefinition.Namespace().Relation {
		// Ensure the relation has a rewrite...
		if rel.GetUsersetRewrite() == nil {
			done[rel.Name] = struct{}{}
			continue
		}

		// ... with a union ...
		union := rel.GetUsersetRewrite().GetUnion()
		if union == nil {
			done[rel.Name] = struct{}{}
			continue
		}

		// ... with a single child ...
		if len(union.Child) != 1 {
			done[rel.Name] = struct{}{}
			continue
		}

		// ... that is a computed userset.
		computedUserset := union.Child[0].GetComputedUserset()
		if computedUserset == nil {
			done[rel.Name] = struct{}{}
			continue
		}

		// If the aliased item is a relation, then we've found the alias target.
		aliasedPermOrRel := computedUserset.GetRelation()
		if !typeDefinition.IsPermission(aliasedPermOrRel) {
			done[rel.Name] = struct{}{}
			aliases[rel.Name] = aliasedPermOrRel
			continue
		}

		// Otherwise, add the permission to the working set.
		unresolvedAliases[rel.Name] = aliasedPermOrRel
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
			return nil, NewPermissionsCycleErr(typeDefinition.Namespace().Name, keys)
		}
	}

	return aliases, nil
}
