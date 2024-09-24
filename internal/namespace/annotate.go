package namespace

import "github.com/authzed/spicedb/pkg/typesystem"

// AnnotateNamespace annotates the namespace in the type system with computed aliasing and cache key
// metadata for more efficient dispatching.
func AnnotateNamespace(ts *typesystem.ValidatedNamespaceTypeSystem) error {
	aliases, aerr := computePermissionAliases(ts)
	if aerr != nil {
		return aerr
	}

	cacheKeys, cerr := computeCanonicalCacheKeys(ts, aliases)
	if cerr != nil {
		return cerr
	}

	for _, rel := range ts.Namespace().Relation {
		if alias, ok := aliases[rel.Name]; ok {
			rel.AliasingRelation = alias
		}

		if cacheKey, ok := cacheKeys[rel.Name]; ok {
			rel.CanonicalCacheKey = cacheKey
		}
	}

	return nil
}
