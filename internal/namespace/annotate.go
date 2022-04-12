package namespace

// AnnotateNamespace annotates the namespace in the type system with computed aliasing and cache key
// metadata for more efficient dispatching.
func AnnotateNamespace(ts *ValidatedNamespaceTypeSystem) error {
	aliases, aerr := computePermissionAliases(ts)
	if aerr != nil {
		return aerr
	}

	cacheKeys, cerr := computeCanonicalCacheKeys(ts, aliases)
	if cerr != nil {
		return cerr
	}

	for _, rel := range ts.nsDef.Relation {
		if alias, ok := aliases[rel.Name]; ok {
			rel.AliasingRelation = alias
		}

		if cacheKey, ok := cacheKeys[rel.Name]; ok {
			rel.CanonicalCacheKey = cacheKey
		}
	}

	return nil
}
