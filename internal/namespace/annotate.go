package namespace

import "github.com/authzed/spicedb/pkg/schema"

// AnnotateNamespace annotates the namespace in the type system with computed aliasing and cache key
// metadata for more efficient dispatching.
func AnnotateNamespace(def *schema.ValidatedDefinition) error {
	aliases, aerr := computePermissionAliases(def)
	if aerr != nil {
		return aerr
	}

	cacheKeys, cerr := computeCanonicalCacheKeys(def, aliases)
	if cerr != nil {
		return cerr
	}

	for _, rel := range def.Namespace().Relation {
		if alias, ok := aliases[rel.Name]; ok {
			rel.AliasingRelation = alias
		}

		if cacheKey, ok := cacheKeys[rel.Name]; ok {
			rel.CanonicalCacheKey = cacheKey
		}
	}

	return nil
}
