package query

import "slices"

// ownedPath copies mutable state retained by a reusable fixed iterator. Reducers
// and alias projection may change returned paths, so each execution owns its
// caveat expressions, integrity messages, excluded paths and metadata containers.
func ownedPath(source *Path) *Path {
	if source == nil {
		return nil
	}

	path := *source

	if source.Caveat != nil {
		path.Caveat = source.Caveat.CloneVT()
	}

	if source.Expiration != nil {
		expiration := *source.Expiration
		path.Expiration = &expiration
	}

	path.Integrity = slices.Clone(source.Integrity)

	for index, integrity := range source.Integrity {
		if integrity != nil {
			path.Integrity[index] = integrity.CloneVT()
		}
	}

	path.ExcludedSubjects = slices.Clone(source.ExcludedSubjects)

	for index, excluded := range source.ExcludedSubjects {
		path.ExcludedSubjects[index] = ownedPath(excluded)
	}

	if source.Metadata != nil {
		path.Metadata = ownedMetadata(source.Metadata).(map[string]any)
	}

	return &path
}

// Metadata uses JSON-shaped values at the serialization boundary. Clone its
// mutable containers without changing scalar types. Opaque values are immutable
// by contract and are never modified by query reducers or projections.
func ownedMetadata(value any) any {
	switch value := value.(type) {
	case map[string]any:
		cloned := make(map[string]any, len(value))

		for key, item := range value {
			cloned[key] = ownedMetadata(item)
		}

		return cloned
	case []any:
		cloned := make([]any, len(value))

		for index, item := range value {
			cloned[index] = ownedMetadata(item)
		}

		return cloned
	case []string:
		return slices.Clone(value)
	case []byte:
		return slices.Clone(value)
	default:
		return value
	}
}
