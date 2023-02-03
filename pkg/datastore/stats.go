package datastore

import (
	"github.com/authzed/spicedb/pkg/namespace"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
)

// ComputeObjectTypeStats creates a list of object type stats from an input list of
// parsed object types.
func ComputeObjectTypeStats(objTypes []RevisionedNamespace) []ObjectTypeStat {
	stats := make([]ObjectTypeStat, 0, len(objTypes))

	for _, objType := range objTypes {
		var relations, permissions uint32

		for _, rel := range objType.Definition.Relation {
			if namespace.GetRelationKind(rel) == iv1.RelationMetadata_PERMISSION {
				permissions++
			} else {
				relations++
			}
		}

		stats = append(stats, ObjectTypeStat{
			NumRelations:   relations,
			NumPermissions: permissions,
		})
	}

	return stats
}
