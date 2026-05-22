package datastore

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// DefinitionsOf returns just the schema definitions found in the list of revisioned
// definitions.
func DefinitionsOf[T SchemaDefinition](revisionedDefinitions []RevisionedDefinition[T]) []T {
	definitions := make([]T, 0, len(revisionedDefinitions))
	for _, revDef := range revisionedDefinitions {
		definitions = append(definitions, revDef.Definition)
	}
	return definitions
}

// DeleteAllData deletes all data from the datastore. Should only be used when explicitly requested.
// The data is transactionally deleted, which means it may time out.
func DeleteAllData(ctx context.Context, ds Datastore) error {
	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		// Delete all relationships.
		typeDefs, err := rwt.LegacyListAllNamespaces(ctx)
		if err != nil {
			return err
		}
		namespaceNames := make([]string, 0, len(typeDefs))
		for _, typeDef := range typeDefs {
			_, _, err = rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
				ResourceType: typeDef.Definition.Name,
			})
			if err != nil {
				return err
			}
			namespaceNames = append(namespaceNames, typeDef.Definition.Name)
		}

		// Delete all caveats.
		caveatDefs, err := rwt.LegacyListAllCaveats(ctx)
		if err != nil {
			return err
		}
		caveatNames := make([]string, 0, len(caveatDefs))
		for _, caveatDef := range caveatDefs {
			caveatNames = append(caveatNames, caveatDef.Definition.Name)
		}

		if err := rwt.LegacyDeleteCaveats(ctx, caveatNames); err != nil {
			return err
		}

		// Delete all namespaces.
		if err := rwt.LegacyDeleteNamespaces(ctx, namespaceNames, DeleteNamespacesAndRelationships); err != nil {
			return err
		}

		return nil
	})
	return err
}
