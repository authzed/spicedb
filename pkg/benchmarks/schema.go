package benchmarks

import (
	"context"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	schema "github.com/authzed/spicedb/pkg/schema/v2"
)

// ReadSchema reads all namespace and caveat definitions from the datastore at
// the given revision and returns the compiled schema.
func ReadSchema(ctx context.Context, ds datastore.Datastore, rev datastore.Revision) (*schema.Schema, error) {
	reader := ds.SnapshotReader(rev)

	nsDefs, err := reader.LegacyListAllNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	caveatDefs, err := reader.LegacyListAllCaveats(ctx)
	if err != nil {
		return nil, err
	}

	objectDefs := make([]*corev1.NamespaceDefinition, len(nsDefs))
	for i, ns := range nsDefs {
		objectDefs[i] = ns.Definition
	}

	caveatProtos := make([]*corev1.CaveatDefinition, len(caveatDefs))
	for i, c := range caveatDefs {
		caveatProtos[i] = c.Definition
	}

	return schema.BuildSchemaFromDefinitions(objectDefs, caveatProtos)
}
