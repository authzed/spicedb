package v1

import (
	"context"

	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/diff"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func schemaDiff(ctx context.Context, comparisonSchemaString string) (*diff.SchemaDiff, *diff.DiffableSchema, *diff.DiffableSchema, error) {
	ds := datastoremw.MustFromContext(ctx)

	// Compile the comparison schema.
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: comparisonSchemaString,
	}, compiler.AllowUnprefixedObjectType())
	if err != nil {
		return nil, nil, nil, err
	}

	// Get the schema at the requested revision.
	atRevision, _, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	reader := ds.SnapshotReader(atRevision)

	namespacesAndRevs, err := reader.ListAllNamespaces(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	caveatsAndRevs, err := reader.ListAllCaveats(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	namespaces := make([]*core.NamespaceDefinition, 0, len(namespacesAndRevs))
	for _, namespaceAndRev := range namespacesAndRevs {
		namespaces = append(namespaces, namespaceAndRev.Definition)
	}

	caveats := make([]*core.CaveatDefinition, 0, len(caveatsAndRevs))
	for _, caveatAndRev := range caveatsAndRevs {
		caveats = append(caveats, caveatAndRev.Definition)
	}

	// Perform the diff.
	existingSchema := diff.DiffableSchema{
		ObjectDefinitions: namespaces,
		CaveatDefinitions: caveats,
	}
	comparisonSchema := diff.NewDiffableSchemaFromCompiledSchema(compiled)

	diff, err := diff.DiffSchemas(existingSchema, comparisonSchema)
	if err != nil {
		return nil, nil, nil, err
	}

	// Return the diff.
	return diff, &existingSchema, &comparisonSchema, nil
}
