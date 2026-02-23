package v1

import (
	"context"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/diff"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func loadCurrentSchema(ctx context.Context) (*diff.DiffableSchema, datastore.Revision, error) {
	dl := datalayer.MustFromContext(ctx)

	atRevision, _, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	reader := dl.SnapshotReader(atRevision)

	sr, err := reader.ReadSchema()
	if err != nil {
		return nil, atRevision, err
	}

	namespacesAndRevs, err := sr.ListAllTypeDefinitions(ctx)
	if err != nil {
		return nil, atRevision, err
	}

	caveatsAndRevs, err := sr.ListAllCaveatDefinitions(ctx)
	if err != nil {
		return nil, atRevision, err
	}

	namespaces := make([]*core.NamespaceDefinition, 0, len(namespacesAndRevs))
	for _, namespaceAndRev := range namespacesAndRevs {
		namespaces = append(namespaces, namespaceAndRev.Definition)
	}

	caveats := make([]*core.CaveatDefinition, 0, len(caveatsAndRevs))
	for _, caveatAndRev := range caveatsAndRevs {
		caveats = append(caveats, caveatAndRev.Definition)
	}

	return &diff.DiffableSchema{
		ObjectDefinitions: namespaces,
		CaveatDefinitions: caveats,
	}, atRevision, nil
}

func schemaDiff(ctx context.Context, comparisonSchemaString string, caveatTypeSet *caveattypes.TypeSet) (*diff.SchemaDiff, *diff.DiffableSchema, *diff.DiffableSchema, error) {
	existingSchema, _, err := loadCurrentSchema(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	// Compile the comparison schema.
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: comparisonSchemaString,
	}, compiler.AllowUnprefixedObjectType(), compiler.CaveatTypeSet(caveatTypeSet))
	if err != nil {
		return nil, nil, nil, err
	}

	comparisonSchema := diff.NewDiffableSchemaFromCompiledSchema(compiled)

	diff, err := diff.DiffSchemas(*existingSchema, comparisonSchema, caveatTypeSet)
	if err != nil {
		return nil, nil, nil, err
	}

	// Return the diff.
	return diff, existingSchema, &comparisonSchema, nil
}
