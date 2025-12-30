package schema

import (
	"context"
	"errors"
	"fmt"

	"github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
)

// LegacySchemaReaderAdapter is a common implementation of SchemaReader that uses the legacy
// schema reader methods. This allows datastores to implement the new SchemaReader interface
// while still using the legacy methods internally during the transition period.
type LegacySchemaReaderAdapter struct {
	legacyReader datastore.LegacySchemaReader
}

// NewLegacySchemaReaderAdapter creates a new LegacySchemaReaderAdapter that wraps a LegacySchemaReader.
func NewLegacySchemaReaderAdapter(legacyReader datastore.LegacySchemaReader) *LegacySchemaReaderAdapter {
	return &LegacySchemaReaderAdapter{
		legacyReader: legacyReader,
	}
}

// SchemaText returns the schema text at the current revision by reading all namespaces and caveats
// and generating the schema text from them.
func (l *LegacySchemaReaderAdapter) SchemaText() (string, error) {
	ctx := context.Background()

	// Read all namespaces
	namespaces, err := l.ListAllTypeDefinitions(ctx)
	if err != nil {
		return "", err
	}

	// Read all caveats
	caveats, err := l.ListAllCaveatDefinitions(ctx)
	if err != nil {
		return "", err
	}

	// Check if there is any schema defined
	if len(namespaces) == 0 {
		return "", datastore.NewSchemaNotDefinedErr()
	}

	// Build a list of all schema definitions
	caveatTypeSet := types.Default.TypeSet
	definitions := make([]compiler.SchemaDefinition, 0, len(caveats)+len(namespaces))
	for _, caveat := range caveats {
		definitions = append(definitions, caveat.Definition)
	}
	for _, ns := range namespaces {
		definitions = append(definitions, ns.Definition)
	}

	// Generate schema text with proper use directives
	schemaText, _, err := generator.GenerateSchemaWithCaveatTypeSet(definitions, caveatTypeSet)
	if err != nil {
		return "", fmt.Errorf("failed to generate schema: %w", err)
	}

	return schemaText, nil
}

// LookupTypeDefByName looks up a type definition (namespace) by name.
func (l *LegacySchemaReaderAdapter) LookupTypeDefByName(ctx context.Context, name string) (datastore.RevisionedTypeDefinition, bool, error) {
	ns, revision, err := l.legacyReader.LegacyReadNamespaceByName(ctx, name)
	if err != nil {
		// Check if it's a not found error
		if errors.As(err, &datastore.NamespaceNotFoundError{}) {
			return datastore.RevisionedTypeDefinition{}, false, nil
		}
		return datastore.RevisionedTypeDefinition{}, false, err
	}

	return datastore.RevisionedTypeDefinition{
		Definition:          ns,
		LastWrittenRevision: revision,
	}, true, nil
}

// LookupCaveatDefByName looks up a caveat definition by name.
func (l *LegacySchemaReaderAdapter) LookupCaveatDefByName(ctx context.Context, name string) (datastore.RevisionedCaveat, bool, error) {
	caveat, revision, err := l.legacyReader.LegacyReadCaveatByName(ctx, name)
	if err != nil {
		// Check if it's a not found error
		if errors.As(err, &datastore.CaveatNameNotFoundError{}) {
			return datastore.RevisionedCaveat{}, false, nil
		}
		return datastore.RevisionedCaveat{}, false, err
	}

	return datastore.RevisionedCaveat{
		Definition:          caveat,
		LastWrittenRevision: revision,
	}, true, nil
}

// ListAllTypeDefinitions lists all type definitions (namespaces).
func (l *LegacySchemaReaderAdapter) ListAllTypeDefinitions(ctx context.Context) ([]datastore.RevisionedTypeDefinition, error) {
	namespaces, err := l.legacyReader.LegacyListAllNamespaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}
	return namespaces, nil
}

// ListAllCaveatDefinitions lists all caveat definitions.
func (l *LegacySchemaReaderAdapter) ListAllCaveatDefinitions(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	caveats, err := l.legacyReader.LegacyListAllCaveats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list caveats: %w", err)
	}
	return caveats, nil
}

// ListAllSchemaDefinitions lists all type and caveat definitions.
func (l *LegacySchemaReaderAdapter) ListAllSchemaDefinitions(ctx context.Context) (map[string]datastore.SchemaDefinition, error) {
	result := make(map[string]datastore.SchemaDefinition)

	// Read all namespaces
	namespaces, err := l.ListAllTypeDefinitions(ctx)
	if err != nil {
		return nil, err
	}

	for _, ns := range namespaces {
		result[ns.Definition.Name] = ns.Definition
	}

	// Read all caveats
	caveats, err := l.ListAllCaveatDefinitions(ctx)
	if err != nil {
		return nil, err
	}

	for _, caveat := range caveats {
		result[caveat.Definition.Name] = caveat.Definition
	}

	return result, nil
}

// LookupSchemaDefinitionsByNames looks up type and caveat definitions by name.
func (l *LegacySchemaReaderAdapter) LookupSchemaDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.SchemaDefinition, error) {
	result := make(map[string]datastore.SchemaDefinition)

	// Try to look up as namespaces
	namespaces, err := l.legacyReader.LegacyLookupNamespacesWithNames(ctx, names)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup namespaces: %w", err)
	}

	for _, ns := range namespaces {
		result[ns.Definition.Name] = ns.Definition
	}

	// Try to look up as caveats
	caveats, err := l.legacyReader.LegacyLookupCaveatsWithNames(ctx, names)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup caveats: %w", err)
	}

	for _, caveat := range caveats {
		result[caveat.Definition.Name] = caveat.Definition
	}

	return result, nil
}

var _ datastore.SchemaReader = (*LegacySchemaReaderAdapter)(nil)
