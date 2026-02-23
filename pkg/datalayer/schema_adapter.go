package datalayer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// SchemaReaderFromLegacy returns a SchemaReader that adapts a datastore.LegacySchemaReader
// by calling Legacy* methods on the underlying reader.
func SchemaReaderFromLegacy(legacyReader datastore.LegacySchemaReader) SchemaReader {
	return &legacySchemaReaderAdapter{legacyReader: legacyReader}
}

// legacySchemaReaderAdapter implements SchemaReader by calling
// Legacy* methods on the underlying datastore.LegacySchemaReader.
type legacySchemaReaderAdapter struct {
	legacyReader datastore.LegacySchemaReader
}

// SchemaText returns the schema text at the current revision by reading all namespaces and caveats
// and generating the schema text from them.
func (l *legacySchemaReaderAdapter) SchemaText() (string, error) {
	ctx := context.Background()

	// Read all namespaces
	namespaces, err := l.ListAllTypeDefinitions(ctx)
	if err != nil {
		return "", err
	}

	// Check if there is any schema defined
	if len(namespaces) == 0 {
		return "", datastore.NewSchemaNotDefinedErr()
	}

	// Read all caveats
	caveats, err := l.ListAllCaveatDefinitions(ctx)
	if err != nil {
		return "", err
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
func (l *legacySchemaReaderAdapter) LookupTypeDefByName(ctx context.Context, name string) (datastore.RevisionedTypeDefinition, bool, error) {
	ns, revision, err := l.legacyReader.LegacyReadNamespaceByName(ctx, name)
	if err != nil {
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
func (l *legacySchemaReaderAdapter) LookupCaveatDefByName(ctx context.Context, name string) (datastore.RevisionedCaveat, bool, error) {
	caveat, revision, err := l.legacyReader.LegacyReadCaveatByName(ctx, name)
	if err != nil {
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
func (l *legacySchemaReaderAdapter) ListAllTypeDefinitions(ctx context.Context) ([]datastore.RevisionedTypeDefinition, error) {
	namespaces, err := l.legacyReader.LegacyListAllNamespaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}
	return namespaces, nil
}

// ListAllCaveatDefinitions lists all caveat definitions.
func (l *legacySchemaReaderAdapter) ListAllCaveatDefinitions(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	caveats, err := l.legacyReader.LegacyListAllCaveats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list caveats: %w", err)
	}
	return caveats, nil
}

// ListAllSchemaDefinitions lists all type and caveat definitions.
func (l *legacySchemaReaderAdapter) ListAllSchemaDefinitions(ctx context.Context) (map[string]datastore.SchemaDefinition, error) {
	result := make(map[string]datastore.SchemaDefinition)

	namespaces, err := l.ListAllTypeDefinitions(ctx)
	if err != nil {
		return nil, err
	}

	for _, ns := range namespaces {
		result[ns.Definition.Name] = ns.Definition
	}

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
func (l *legacySchemaReaderAdapter) LookupSchemaDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.SchemaDefinition, error) {
	nameSet := mapz.NewSet(names...)
	allDefs := make(map[string]datastore.SchemaDefinition, len(names))

	typeDefs, err := l.LookupTypeDefinitionsByNames(ctx, names)
	if err != nil {
		return nil, err
	}

	for name, def := range typeDefs {
		allDefs[name] = def
	}

	remainingNames := nameSet.Subtract(mapz.NewSet(slices.Collect(maps.Keys(typeDefs))...))
	caveatDefs, err := l.LookupCaveatDefinitionsByNames(ctx, remainingNames.AsSlice())
	if err != nil {
		return nil, err
	}

	for name, def := range caveatDefs {
		allDefs[name] = def
	}

	return allDefs, nil
}

// LookupTypeDefinitionsByNames looks up type definitions by name.
func (l *legacySchemaReaderAdapter) LookupTypeDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.TypeDefinition, error) {
	result := make(map[string]datastore.TypeDefinition)

	namespaces, err := l.legacyReader.LegacyLookupNamespacesWithNames(ctx, names)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup namespaces: %w", err)
	}

	for _, ns := range namespaces {
		result[ns.Definition.Name] = ns.Definition
	}
	return result, nil
}

// LookupCaveatDefinitionsByNames looks up caveat definitions by name.
func (l *legacySchemaReaderAdapter) LookupCaveatDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.CaveatDefinition, error) {
	result := make(map[string]datastore.CaveatDefinition)

	caveats, err := l.legacyReader.LegacyLookupCaveatsWithNames(ctx, names)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup caveats: %w", err)
	}

	for _, caveat := range caveats {
		result[caveat.Definition.Name] = caveat.Definition
	}

	return result, nil
}

var _ SchemaReader = (*legacySchemaReaderAdapter)(nil)

// writeSchemaViaLegacy implements the WriteSchema logic using Legacy* methods.
func writeSchemaViaLegacy(ctx context.Context, legacyWriter datastore.LegacySchemaWriter,
	legacyReader datastore.LegacySchemaReader, definitions []datastore.SchemaDefinition,
	schemaString string, caveatTypeSet *types.TypeSet,
) error {
	// Validate that no definition names overlap
	nameSet := mapz.NewSet[string]()
	var namespaces []*core.NamespaceDefinition
	var caveats []*core.CaveatDefinition

	for _, def := range definitions {
		name := def.GetName()
		if nameSet.Has(name) {
			return fmt.Errorf("duplicate definition name: %s", name)
		}
		nameSet.Insert(name)

		switch typedDef := def.(type) {
		case *core.NamespaceDefinition:
			namespaces = append(namespaces, typedDef)
		case *core.CaveatDefinition:
			caveats = append(caveats, typedDef)
		default:
			return spiceerrors.MustBugf("unknown definition type: %T", def)
		}
	}

	// Load existing type and caveat names
	existingTypeDefs, err := legacyReader.LegacyListAllNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("failed to list existing namespaces: %w", err)
	}

	existingCaveatDefs, err := legacyReader.LegacyListAllCaveats(ctx)
	if err != nil {
		return fmt.Errorf("failed to list existing caveats: %w", err)
	}

	// Build maps of existing definitions for comparison
	existingTypeMap := make(map[string]*core.NamespaceDefinition, len(existingTypeDefs))
	existingTypeNames := mapz.NewSet[string]()
	for _, typeDef := range existingTypeDefs {
		existingTypeMap[typeDef.Definition.Name] = typeDef.Definition
		existingTypeNames.Insert(typeDef.Definition.Name)
	}

	existingCaveatMap := make(map[string]*core.CaveatDefinition, len(existingCaveatDefs))
	existingCaveatNames := mapz.NewSet[string]()
	for _, caveatDef := range existingCaveatDefs {
		existingCaveatMap[caveatDef.Definition.Name] = caveatDef.Definition
		existingCaveatNames.Insert(caveatDef.Definition.Name)
	}

	// Filter namespaces to only those that are new or changed
	var namespacesToWrite []*core.NamespaceDefinition
	newTypeNames := mapz.NewSet[string]()
	for _, ns := range namespaces {
		newTypeNames.Insert(ns.Name)
		existing, exists := existingTypeMap[ns.Name]
		if !exists || !ns.EqualVT(existing) {
			namespacesToWrite = append(namespacesToWrite, ns)
		}
	}

	// Filter caveats to only those that are new or changed
	var caveatsToWrite []*core.CaveatDefinition
	newCaveatNames := mapz.NewSet[string]()
	for _, caveat := range caveats {
		newCaveatNames.Insert(caveat.Name)
		existing, exists := existingCaveatMap[caveat.Name]
		if !exists || !caveat.EqualVT(existing) {
			caveatsToWrite = append(caveatsToWrite, caveat)
		}
	}

	// Write namespaces (only new and changed)
	if len(namespacesToWrite) > 0 {
		if err := legacyWriter.LegacyWriteNamespaces(ctx, namespacesToWrite...); err != nil {
			return fmt.Errorf("failed to write namespaces: %w", err)
		}
	}

	// Write caveats (only new and changed)
	if len(caveatsToWrite) > 0 {
		if err := legacyWriter.LegacyWriteCaveats(ctx, caveatsToWrite); err != nil {
			return fmt.Errorf("failed to write caveats: %w", err)
		}
	}

	// Delete removed namespaces
	removedTypeNames := existingTypeNames.Subtract(newTypeNames)
	if removedTypeNames.Len() > 0 {
		if err := legacyWriter.LegacyDeleteNamespaces(ctx, removedTypeNames.AsSlice(), datastore.DeleteNamespacesOnly); err != nil {
			return fmt.Errorf("failed to delete removed namespaces: %w", err)
		}
	}

	// Delete removed caveats
	removedCaveatNames := existingCaveatNames.Subtract(newCaveatNames)
	if removedCaveatNames.Len() > 0 {
		if err := legacyWriter.LegacyDeleteCaveats(ctx, removedCaveatNames.AsSlice()); err != nil {
			return fmt.Errorf("failed to delete removed caveats: %w", err)
		}
	}

	return nil
}

// addDefinitionsForTestingViaLegacy implements AddDefinitionsForTesting using Legacy* methods.
func addDefinitionsForTestingViaLegacy(ctx context.Context, legacyWriter datastore.LegacySchemaWriter,
	tb testing.TB, definitions ...datastore.SchemaDefinition,
) error {
	tb.Helper()

	var namespaces []*core.NamespaceDefinition
	var caveats []*core.CaveatDefinition

	for _, def := range definitions {
		switch typedDef := def.(type) {
		case *core.NamespaceDefinition:
			namespaces = append(namespaces, typedDef)
		case *core.CaveatDefinition:
			caveats = append(caveats, typedDef)
		default:
			return spiceerrors.MustBugf("unknown definition type: %T", def)
		}
	}

	if len(namespaces) > 0 {
		if err := legacyWriter.LegacyWriteNamespaces(ctx, namespaces...); err != nil {
			return fmt.Errorf("failed to write namespaces: %w", err)
		}
	}

	if len(caveats) > 0 {
		if err := legacyWriter.LegacyWriteCaveats(ctx, caveats); err != nil {
			return fmt.Errorf("failed to write caveats: %w", err)
		}
	}

	return nil
}
