package schema

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/spiceerrors"
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
	nameSet := mapz.NewSet(names...)
	allDefs := make(map[string]datastore.SchemaDefinition, len(names))

	// Look up as typedefs first
	typeDefs, err := l.LookupTypeDefinitionsByNames(ctx, names)
	if err != nil {
		return nil, err
	}

	// Look up remaining names as caveats
	remainingNames := nameSet.Subtract(mapz.NewSet(slices.Collect(maps.Keys(typeDefs))...))
	caveatDefs, err := l.LookupCaveatDefinitionsByNames(ctx, remainingNames.AsSlice())
	if err != nil {
		return nil, err
	}

	// Merge the maps and return
	// NOTE: we're able to do this naively because caveat names and definition names can't overlap.
	maps.Copy(allDefs, typeDefs)
	maps.Copy(allDefs, caveatDefs)
	return allDefs, nil
}

func (l *LegacySchemaReaderAdapter) LookupTypeDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.SchemaDefinition, error) {
	result := make(map[string]datastore.SchemaDefinition)

	// Try to look up as namespaces
	namespaces, err := l.legacyReader.LegacyLookupNamespacesWithNames(ctx, names)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup namespaces: %w", err)
	}

	for _, ns := range namespaces {
		result[ns.Definition.Name] = ns.Definition
	}
	return result, nil
}

// LookupCaveatDefinitionsByNames looks up type definitions by name.
func (l *LegacySchemaReaderAdapter) LookupCaveatDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.SchemaDefinition, error) {
	result := make(map[string]datastore.SchemaDefinition)

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

// LegacySchemaWriterAdapter is a common implementation of SchemaWriter that uses the legacy
// schema writer methods. This allows datastores to implement the new SchemaWriter interface
// while still using the legacy methods internally during the transition period.
type LegacySchemaWriterAdapter struct {
	legacyWriter datastore.LegacySchemaWriter
	legacyReader datastore.LegacySchemaReader
}

// NewLegacySchemaWriterAdapter creates a new LegacySchemaWriterAdapter that wraps a LegacySchemaWriter.
func NewLegacySchemaWriterAdapter(legacyWriter datastore.LegacySchemaWriter, legacyReader datastore.LegacySchemaReader) *LegacySchemaWriterAdapter {
	return &LegacySchemaWriterAdapter{
		legacyWriter: legacyWriter,
		legacyReader: legacyReader,
	}
}

// WriteSchema writes the full set of schema definitions. The schema string is provided for
// future use but is currently ignored by implementations. The method validates that no
// definition names overlap, loads existing definitions, replaces changed ones, and deletes
// definitions no longer present.
func (l *LegacySchemaWriterAdapter) WriteSchema(ctx context.Context, definitions []datastore.SchemaDefinition, schemaString string, caveatTypeSet *types.TypeSet) error {
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

		// Type assertion to determine if it's a namespace or caveat
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
	existingTypeDefs, err := l.legacyReader.LegacyListAllNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("failed to list existing namespaces: %w", err)
	}

	existingCaveatDefs, err := l.legacyReader.LegacyListAllCaveats(ctx)
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
		if err := l.legacyWriter.LegacyWriteNamespaces(ctx, namespacesToWrite...); err != nil {
			return fmt.Errorf("failed to write namespaces: %w", err)
		}
	}

	// Write caveats (only new and changed)
	if len(caveatsToWrite) > 0 {
		if err := l.legacyWriter.LegacyWriteCaveats(ctx, caveatsToWrite); err != nil {
			return fmt.Errorf("failed to write caveats: %w", err)
		}
	}

	// Delete removed namespaces
	removedTypeNames := existingTypeNames.Subtract(newTypeNames)
	if removedTypeNames.Len() > 0 {
		if err := l.legacyWriter.LegacyDeleteNamespaces(ctx, removedTypeNames.AsSlice(), datastore.DeleteNamespacesOnly); err != nil {
			return fmt.Errorf("failed to delete removed namespaces: %w", err)
		}
	}

	// Delete removed caveats
	removedCaveatNames := existingCaveatNames.Subtract(newCaveatNames)
	if removedCaveatNames.Len() > 0 {
		if err := l.legacyWriter.LegacyDeleteCaveats(ctx, removedCaveatNames.AsSlice()); err != nil {
			return fmt.Errorf("failed to delete removed caveats: %w", err)
		}
	}

	// If the writer supports writing legacy schema hashes, compute and write the hash
	// from the final set of definitions (after all writes are buffered).
	// This avoids the problem of reading back buffered writes that aren't yet visible.
	if hashWriter, ok := l.legacyWriter.(datastore.LegacySchemaHashWriter); ok {
		// Build the final list of namespaces (new ones, replacing existing)
		finalNamespaces := make([]datastore.RevisionedNamespace, 0, len(namespaces))
		for _, ns := range namespaces {
			finalNamespaces = append(finalNamespaces, datastore.RevisionedNamespace{
				Definition: ns,
				// Revision doesn't matter for hash computation
				LastWrittenRevision: datastore.NoRevision,
			})
		}

		// Build the final list of caveats (new ones, replacing existing)
		finalCaveats := make([]datastore.RevisionedCaveat, 0, len(caveats))
		for _, caveat := range caveats {
			finalCaveats = append(finalCaveats, datastore.RevisionedCaveat{
				Definition: caveat,
				// Revision doesn't matter for hash computation
				LastWrittenRevision: datastore.NoRevision,
			})
		}

		if err := hashWriter.WriteLegacySchemaHashFromDefinitions(ctx, finalNamespaces, finalCaveats); err != nil {
			return fmt.Errorf("failed to write schema hash: %w", err)
		}
	}

	return nil
}

// AddDefinitionsForTesting adds or overwrites the given schema definitions. This method is
// only for use in testing and requires a testing.TB instance to enforce this constraint.
func (l *LegacySchemaWriterAdapter) AddDefinitionsForTesting(ctx context.Context, tb testing.TB, definitions ...datastore.SchemaDefinition) error {
	// The tb parameter ensures this is only called from tests
	tb.Helper()

	var namespaces []*core.NamespaceDefinition
	var caveats []*core.CaveatDefinition

	for _, def := range definitions {
		// Type assertion to determine if it's a namespace or caveat
		switch typedDef := def.(type) {
		case *core.NamespaceDefinition:
			namespaces = append(namespaces, typedDef)
		case *core.CaveatDefinition:
			caveats = append(caveats, typedDef)
		default:
			return spiceerrors.MustBugf("unknown definition type: %T", def)
		}
	}

	// Write namespaces (add or overwrite)
	if len(namespaces) > 0 {
		if err := l.legacyWriter.LegacyWriteNamespaces(ctx, namespaces...); err != nil {
			return fmt.Errorf("failed to write namespaces: %w", err)
		}
	}

	// Write caveats (add or overwrite)
	if len(caveats) > 0 {
		if err := l.legacyWriter.LegacyWriteCaveats(ctx, caveats); err != nil {
			return fmt.Errorf("failed to write caveats: %w", err)
		}
	}

	return nil
}

var _ datastore.SchemaWriter = (*LegacySchemaWriterAdapter)(nil)

// NewSchemaReader creates a new schema reader based on the experimental schema mode.
// The reader parameter must implement DualSchemaReader, which provides both legacy and single-store methods.
// The snapshotRevision is the revision at which the reader is reading.
// Based on the schema mode, this function returns the appropriate reader implementation.
func NewSchemaReader(reader datastore.DualSchemaReader, schemaMode dsoptions.SchemaMode, snapshotRevision datastore.Revision) datastore.SchemaReader {
	switch schemaMode {
	case dsoptions.SchemaModeReadNewWriteBoth, dsoptions.SchemaModeReadNewWriteNew:
		// Use unified schema storage for reading
		return newSingleStoreSchemaReader(reader, snapshotRevision)

	case dsoptions.SchemaModeReadLegacyWriteLegacy, dsoptions.SchemaModeReadLegacyWriteBoth:
		// Use legacy schema storage for reading
		return NewLegacySchemaReaderAdapter(reader)

	default:
		panic(fmt.Sprintf("unsupported schema mode: %v", schemaMode))
	}
}

// NewSchemaWriter creates a new schema writer based on the experimental schema mode.
// The writer parameter must implement DualSchemaWriter, which provides both legacy and single-store methods.
// Based on the schema mode, this function returns the appropriate writer implementation:
// - SchemaModeReadLegacyWriteLegacy: writes only to legacy storage
// - SchemaModeReadLegacyWriteBoth or SchemaModeReadNewWriteBoth: writes to both legacy and unified storage
// - SchemaModeReadNewWriteNew: writes only to unified storage
func NewSchemaWriter(writer datastore.DualSchemaWriter, reader datastore.DualSchemaReader, schemaMode dsoptions.SchemaMode) datastore.SchemaWriter {
	switch schemaMode {
	case dsoptions.SchemaModeReadNewWriteNew:
		// Use unified schema storage only
		return newSingleStoreSchemaWriter(writer, reader)

	case dsoptions.SchemaModeReadLegacyWriteBoth, dsoptions.SchemaModeReadNewWriteBoth:
		// Write to both legacy and unified storage
		return newDualSchemaWriter(writer, reader)

	case dsoptions.SchemaModeReadLegacyWriteLegacy:
		// Use legacy schema storage only
		return NewLegacySchemaWriterAdapter(writer, reader)

	default:
		panic(fmt.Sprintf("unsupported schema mode: %v", schemaMode))
	}
}

// dualSchemaWriter writes to both legacy and unified schema storage.
type dualSchemaWriter struct {
	legacyWriter  *LegacySchemaWriterAdapter
	unifiedWriter *singleStoreSchemaWriter
}

// newDualSchemaWriter creates a new writer that writes to both legacy and unified storage.
func newDualSchemaWriter(writer datastore.DualSchemaWriter, reader datastore.DualSchemaReader) *dualSchemaWriter {
	return &dualSchemaWriter{
		legacyWriter:  NewLegacySchemaWriterAdapter(writer, reader),
		unifiedWriter: newSingleStoreSchemaWriter(writer, reader),
	}
}

// WriteSchema writes the schema to both legacy and unified storage.
func (d *dualSchemaWriter) WriteSchema(ctx context.Context, definitions []datastore.SchemaDefinition, schemaString string, caveatTypeSet *types.TypeSet) error {
	// Write to legacy storage first
	if err := d.legacyWriter.WriteSchema(ctx, definitions, schemaString, caveatTypeSet); err != nil {
		return fmt.Errorf("failed to write to legacy storage: %w", err)
	}

	// Write to unified storage
	if err := d.unifiedWriter.WriteSchema(ctx, definitions, schemaString, caveatTypeSet); err != nil {
		return fmt.Errorf("failed to write to unified storage: %w", err)
	}

	return nil
}

// AddDefinitionsForTesting adds or overwrites schema definitions in both storages.
func (d *dualSchemaWriter) AddDefinitionsForTesting(ctx context.Context, tb testing.TB, definitions ...datastore.SchemaDefinition) error {
	tb.Helper()

	// Add to legacy storage first
	if err := d.legacyWriter.AddDefinitionsForTesting(ctx, tb, definitions...); err != nil {
		return fmt.Errorf("failed to add to legacy storage: %w", err)
	}

	// Add to unified storage
	if err := d.unifiedWriter.AddDefinitionsForTesting(ctx, tb, definitions...); err != nil {
		return fmt.Errorf("failed to add to unified storage: %w", err)
	}

	return nil
}

var _ datastore.SchemaWriter = (*dualSchemaWriter)(nil)
