package datalayer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"

	"go.opentelemetry.io/otel/attribute"

	"github.com/authzed/spicedb/internal/telemetry/otelconv"
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
func (l *legacySchemaReaderAdapter) SchemaText(ctx context.Context) (string, error) {
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

	// Sort the definitions by name for deterministic output
	sort.Slice(namespaces, func(i, j int) bool {
		return namespaces[i].Definition.Name < namespaces[j].Definition.Name
	})
	sort.Slice(caveats, func(i, j int) bool {
		return caveats[i].Definition.Name < caveats[j].Definition.Name
	})

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
	schemaText, _, err := generator.GenerateSchemaWithCaveatTypeSet(ctx, definitions, caveatTypeSet)
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

// storedSchemaReaderAdapter implements SchemaReader by reading from the unified
// StoredSchema proto via ReadStoredSchema on the underlying datastore reader.
type storedSchemaReaderAdapter struct {
	storedSchema        *datastore.ReadOnlyStoredSchema
	lastWrittenRevision datastore.Revision
}

// storedSchemaReader is the interface required to read a stored schema.
type storedSchemaReader interface {
	ReadStoredSchema(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error)
}

// newStoredSchemaReaderAdapter creates a storedSchemaReaderAdapter by loading
// the stored schema from the reader. The lastWrittenRevision is the revision at
// which this snapshot was taken; it is returned as LastWrittenRevision on each definition.
// The cache is used to cache and deduplicate ReadStoredSchema calls.
func newStoredSchemaReaderAdapter(ctx context.Context, reader storedSchemaReader, schemaHash SchemaHash, lastWrittenRevision datastore.Revision, cache storedSchemaCache,
) (*storedSchemaReaderAdapter, error) {
	ctx, span := tracer.Start(ctx, "ReadStoredSchema")
	defer span.End()
	span.SetAttributes(attribute.String(otelconv.AttrSchemaHash, string(schemaHash)))

	storedSchema, err := cache.GetOrLoad(ctx, lastWrittenRevision, schemaHash, func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		return reader.ReadStoredSchema(ctx)
	})
	if err != nil {
		if errors.Is(err, datastore.ErrSchemaNotFound) {
			// No unified schema yet; return an adapter with no definitions
			return &storedSchemaReaderAdapter{
				storedSchema: datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
					Version: 1,
					VersionOneof: &core.StoredSchema_V1{
						V1: &core.StoredSchema_V1StoredSchema{},
					},
				}),
				lastWrittenRevision: lastWrittenRevision,
			}, nil
		}
		return nil, fmt.Errorf("failed to read stored schema: %w", err)
	}
	return &storedSchemaReaderAdapter{storedSchema: storedSchema, lastWrittenRevision: lastWrittenRevision}, nil
}

func (s *storedSchemaReaderAdapter) v1() *core.StoredSchema_V1StoredSchema {
	if v1 := s.storedSchema.Get().GetV1(); v1 != nil {
		return v1
	}
	return &core.StoredSchema_V1StoredSchema{}
}

func (s *storedSchemaReaderAdapter) SchemaText(_ context.Context) (string, error) {
	v1 := s.v1()
	if v1.SchemaText == "" {
		if len(v1.NamespaceDefinitions) == 0 && len(v1.CaveatDefinitions) == 0 {
			return "", datastore.NewSchemaNotDefinedErr()
		}
	}
	return v1.SchemaText, nil
}

func (s *storedSchemaReaderAdapter) LookupTypeDefByName(_ context.Context, name string) (datastore.RevisionedTypeDefinition, bool, error) {
	v1 := s.v1()
	ns, ok := v1.NamespaceDefinitions[name]
	if !ok {
		return datastore.RevisionedTypeDefinition{}, false, nil
	}
	return datastore.RevisionedTypeDefinition{
		Definition:          ns,
		LastWrittenRevision: s.lastWrittenRevision,
	}, true, nil
}

func (s *storedSchemaReaderAdapter) LookupCaveatDefByName(_ context.Context, name string) (datastore.RevisionedCaveat, bool, error) {
	v1 := s.v1()
	caveat, ok := v1.CaveatDefinitions[name]
	if !ok {
		return datastore.RevisionedCaveat{}, false, nil
	}
	return datastore.RevisionedCaveat{
		Definition:          caveat,
		LastWrittenRevision: s.lastWrittenRevision,
	}, true, nil
}

func (s *storedSchemaReaderAdapter) ListAllTypeDefinitions(_ context.Context) ([]datastore.RevisionedTypeDefinition, error) {
	v1 := s.v1()
	result := make([]datastore.RevisionedTypeDefinition, 0, len(v1.NamespaceDefinitions))
	for _, ns := range v1.NamespaceDefinitions {
		result = append(result, datastore.RevisionedTypeDefinition{
			Definition:          ns,
			LastWrittenRevision: s.lastWrittenRevision,
		})
	}
	return result, nil
}

func (s *storedSchemaReaderAdapter) ListAllCaveatDefinitions(_ context.Context) ([]datastore.RevisionedCaveat, error) {
	v1 := s.v1()
	result := make([]datastore.RevisionedCaveat, 0, len(v1.CaveatDefinitions))
	for _, caveat := range v1.CaveatDefinitions {
		result = append(result, datastore.RevisionedCaveat{
			Definition:          caveat,
			LastWrittenRevision: s.lastWrittenRevision,
		})
	}
	return result, nil
}

func (s *storedSchemaReaderAdapter) ListAllSchemaDefinitions(_ context.Context) (map[string]datastore.SchemaDefinition, error) {
	v1 := s.v1()
	result := make(map[string]datastore.SchemaDefinition, len(v1.NamespaceDefinitions)+len(v1.CaveatDefinitions))
	for name, ns := range v1.NamespaceDefinitions {
		result[name] = ns
	}
	for name, caveat := range v1.CaveatDefinitions {
		result[name] = caveat
	}
	return result, nil
}

func (s *storedSchemaReaderAdapter) LookupSchemaDefinitionsByNames(_ context.Context, names []string) (map[string]datastore.SchemaDefinition, error) {
	v1 := s.v1()
	result := make(map[string]datastore.SchemaDefinition, len(names))
	for _, name := range names {
		if ns, ok := v1.NamespaceDefinitions[name]; ok {
			result[name] = ns
		} else if caveat, ok := v1.CaveatDefinitions[name]; ok {
			result[name] = caveat
		}
	}
	return result, nil
}

func (s *storedSchemaReaderAdapter) LookupTypeDefinitionsByNames(_ context.Context, names []string) (map[string]datastore.TypeDefinition, error) {
	v1 := s.v1()
	result := make(map[string]datastore.TypeDefinition, len(names))
	for _, name := range names {
		if ns, ok := v1.NamespaceDefinitions[name]; ok {
			result[name] = ns
		}
	}
	return result, nil
}

func (s *storedSchemaReaderAdapter) LookupCaveatDefinitionsByNames(_ context.Context, names []string) (map[string]datastore.CaveatDefinition, error) {
	v1 := s.v1()
	result := make(map[string]datastore.CaveatDefinition, len(names))
	for _, name := range names {
		if caveat, ok := v1.CaveatDefinitions[name]; ok {
			result[name] = caveat
		}
	}
	return result, nil
}

var _ SchemaReader = (*storedSchemaReaderAdapter)(nil)

// WriteSchemaViaStoredSchema builds a StoredSchema proto and writes it via WriteStoredSchema.
// If cache is nil, a no-op cache is used.
func WriteSchemaViaStoredSchema(ctx context.Context, rwt datastore.ReadWriteTransaction,
	definitions []datastore.SchemaDefinition, schemaString string, cache storedSchemaCache,
) error {
	if cache == nil {
		cache = noopSchemaCache{}
	}

	ctx, span := tracer.Start(ctx, "WriteSchemaViaStoredSchema")
	defer span.End()

	namespaceDefs := make(map[string]*core.NamespaceDefinition)
	caveatDefs := make(map[string]*core.CaveatDefinition)

	for _, def := range definitions {
		if _, existing := namespaceDefs[def.GetName()]; existing {
			return spiceerrors.MustBugf("duplicate definition name: %s", def.GetName())
		}
		if _, existing := caveatDefs[def.GetName()]; existing {
			return spiceerrors.MustBugf("duplicate definition name: %s", def.GetName())
		}

		switch typedDef := def.(type) {
		case *core.NamespaceDefinition:
			namespaceDefs[typedDef.Name] = typedDef
		case *core.CaveatDefinition:
			caveatDefs[typedDef.Name] = typedDef
		default:
			return spiceerrors.MustBugf("unknown definition type: %T", def)
		}
	}

	// Compute schema hash from the definitions
	compDefs := make([]compiler.SchemaDefinition, 0, len(definitions))
	for _, def := range definitions {
		compDef, ok := def.(compiler.SchemaDefinition)
		if !ok {
			return fmt.Errorf("definition %q does not implement compiler.SchemaDefinition", def.GetName())
		}
		compDefs = append(compDefs, compDef)
	}

	schemaHash, err := generator.ComputeSchemaHash(compDefs)
	if err != nil {
		return fmt.Errorf("failed to compute schema hash: %w", err)
	}
	span.SetAttributes(attribute.String(otelconv.AttrSchemaHash, schemaHash))
	span.SetAttributes(attribute.Int(otelconv.AttrSchemaDataSizeBytes, len(schemaString)))

	storedSchema := &core.StoredSchema{
		Version: 1,
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaText:           schemaString,
				SchemaHash:           schemaHash,
				NamespaceDefinitions: namespaceDefs,
				CaveatDefinitions:    caveatDefs,
			},
		},
	}

	if err := rwt.WriteStoredSchema(ctx, storedSchema); err != nil {
		return err
	}

	// Update cache after successful write
	if v1 := storedSchema.GetV1(); v1 != nil && v1.SchemaHash != "" {
		if err := cache.Set(SchemaHash(v1.SchemaHash), datastore.NewReadOnlyStoredSchema(storedSchema)); err != nil {
			return err
		}
	}

	return nil
}
