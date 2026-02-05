package schema

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	// currentSchemaVersion is the current version of the StoredSchema proto.
	currentSchemaVersion = 1
)

// singleStoreSchemaReader implements SchemaReader using the unified schema storage.
type singleStoreSchemaReader struct {
	reader           datastore.SingleStoreSchemaReader
	snapshotRevision datastore.Revision
}

// newSingleStoreSchemaReader creates a new schema reader that uses unified schema storage.
func newSingleStoreSchemaReader(reader datastore.SingleStoreSchemaReader, snapshotRevision datastore.Revision) *singleStoreSchemaReader {
	return &singleStoreSchemaReader{
		reader:           reader,
		snapshotRevision: snapshotRevision,
	}
}

// SchemaText returns the schema text from the unified schema storage.
func (s *singleStoreSchemaReader) SchemaText() (string, error) {
	ctx := context.Background()

	// Read the stored schema
	storedSchema, err := s.reader.ReadStoredSchema(ctx)
	if err != nil {
		if errors.Is(err, datastore.ErrSchemaNotFound) {
			return "", datastore.NewSchemaNotDefinedErr()
		}
		return "", err
	}

	// Extract schema text based on version
	switch {
	case storedSchema.GetV1() != nil:
		return storedSchema.GetV1().SchemaText, nil
	default:
		return "", fmt.Errorf("unsupported schema version: %d", storedSchema.Version)
	}
}

// LookupTypeDefByName looks up a type definition by name from the unified schema.
func (s *singleStoreSchemaReader) LookupTypeDefByName(ctx context.Context, name string) (datastore.RevisionedTypeDefinition, bool, error) {
	storedSchema, err := s.reader.ReadStoredSchema(ctx)
	if err != nil {
		if errors.Is(err, datastore.ErrSchemaNotFound) {
			return datastore.RevisionedTypeDefinition{}, false, nil
		}
		return datastore.RevisionedTypeDefinition{}, false, err
	}

	v1 := storedSchema.GetV1()
	if v1 == nil {
		return datastore.RevisionedTypeDefinition{}, false, fmt.Errorf("unsupported schema version: %d", storedSchema.Version)
	}

	ns, found := v1.NamespaceDefinitions[name]
	if !found {
		return datastore.RevisionedTypeDefinition{}, false, nil
	}

	return datastore.RevisionedTypeDefinition{
		Definition: ns,
		// In unified schema mode, all definitions share the schema's revision
		LastWrittenRevision: s.snapshotRevision,
	}, true, nil
}

// LookupCaveatDefByName looks up a caveat definition by name from the unified schema.
func (s *singleStoreSchemaReader) LookupCaveatDefByName(ctx context.Context, name string) (datastore.RevisionedCaveat, bool, error) {
	storedSchema, err := s.reader.ReadStoredSchema(ctx)
	if err != nil {
		if errors.Is(err, datastore.ErrSchemaNotFound) {
			return datastore.RevisionedCaveat{}, false, nil
		}
		return datastore.RevisionedCaveat{}, false, err
	}

	v1 := storedSchema.GetV1()
	if v1 == nil {
		return datastore.RevisionedCaveat{}, false, fmt.Errorf("unsupported schema version: %d", storedSchema.Version)
	}

	caveat, found := v1.CaveatDefinitions[name]
	if !found {
		return datastore.RevisionedCaveat{}, false, nil
	}

	return datastore.RevisionedCaveat{
		Definition: caveat,
		// In unified schema mode, all definitions share the schema's revision
		LastWrittenRevision: s.snapshotRevision,
	}, true, nil
}

// ListAllTypeDefinitions lists all type definitions from the unified schema.
func (s *singleStoreSchemaReader) ListAllTypeDefinitions(ctx context.Context) ([]datastore.RevisionedTypeDefinition, error) {
	storedSchema, err := s.reader.ReadStoredSchema(ctx)
	if err != nil {
		if errors.Is(err, datastore.ErrSchemaNotFound) {
			return nil, nil
		}
		return nil, err
	}

	v1 := storedSchema.GetV1()
	if v1 == nil {
		return nil, fmt.Errorf("unsupported schema version: %d", storedSchema.Version)
	}

	results := make([]datastore.RevisionedTypeDefinition, 0, len(v1.NamespaceDefinitions))
	for _, ns := range v1.NamespaceDefinitions {
		results = append(results, datastore.RevisionedTypeDefinition{
			Definition:          ns,
			LastWrittenRevision: s.snapshotRevision,
		})
	}

	return results, nil
}

// ListAllCaveatDefinitions lists all caveat definitions from the unified schema.
func (s *singleStoreSchemaReader) ListAllCaveatDefinitions(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	storedSchema, err := s.reader.ReadStoredSchema(ctx)
	if err != nil {
		if errors.Is(err, datastore.ErrSchemaNotFound) {
			return nil, nil
		}
		return nil, err
	}

	v1 := storedSchema.GetV1()
	if v1 == nil {
		return nil, fmt.Errorf("unsupported schema version: %d", storedSchema.Version)
	}

	results := make([]datastore.RevisionedCaveat, 0, len(v1.CaveatDefinitions))
	for _, caveat := range v1.CaveatDefinitions {
		results = append(results, datastore.RevisionedCaveat{
			Definition:          caveat,
			LastWrittenRevision: s.snapshotRevision,
		})
	}

	return results, nil
}

// ListAllSchemaDefinitions lists all schema definitions from the unified schema.
func (s *singleStoreSchemaReader) ListAllSchemaDefinitions(ctx context.Context) (map[string]datastore.SchemaDefinition, error) {
	storedSchema, err := s.reader.ReadStoredSchema(ctx)
	if err != nil {
		if errors.Is(err, datastore.ErrSchemaNotFound) {
			return make(map[string]datastore.SchemaDefinition), nil
		}
		return nil, err
	}

	v1 := storedSchema.GetV1()
	if v1 == nil {
		return nil, fmt.Errorf("unsupported schema version: %d", storedSchema.Version)
	}

	result := make(map[string]datastore.SchemaDefinition, len(v1.NamespaceDefinitions)+len(v1.CaveatDefinitions))
	for _, ns := range v1.NamespaceDefinitions {
		result[ns.Name] = ns
	}
	for _, caveat := range v1.CaveatDefinitions {
		result[caveat.Name] = caveat
	}

	return result, nil
}

// LookupSchemaDefinitionsByNames looks up schema definitions by name from the unified schema.
func (s *singleStoreSchemaReader) LookupSchemaDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.SchemaDefinition, error) {
	storedSchema, err := s.reader.ReadStoredSchema(ctx)
	if err != nil {
		if errors.Is(err, datastore.ErrSchemaNotFound) {
			return make(map[string]datastore.SchemaDefinition), nil
		}
		return nil, err
	}

	v1 := storedSchema.GetV1()
	if v1 == nil {
		return nil, fmt.Errorf("unsupported schema version: %d", storedSchema.Version)
	}

	result := make(map[string]datastore.SchemaDefinition)
	for _, name := range names {
		if ns, found := v1.NamespaceDefinitions[name]; found {
			result[name] = ns
		} else if caveat, found := v1.CaveatDefinitions[name]; found {
			result[name] = caveat
		}
	}

	return result, nil
}

// LookupTypeDefinitionsByNames looks up type definitions by name from the unified schema.
func (s *singleStoreSchemaReader) LookupTypeDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.SchemaDefinition, error) {
	storedSchema, err := s.reader.ReadStoredSchema(ctx)
	if err != nil {
		if errors.Is(err, datastore.ErrSchemaNotFound) {
			return make(map[string]datastore.SchemaDefinition), nil
		}
		return nil, err
	}

	v1 := storedSchema.GetV1()
	if v1 == nil {
		return nil, fmt.Errorf("unsupported schema version: %d", storedSchema.Version)
	}

	result := make(map[string]datastore.SchemaDefinition)
	for _, name := range names {
		if ns, found := v1.NamespaceDefinitions[name]; found {
			result[name] = ns
		}
	}

	return result, nil
}

// LookupCaveatDefinitionsByNames looks up caveat definitions by name from the unified schema.
func (s *singleStoreSchemaReader) LookupCaveatDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.SchemaDefinition, error) {
	storedSchema, err := s.reader.ReadStoredSchema(ctx)
	if err != nil {
		if errors.Is(err, datastore.ErrSchemaNotFound) {
			return make(map[string]datastore.SchemaDefinition), nil
		}
		return nil, err
	}

	v1 := storedSchema.GetV1()
	if v1 == nil {
		return nil, fmt.Errorf("unsupported schema version: %d", storedSchema.Version)
	}

	result := make(map[string]datastore.SchemaDefinition)
	for _, name := range names {
		if caveat, found := v1.CaveatDefinitions[name]; found {
			result[name] = caveat
		}
	}

	return result, nil
}

var _ datastore.SchemaReader = (*singleStoreSchemaReader)(nil)

// singleStoreSchemaWriter implements SchemaWriter using the unified schema storage.
type singleStoreSchemaWriter struct {
	writer datastore.SingleStoreSchemaWriter
	reader datastore.SingleStoreSchemaReader
}

// newSingleStoreSchemaWriter creates a new schema writer that uses unified schema storage.
func newSingleStoreSchemaWriter(writer datastore.SingleStoreSchemaWriter, reader datastore.SingleStoreSchemaReader) *singleStoreSchemaWriter {
	return &singleStoreSchemaWriter{
		writer: writer,
		reader: reader,
	}
}

// WriteSchema writes the schema to unified storage.
func (s *singleStoreSchemaWriter) WriteSchema(ctx context.Context, definitions []datastore.SchemaDefinition, schemaString string, caveatTypeSet *types.TypeSet) error {
	// Build namespace and caveat maps
	namespaces := make(map[string]*core.NamespaceDefinition)
	caveats := make(map[string]*core.CaveatDefinition)

	for _, def := range definitions {
		switch typedDef := def.(type) {
		case *core.NamespaceDefinition:
			namespaces[typedDef.Name] = typedDef
		case *core.CaveatDefinition:
			caveats[typedDef.Name] = typedDef
		default:
			return spiceerrors.MustBugf("unknown definition type: %T", def)
		}
	}

	// Generate canonical schema hash by sorting definitions alphabetically
	// This ensures the hash is deterministic regardless of definition order
	// Create a sortable slice - each definition implements both interfaces
	sortedDefs := make([]datastore.SchemaDefinition, len(definitions))
	copy(sortedDefs, definitions)
	sort.Slice(sortedDefs, func(i, j int) bool {
		return sortedDefs[i].GetName() < sortedDefs[j].GetName()
	})

	// Convert to compiler.SchemaDefinition for generator
	canonicalDefs := make([]compiler.SchemaDefinition, len(sortedDefs))
	for i, def := range sortedDefs {
		canonicalDefs[i] = def.(compiler.SchemaDefinition)
	}

	canonicalSchemaText, _, err := generator.GenerateSchema(canonicalDefs)
	if err != nil {
		return fmt.Errorf("failed to generate canonical schema: %w", err)
	}

	schemaHash := hex.EncodeToString([]byte(canonicalSchemaText))

	// Create the stored schema proto
	storedSchema := &core.StoredSchema{
		Version: currentSchemaVersion,
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaText:           schemaString,
				SchemaHash:           schemaHash,
				NamespaceDefinitions: namespaces,
				CaveatDefinitions:    caveats,
			},
		},
	}

	// Write to storage
	return s.writer.WriteStoredSchema(ctx, storedSchema)
}

// AddDefinitionsForTesting adds or overwrites schema definitions for testing.
func (s *singleStoreSchemaWriter) AddDefinitionsForTesting(ctx context.Context, tb testing.TB, definitions ...datastore.SchemaDefinition) error {
	tb.Helper()

	// Read existing schema
	existingSchema, err := s.reader.ReadStoredSchema(ctx)
	if err != nil && !errors.Is(err, datastore.ErrSchemaNotFound) {
		return err
	}

	// Start with empty maps if no existing schema
	var namespaces map[string]*core.NamespaceDefinition
	var caveats map[string]*core.CaveatDefinition
	var schemaText string

	if existingSchema != nil && existingSchema.GetV1() != nil {
		v1 := existingSchema.GetV1()
		namespaces = make(map[string]*core.NamespaceDefinition, len(v1.NamespaceDefinitions))
		caveats = make(map[string]*core.CaveatDefinition, len(v1.CaveatDefinitions))
		for k, v := range v1.NamespaceDefinitions {
			namespaces[k] = v
		}
		for k, v := range v1.CaveatDefinitions {
			caveats[k] = v
		}
		schemaText = v1.SchemaText
	} else {
		namespaces = make(map[string]*core.NamespaceDefinition)
		caveats = make(map[string]*core.CaveatDefinition)
	}

	// Add or overwrite definitions
	for _, def := range definitions {
		switch typedDef := def.(type) {
		case *core.NamespaceDefinition:
			namespaces[typedDef.Name] = typedDef
		case *core.CaveatDefinition:
			caveats[typedDef.Name] = typedDef
		default:
			return spiceerrors.MustBugf("unknown definition type: %T", def)
		}
	}

	// Regenerate schema text if needed
	if schemaText == "" {
		allDefs := make([]compiler.SchemaDefinition, 0, len(namespaces)+len(caveats))
		for _, ns := range namespaces {
			allDefs = append(allDefs, ns)
		}
		for _, caveat := range caveats {
			allDefs = append(allDefs, caveat)
		}

		caveatTypeSet := types.Default.TypeSet
		newSchemaText, _, err := generator.GenerateSchemaWithCaveatTypeSet(allDefs, caveatTypeSet)
		if err != nil {
			return fmt.Errorf("failed to generate schema text: %w", err)
		}
		schemaText = newSchemaText
	}

	// Generate canonical schema hash by sorting all definitions alphabetically
	allDefs := make([]compiler.SchemaDefinition, 0, len(namespaces)+len(caveats))
	for _, ns := range namespaces {
		allDefs = append(allDefs, ns)
	}
	for _, caveat := range caveats {
		allDefs = append(allDefs, caveat)
	}

	sort.Slice(allDefs, func(i, j int) bool {
		return allDefs[i].GetName() < allDefs[j].GetName()
	})

	canonicalSchemaText, _, err := generator.GenerateSchema(allDefs)
	if err != nil {
		return fmt.Errorf("failed to generate canonical schema: %w", err)
	}

	schemaHash := hex.EncodeToString([]byte(canonicalSchemaText))
	storedSchema := &core.StoredSchema{
		Version: currentSchemaVersion,
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaText:           schemaText,
				SchemaHash:           schemaHash,
				NamespaceDefinitions: namespaces,
				CaveatDefinitions:    caveats,
			},
		},
	}

	// Write to storage
	return s.writer.WriteStoredSchema(ctx, storedSchema)
}

var _ datastore.SchemaWriter = (*singleStoreSchemaWriter)(nil)

// BuildStoredSchemaFromDefinitions is a helper function to build a StoredSchema proto from schema definitions.
func BuildStoredSchemaFromDefinitions(definitions []datastore.SchemaDefinition, schemaString string) (*core.StoredSchema, error) {
	// Build namespace and caveat maps
	namespaces := make(map[string]*core.NamespaceDefinition)
	caveats := make(map[string]*core.CaveatDefinition)

	for _, def := range definitions {
		switch typedDef := def.(type) {
		case *core.NamespaceDefinition:
			namespaces[typedDef.Name] = typedDef
		case *core.CaveatDefinition:
			caveats[typedDef.Name] = typedDef
		default:
			return nil, spiceerrors.MustBugf("unknown definition type: %T", def)
		}
	}

	// Generate schema hash from the schema string
	schemaHash := hex.EncodeToString([]byte(schemaString))

	// Create the stored schema proto
	return &core.StoredSchema{
		Version: currentSchemaVersion,
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaText:           schemaString,
				SchemaHash:           schemaHash,
				NamespaceDefinitions: namespaces,
				CaveatDefinitions:    caveats,
			},
		},
	}, nil
}

// UnmarshalStoredSchema unmarshals a StoredSchema from bytes.
func UnmarshalStoredSchema(data []byte) (*core.StoredSchema, error) {
	var stored core.StoredSchema
	if err := stored.UnmarshalVT(data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}
	return &stored, nil
}

// MarshalStoredSchema marshals a StoredSchema to bytes.
func MarshalStoredSchema(schema *core.StoredSchema) ([]byte, error) {
	data, err := schema.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal schema: %w", err)
	}
	return data, nil
}
