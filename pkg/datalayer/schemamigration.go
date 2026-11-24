package datalayer

import (
	"context"
	"sort"
	"sync/atomic"

	"github.com/maypok86/otter/v2"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
)

// The marker is immutable once the refresh commits. Cache it for the datalayer's
// lifetime; do not cache absence or failures while an upgrade is still running.
// Every schema read consults the boundary, so it is loaded without a lock:
// concurrent first loads all store the same immutable value.
type schemaMigrationState struct {
	reader   datastore.SchemaMigrationBoundaryReader
	boundary atomic.Pointer[datastore.SchemaMigrationBoundary]

	// TODO: remove this cache, along with the rest of the historical legacy read
	// path, once the legacy-to-unified schema migration is removed and no
	// supported deployment can serve revisions from before its boundary.
	//
	// The legacy caching proxy is not installed when reads come from unified
	// storage, and it does not cache full listings in any case. Without this cache,
	// every schema read at a pre-boundary revision would list every legacy
	// definition and regenerate the schema.
	historical *otter.Cache[string, SchemaReader]
}

// historicalSchemaCacheSize bounds the pre-boundary revisions kept materialized.
// Requests from shortly before the refresh share a few quantized revisions, and
// cursors reuse theirs across pages.
const historicalSchemaCacheSize = 128

func newSchemaMigrationState(reader datastore.SchemaMigrationBoundaryReader) *schemaMigrationState {
	return &schemaMigrationState{
		reader:     reader,
		historical: otter.Must(&otter.Options[string, SchemaReader]{MaximumSize: historicalSchemaCacheSize}),
	}
}

// historicalSchema returns the legacy schema as of rev. A snapshot never changes,
// so each revision is materialized once; failures are not cached.
func (s *schemaMigrationState) historicalSchema(ctx context.Context, reader datastore.LegacySchemaReader, rev datastore.Revision) (SchemaReader, error) {
	return s.historical.Get(ctx, rev.String(), otter.LoaderFunc[string, SchemaReader](func(ctx context.Context, _ string) (SchemaReader, error) {
		return newHistoricalSchemaReader(ctx, reader, rev)
	}))
}

func (s *schemaMigrationState) loadBoundary(ctx context.Context) (datastore.SchemaMigrationBoundary, error) {
	if s == nil || s.reader == nil {
		return nil, nil
	}
	if cached := s.boundary.Load(); cached != nil {
		return *cached, nil
	}
	boundary, err := s.reader.ReadSchemaMigrationBoundary(ctx)
	if err != nil || boundary == nil {
		return nil, err
	}
	s.boundary.Store(&boundary)
	return boundary, nil
}

func (s *schemaMigrationState) useLegacy(ctx context.Context, rev datastore.Revision, hash SchemaHash) (bool, error) {
	boundary, err := s.loadBoundary(ctx)
	if err != nil {
		return false, err
	}
	if boundary != nil {
		return !boundary.ContainsRevision(rev), nil
	}
	// Stores without a marker can still receive tokens issued by legacy code.
	switch hash {
	case NoSchemaHashInLegacyZedToken, NoSchemaHashForLegacyCursor, NoSchemaHashInLegacyMode:
		return true, nil
	default:
		return false, nil
	}
}

// Materialize historical legacy definitions before graph traversal. Returning a
// lazy legacy reader here would reintroduce schema reads while relationship
// iterators hold connections, which the unified read path deliberately avoids.
func newHistoricalSchemaReader(ctx context.Context, reader datastore.LegacySchemaReader, rev datastore.Revision) (SchemaReader, error) {
	schema, err := legacyStoredSchema(ctx, reader)
	if err != nil {
		return nil, err
	}
	stored := datastore.NewReadOnlyStoredSchema(&core.StoredSchema{Version: 1, VersionOneof: &core.StoredSchema_V1{V1: schema}})
	return &storedSchemaReaderAdapter{storedSchema: stored, lastWrittenRevision: rev}, nil
}

// legacyStoredSchema builds the stored schema form of the legacy definitions.
func legacyStoredSchema(ctx context.Context, reader datastore.LegacySchemaReader) (*core.StoredSchema_V1StoredSchema, error) {
	namespaces, err := reader.LegacyListAllNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	caveats, err := reader.LegacyListAllCaveats(ctx)
	if err != nil {
		return nil, err
	}
	namespaceDefs := make(map[string]*core.NamespaceDefinition, len(namespaces))
	caveatDefs := make(map[string]*core.CaveatDefinition, len(caveats))
	definitions := make([]compiler.SchemaDefinition, 0, len(namespaces)+len(caveats))
	for _, namespace := range namespaces {
		namespaceDefs[namespace.Definition.Name] = namespace.Definition
		definitions = append(definitions, namespace.Definition)
	}
	for _, caveat := range caveats {
		caveatDefs[caveat.Definition.Name] = caveat.Definition
		definitions = append(definitions, caveat.Definition)
	}
	sort.Slice(definitions, func(i, j int) bool { return definitions[i].GetName() < definitions[j].GetName() })
	text, _, err := generator.GenerateSchema(ctx, definitions)
	if err != nil {
		return nil, err
	}
	hash, err := generator.ComputeSchemaHash(definitions)
	if err != nil {
		return nil, err
	}
	return &core.StoredSchema_V1StoredSchema{
		SchemaText: text, SchemaHash: hash, NamespaceDefinitions: namespaceDefs, CaveatDefinitions: caveatDefs,
	}, nil
}
