package datalayer

import (
	"context"
	"sort"
	"sync"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
)

// The marker is immutable once the refresh commits. Cache it for the datalayer's
// lifetime; do not cache absence or failures while an upgrade is still running.
type schemaMigrationState struct {
	reader   datastore.SchemaMigrationBoundaryReader
	mu       sync.Mutex
	boundary datastore.SchemaMigrationBoundary // GUARDED_BY(mu)
}

func (s *schemaMigrationState) useLegacy(ctx context.Context, rev datastore.Revision, hash SchemaHash) (bool, error) {
	if s != nil && s.reader != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.boundary == nil {
			boundary, err := s.reader.ReadSchemaMigrationBoundary(ctx)
			if err != nil {
				return false, err
			}
			s.boundary = boundary
		}
		if s.boundary != nil {
			return !s.boundary.ContainsRevision(rev), nil
		}
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
	stored := datastore.NewReadOnlyStoredSchema(&core.StoredSchema{Version: 1, VersionOneof: &core.StoredSchema_V1{V1: &core.StoredSchema_V1StoredSchema{
		SchemaText: text, SchemaHash: hash, NamespaceDefinitions: namespaceDefs, CaveatDefinitions: caveatDefs,
	}}})
	return &storedSchemaReaderAdapter{storedSchema: stored, lastWrittenRevision: rev}, nil
}
