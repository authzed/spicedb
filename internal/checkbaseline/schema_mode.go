package checkbaseline

import (
	"context"
	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// Match server wiring: unified reads plus its standard 32 MiB stored-schema cache.
// The same data layer and cache serve both engines for a dataset/profile.
func baselineDataLayers(ds datastore.Datastore, mode datalayer.SchemaMode) (datalayer.DataLayer, datalayer.DataLayer, func(), error) {
	opts := []datalayer.DataLayerOption{datalayer.WithSchemaMode(mode)}
	close := func() {}
	if mode.ReadsFromNew() {
		c, err := cache.NewStandardCache[datalayer.SchemaCacheKey, *datastore.ReadOnlyStoredSchema](&cache.Config{MaxCost: 32 << 20})
		if err != nil {
			return nil, nil, nil, err
		}
		opts = append(opts, datalayer.WithSchemaCache(c))
		close = c.Close
	}
	return datalayer.NewDataLayer(ds, opts...), datalayer.NewDataLayer(&schemaObservedDatastore{ds}, opts...), close, nil
}

type schemaObservedDatastore struct{ datastore.Datastore }

func (d *schemaObservedDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	return &schemaObservedReader{d.Datastore.SnapshotReader(rev)}
}

type schemaObservedReader struct{ datastore.Reader }

func (r *schemaObservedReader) ReadStoredSchema(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
	event(ctx, "schema-load", "stored", nil, 0)
	return r.Reader.ReadStoredSchema(ctx)
}
func (r *schemaObservedReader) LegacyReadNamespaceByName(ctx context.Context, name string) (*core.NamespaceDefinition, datastore.Revision, error) {
	event(ctx, "schema-load", "namespace", nil, 0)
	return r.Reader.LegacyReadNamespaceByName(ctx, name)
}
func (r *schemaObservedReader) LegacyReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	event(ctx, "schema-load", "caveat", nil, 0)
	return r.Reader.LegacyReadCaveatByName(ctx, name)
}
func (r *schemaObservedReader) LegacyListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	event(ctx, "schema-load", "list-namespaces", nil, 0)
	return r.Reader.LegacyListAllNamespaces(ctx)
}
func (r *schemaObservedReader) LegacyListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	event(ctx, "schema-load", "list-caveats", nil, 0)
	return r.Reader.LegacyListAllCaveats(ctx)
}
func (r *schemaObservedReader) LegacyLookupNamespacesWithNames(ctx context.Context, names []string) ([]datastore.RevisionedNamespace, error) {
	event(ctx, "schema-load", "lookup-namespaces", nil, 0)
	return r.Reader.LegacyLookupNamespacesWithNames(ctx, names)
}
func (r *schemaObservedReader) LegacyLookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	event(ctx, "schema-load", "lookup-caveats", nil, 0)
	return r.Reader.LegacyLookupCaveatsWithNames(ctx, names)
}
