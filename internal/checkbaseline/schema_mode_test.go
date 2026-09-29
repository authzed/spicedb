package checkbaseline

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datalayer"
)

func TestSingleStoreBothEnginesAvoidSchemaLoads(t *testing.T) {
	datasets := GeneratedDatasets([]Scale{{Name: "schema", Fanout: 3, Depth: 3, DirectRelationships: 10}})
	cfg := AuditConfig{Policy: DefaultPolicy(), Repetitions: 2, DatasetPattern: ".*", CasePattern: ".*"}
	legacy, err := Audit(t.Context(), datasets, cfg)
	require.NoError(t, err)
	cfg.SchemaMode = "read-new-write-new"
	unified, err := Audit(t.Context(), datasets, cfg)
	require.NoError(t, err)
	require.Equal(t, "read-new-write-new", unified.Provenance["schema_mode"])
	require.Len(t, unified.Results, len(legacy.Results))
	for i, r := range unified.Results {
		require.True(t, r.Valid, r.Dataset.ID+"/"+r.Case.ID)
		require.Equal(t, legacy.Results[i].Dataset.Hash, r.Dataset.Hash)
		require.Positive(t, schemaLoadCount(legacy.Results[i].Engines[0].Work[0]), "legacy actually reads schema")
		for _, e := range r.Engines {
			for _, w := range e.Work {
				require.Zero(t, schemaLoadCount(w), e.Name+"/"+r.Dataset.ID)
			}
		}
	}
}

func schemaLoadCount(w Work) int {
	n := 0
	for _, e := range w.Events {
		if e.Operation == "schema-load" {
			n++
		}
	}
	return n
}

func TestSingleStoreColdAndWarmSchemaReads(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)
	defer ds.Close()
	_, err = datalayer.WriteStoredSchemaForTest(t.Context(), ds, "definition user {}")
	require.NoError(t, err)
	rev, err := ds.HeadRevision(t.Context())
	require.NoError(t, err)
	_, dl, closeCache, err := baselineDataLayers(ds, datalayer.SchemaModeReadNewWriteNew)
	require.NoError(t, err)
	defer closeCache()
	for i := 0; i < 2; i++ {
		record := NewRecorder()
		reader := dl.SnapshotReader(rev.Revision, datalayer.SchemaHash(rev.SchemaHash))
		_, err := reader.ReadSchema(WithRecorder(t.Context(), record))
		require.NoError(t, err)
		require.Equal(t, 1-i, schemaLoadCount(record.Seal()))
	}
}

func TestSingleStoreTraits(t *testing.T) {
	ds, err := FixtureDatasets("../..")
	require.NoError(t, err)
	a, err := Audit(t.Context(), ds, AuditConfig{SchemaMode: "read-new-write-new", Policy: DefaultPolicy(), Repetitions: 2, DatasetPattern: "^steelthread/document-with-traits.yaml$", CasePattern: ".*"})
	require.NoError(t, err)
	require.Len(t, a.Results, 5)
	for _, r := range a.Results {
		require.True(t, r.Valid)
		for _, e := range r.Engines {
			for _, w := range e.Work {
				require.Zero(t, schemaLoadCount(w))
			}
		}
	}
}

func TestSingleStorePostgres(t *testing.T) {
	uri := os.Getenv("CHECKBASELINE_POSTGRES_URI")
	if uri == "" {
		t.Skip("requires isolated PostgreSQL")
	}
	datasets := GeneratedDatasets([]Scale{{Name: "schema", Fanout: 3, Depth: 3, DirectRelationships: 10}})
	a, err := Audit(t.Context(), datasets, AuditConfig{Backend: "postgres", PostgresURI: uri, BackendMetadata: os.Getenv("CHECKBASELINE_BACKEND_METADATA"), SchemaMode: "read-new-write-new", Policy: DefaultPolicy(), Repetitions: 2, DatasetPattern: ".*", CasePattern: ".*"})
	require.NoError(t, err)
	require.NotEmpty(t, a.Results)
	for _, r := range a.Results {
		require.True(t, r.Valid)
		for _, e := range r.Engines {
			for _, w := range e.Work {
				require.Zero(t, schemaLoadCount(w), r.Dataset.ID+"/"+e.Name)
			}
		}
	}
}

func TestTimedSchemaLayerHasNoAuditInstrumentation(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)
	defer ds.Close()
	_, err = datalayer.WriteStoredSchemaForTest(t.Context(), ds, "definition user {}")
	require.NoError(t, err)
	rev, err := ds.HeadRevision(t.Context())
	require.NoError(t, err)
	timed, audit, closeCache, err := baselineDataLayers(ds, datalayer.SchemaModeReadNewWriteNew)
	require.NoError(t, err)
	defer closeCache()
	record := NewRecorder()
	_, err = timed.SnapshotReader(rev.Revision, datalayer.SchemaHash(rev.SchemaHash)).ReadSchema(WithRecorder(t.Context(), record))
	require.NoError(t, err)
	require.Zero(t, schemaLoadCount(record.Seal()), "timed layer must bypass schema audit instrumentation even on a cold load")
	auditRecord := NewRecorder()
	_, err = audit.SnapshotReader(rev.Revision, datalayer.SchemaHash(rev.SchemaHash)).ReadSchema(WithRecorder(t.Context(), auditRecord))
	require.NoError(t, err)
	require.Zero(t, schemaLoadCount(auditRecord.Seal()), "audit layer shares the cache populated by timing layer")
}
