package checkbaseline

import (
	"context"
	"fmt"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestPostgresAdminLocalOnly(t *testing.T) {
	for _, uri := range []string{"postgres://postgres@remote.example/postgres", "postgres://postgres@127.0.0.1/existing", "postgres://postgres@localhost/postgres?host=remote.example", ""} {
		_, err := postgresAdminConfig(uri)
		require.Error(t, err, uri)
	}
	_, err := postgresAdminConfig("postgres://postgres@127.0.0.1:5432/postgres?sslmode=disable")
	require.NoError(t, err)
}
func TestPostgresBaselineIntegration(t *testing.T) {
	uri := os.Getenv("CHECKBASELINE_POSTGRES_URI")
	if uri == "" {
		t.Skip("requires isolated local PostgreSQL")
	}
	admin, err := pgx.Connect(t.Context(), uri)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	var before int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM pg_database WHERE datname LIKE 'checkbaseline_%'").Scan(&before))
	t.Cleanup(func() {
		var after int
		require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM pg_database WHERE datname LIKE 'checkbaseline_%'").Scan(&after))
		require.Equal(t, before, after, "owned databases cleaned up")
	})
	datasets := GeneratedDatasets([]Scale{{Name: "pgtest", Fanout: 3, Depth: 3, DirectRelationships: 10}})
	cfg := AuditConfig{Policy: DefaultPolicy(), Repetitions: 1, DatasetPattern: ".*", CasePattern: ".*"}
	mem, err := Audit(t.Context(), datasets, cfg)
	require.NoError(t, err)
	cfg.Backend = "postgres"
	cfg.PostgresURI = uri
	cfg.BackendMetadata = os.Getenv("CHECKBASELINE_BACKEND_METADATA")
	cfg.Profiles = []string{"postgres"}
	pg, err := Audit(t.Context(), datasets, cfg)
	require.NoError(t, err)
	require.Len(t, pg.Results, len(mem.Results))
	require.Equal(t, "postgres", pg.Provenance["backend"])
	require.NotEmpty(t, pg.Provenance["backend_version"])
	failure := []Dataset{{ID: "failure", Setup: func(context.Context, datastore.Datastore) ([]Case, error) {
		return nil, fmt.Errorf("deliberate setup failure")
	}}}
	failed, err := Audit(t.Context(), failure, cfg)
	require.Error(t, err)
	require.Contains(t, failed.Omissions[0], "deliberate setup failure")
	for i, r := range pg.Results {
		require.True(t, r.Valid, r.Dataset.ID)
		require.Equal(t, "relationship work matched", r.Status, r.Dataset.ID+"/"+r.Case.ID)
		require.Equal(t, mem.Results[i].Dataset.Hash, r.Dataset.Hash, r.Dataset.ID)
		require.Positive(t, r.Dataset.Database.RelationshipRows)
		require.EqualValues(t, r.Dataset.Relationships, r.Dataset.Database.RelationshipRows)
		require.Positive(t, r.Dataset.Database.TableBytes)
		require.Positive(t, r.Dataset.Database.IndexBytes)
	}
}

func TestPostgresRejectsImplicitSettings(t *testing.T) {
	t.Setenv("PGOPTIONS", "-c enable_indexscan=off")
	require.Error(t, validatePostgresEnvironment())
}
func TestPostgresMetadataRequired(t *testing.T) {
	for _, metadata := range []string{"", "{}", `{"image":"test"}`} {
		require.Error(t, validateBackendMetadata(metadata))
	}
}
