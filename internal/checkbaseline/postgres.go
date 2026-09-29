package checkbaseline

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

// DatabaseInfo describes physical PostgreSQL storage after load and VACUUM ANALYZE.
// These bytes are distinct from the canonical input and consumed-row encodings.
type DatabaseInfo struct {
	RelationshipRows, TableBytes, IndexBytes, DatabaseBytes int64
	SetupSeconds                                            float64
	IndexDefinitions                                        []string
}
type baselineBackend struct {
	ds        datastore.Datastore
	metadata  map[string]string
	afterLoad func(context.Context) (*DatabaseInfo, error)
	close     func() error
}

func validatePostgresEnvironment() error {
	for _, key := range []string{"PGOPTIONS", "PGSERVICE", "PGSERVICEFILE"} {
		if os.Getenv(key) != "" {
			return fmt.Errorf("%s is unsupported for reproducible PostgreSQL benchmarks", key)
		}
	}
	return nil
}

func validateBackendMetadata(value string) error {
	var m struct {
		Image, Architecture, DockerVersion, Transport string
		CPUs                                          float64
		MemoryBytes, VMCPUs, VMMemoryBytes            int64
	}
	if err := json.Unmarshal([]byte(value), &m); err != nil {
		return errors.New("CHECKBASELINE_BACKEND_METADATA must be valid deployment JSON")
	}
	if m.Image == "" || m.Architecture == "" || m.DockerVersion == "" || m.Transport == "" || m.CPUs <= 0 || m.MemoryBytes <= 0 || m.VMCPUs <= 0 || m.VMMemoryBytes <= 0 {
		return errors.New("backend metadata requires image, architecture, Docker version, transport, container and VM resources")
	}
	return nil
}

func postgresAdminConfig(uri string) (*pgx.ConnConfig, error) {
	u, err := url.Parse(uri)
	if err != nil || u == nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Path != "/postgres" {
		return nil, errors.New("benchmark requires a local PostgreSQL admin URI targeting /postgres")
	}
	switch u.Hostname() {
	case "127.0.0.1", "::1", "localhost":
	default:
		return nil, errors.New("benchmark PostgreSQL host must be loopback")
	}
	for k := range u.Query() {
		if k != "sslmode" {
			return nil, fmt.Errorf("unsupported PostgreSQL URI option %q", k)
		}
	}
	cfg, err := pgx.ParseConfig(uri)
	if err != nil {
		return nil, errors.New("invalid PostgreSQL admin configuration")
	}
	return cfg, nil
}

func openBaselineBackend(ctx context.Context, cfg AuditConfig) (*baselineBackend, error) {
	if cfg.Backend == "" || cfg.Backend == "memdb" {
		ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
		if err != nil {
			return nil, err
		}
		return &baselineBackend{ds: ds, metadata: map[string]string{"backend": "memdb"}, close: ds.Close}, nil
	}
	if cfg.Backend != "postgres" {
		return nil, fmt.Errorf("unknown backend %q", cfg.Backend)
	}
	if err := validatePostgresEnvironment(); err != nil {
		return nil, err
	}
	if err := validateBackendMetadata(cfg.BackendMetadata); err != nil {
		return nil, err
	}
	adminConfig, err := postgresAdminConfig(cfg.PostgresURI)
	if err != nil {
		return nil, err
	}
	admin, err := pgx.ConnectConfig(ctx, adminConfig)
	if err != nil {
		return nil, err
	}
	var nonce [12]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		admin.Close(ctx)
		return nil, err
	}
	name := "checkbaseline_" + hex.EncodeToString(nonce[:])
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" TEMPLATE template0"); err != nil {
		admin.Close(ctx)
		return nil, err
	}
	var ds datastore.Datastore
	var sql *pgx.Conn
	cleanup := func() error {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var first error
		if ds != nil {
			first = ds.Close()
		}
		if sql != nil {
			if e := sql.Close(closeCtx); first == nil {
				first = e
			}
		}
		_, e := admin.Exec(closeCtx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize())
		if first == nil {
			first = e
		}
		if e = admin.Close(closeCtx); first == nil {
			first = e
		}
		return first
	}
	success := false
	defer func() {
		if !success {
			_ = cleanup()
		}
	}()
	u, _ := url.Parse(cfg.PostgresURI)
	u.Path = "/" + name
	driver, err := migrations.NewAlembicPostgresDriver(ctx, u.String(), nil, false)
	if err != nil {
		return nil, err
	}
	defer driver.Close(context.Background())
	err = migrations.DatabaseMigrations.Run(context.WithValue(ctx, migrate.BackfillBatchSize, uint64(1000)), driver, migrate.Head, migrate.LiveRun)
	closeErr := driver.Close(ctx)
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	ds, err = postgres.NewPostgresDatastore(ctx, u.String(), postgres.GCEnabled(false), postgres.WithRevisionHeartbeat(false), postgres.ReadConnsMinOpen(1), postgres.ReadConnsMaxOpen(1), postgres.WriteConnsMinOpen(1), postgres.WriteConnsMaxOpen(1), postgres.RevisionQuantization(0))
	if err != nil {
		return nil, err
	}
	sql, err = pgx.Connect(ctx, u.String())
	if err != nil {
		return nil, err
	}
	var version string
	if err = sql.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return nil, err
	}
	settings := map[string]string{"pool_read_max": "1", "pool_write_max": "1", "gc": "disabled", "heartbeat": "disabled", "cache_policy": "warm; shared datastore pools; no buffer flush between engines", "preparation": "VACUUM ANALYZE after load; dataset export and warmup before timing", "container": cfg.BackendMetadata}
	rows, err := sql.Query(ctx, `SELECT name,setting||COALESCE(unit,'') FROM pg_settings ORDER BY name`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k, v string
		if err = rows.Scan(&k, &v); err != nil {
			rows.Close()
			return nil, err
		}
		settings[k] = v
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	encoded, _ := json.Marshal(settings)
	b := &baselineBackend{ds: ds, close: cleanup, metadata: map[string]string{"backend": "postgres", "backend_version": version, "backend_settings": string(encoded)}}
	b.afterLoad = func(ctx context.Context) (*DatabaseInfo, error) {
		if _, err := sql.Exec(ctx, "VACUUM ANALYZE"); err != nil {
			return nil, err
		}
		info := &DatabaseInfo{}
		err := sql.QueryRow(ctx, `SELECT (SELECT count(*) FROM relation_tuple),pg_table_size('relation_tuple'),pg_indexes_size('relation_tuple'),pg_database_size(current_database())`).Scan(&info.RelationshipRows, &info.TableBytes, &info.IndexBytes, &info.DatabaseBytes)
		if err != nil {
			return nil, err
		}
		rows, err := sql.Query(ctx, "SELECT indexdef FROM pg_indexes WHERE tablename='relation_tuple' ORDER BY indexname")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var def string
			if err := rows.Scan(&def); err != nil {
				return nil, err
			}
			info.IndexDefinitions = append(info.IndexDefinitions, def)
		}
		return info, rows.Err()
	}
	success = true
	return b, nil
}
