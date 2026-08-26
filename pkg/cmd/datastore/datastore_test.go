package datastore

import (
	"os"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestDefaults(t *testing.T) {
	f := pflag.FlagSet{}
	expected := NewConfigWithOptionsAndDefaults()
	err := RegisterDatastoreFlagsWithPrefix(&f, "", expected)
	require.NoError(t, err)
	received := DefaultDatastoreConfig()
	require.Equal(t, expected, received)
}

func TestLoadDatastoreFromFileContents(t *testing.T) {
	ctx := t.Context()
	ds, err := NewDatastore(ctx,
		SetBootstrapFileContents(map[string][]byte{"test": []byte("schema: definition user{}")}),
		WithEngine(MemoryEngine))
	require.NoError(t, err)
	t.Cleanup(func() {
		ds.Close()
	})

	revisionResult, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	namespaces, err := ds.SnapshotReader(revisionResult.Revision).LegacyListAllNamespaces(ctx)
	require.NoError(t, err)
	require.Len(t, namespaces, 1)
	require.Equal(t, "user", namespaces[0].Definition.Name)
}

func TestLoadDatastoreFromFile(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "")
	require.NoError(t, err)
	_, err = file.Write([]byte("schema: definition user{}"))
	require.NoError(t, err)

	ctx := t.Context()
	ds, err := NewDatastore(ctx,
		SetBootstrapFiles([]string{file.Name()}),
		WithEngine(MemoryEngine))
	require.NoError(t, err)
	t.Cleanup(func() {
		ds.Close()
	})

	revisionResult, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	namespaces, err := ds.SnapshotReader(revisionResult.Revision).LegacyListAllNamespaces(ctx)
	require.NoError(t, err)
	require.Len(t, namespaces, 1)
	require.Equal(t, "user", namespaces[0].Definition.Name)
}

// NOTE: this test captured a segfault in https://github.com/authzed/spicedb/issues/2783
func TestLoadDatastoreFromFileWithCaveats(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "")
	require.NoError(t, err)
	_, err = file.Write([]byte(`
schema: |-
  
  definition user {}
  
  caveat mfa_match_multi(acceptable_amr list<string>, provided_amr list<string>) {
     size(acceptable_amr) == 0 || (size(provided_amr) > 0 && acceptable_amr.exists(x, x in provided_amr))
  }

  definition organization {
    relation mfa_guard: organization with mfa_match_multi
    relation check: user:*
      
    permission secured_access = mfa_guard->check
  }
  
relationships: |-
  organization:orga#mfa_guard@organization:orga[mfa_match_multi:{"acceptable_amr": ["mfa"]}]`))
	require.NoError(t, err)

	ctx := t.Context()
	ds, err := NewDatastore(ctx,
		SetBootstrapFiles([]string{file.Name()}),
		WithEngine(MemoryEngine))
	require.NoError(t, err)
	t.Cleanup(func() {
		ds.Close()
	})

	revisionResult, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	namespaces, err := ds.SnapshotReader(revisionResult.Revision).LegacyListAllNamespaces(ctx)
	require.NoError(t, err)
	require.Len(t, namespaces, 2)
	require.Equal(t, "organization", namespaces[0].Definition.Name)
}

func TestLoadDatastoreFromFileAndContents(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "")
	require.NoError(t, err)
	_, err = file.Write([]byte("schema: definition repository{}"))
	require.NoError(t, err)

	ctx := t.Context()
	ds, err := NewDatastore(ctx,
		SetBootstrapFiles([]string{file.Name()}),
		SetBootstrapFileContents(map[string][]byte{"test": []byte("schema: definition user{}")}),
		WithEngine(MemoryEngine))
	require.NoError(t, err)

	revisionResult, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	namespaces, err := ds.SnapshotReader(revisionResult.Revision).LegacyListAllNamespaces(ctx)
	require.NoError(t, err)
	require.Len(t, namespaces, 2)
	namespaceNames := []string{namespaces[0].Definition.Name, namespaces[1].Definition.Name}
	require.Contains(t, namespaceNames, "user")
	require.Contains(t, namespaceNames, "repository")
}

//nolint:gosec // the credentials in the table below are fixtures, not real secrets
func TestBuildConnectionURI(t *testing.T) {
	tests := []struct {
		name        string
		engine      string
		host        string
		port        string
		username    string
		password    string
		database    string
		connParams  map[string]string
		expected    string
		expectedErr string
	}{
		{
			name:     "postgres with all params",
			engine:   PostgresEngine,
			host:     "localhost",
			port:     "5432",
			username: "testuser",
			password: "testpass",
			database: "testdb",
			expected: "postgres://testuser:testpass@localhost:5432/testdb",
		},
		{
			name:     "postgres with default port",
			engine:   PostgresEngine,
			host:     "localhost",
			username: "testuser",
			password: "testpass",
			database: "testdb",
			expected: "postgres://testuser:testpass@localhost:5432/testdb",
		},
		{
			name:     "postgres without password",
			engine:   PostgresEngine,
			host:     "localhost",
			port:     "5432",
			username: "testuser",
			database: "testdb",
			expected: "postgres://testuser@localhost:5432/testdb",
		},
		{
			name:     "postgres without username or password",
			engine:   PostgresEngine,
			host:     "localhost",
			port:     "5432",
			database: "testdb",
			expected: "postgres://localhost:5432/testdb",
		},
		{
			name:     "postgres without database",
			engine:   PostgresEngine,
			host:     "localhost",
			port:     "5432",
			username: "testuser",
			password: "testpass",
			expected: "postgres://testuser:testpass@localhost:5432",
		},
		{
			name:     "cockroachdb with all params",
			engine:   CockroachEngine,
			host:     "localhost",
			port:     "26257",
			username: "root",
			password: "rootpass",
			database: "defaultdb",
			expected: "postgres://root:rootpass@localhost:26257/defaultdb",
		},
		{
			name:     "cockroachdb with default port",
			engine:   CockroachEngine,
			host:     "localhost",
			username: "root",
			password: "rootpass",
			database: "defaultdb",
			expected: "postgres://root:rootpass@localhost:26257/defaultdb",
		},
		{
			name:     "mysql with all params",
			engine:   MySQLEngine,
			host:     "localhost",
			port:     "3306",
			username: "root",
			password: "rootpass",
			database: "mydb",
			expected: "root:rootpass@tcp(localhost:3306)/mydb?parseTime=true",
		},
		{
			name:     "mysql with default port",
			engine:   MySQLEngine,
			host:     "localhost",
			username: "root",
			password: "rootpass",
			database: "mydb",
			expected: "root:rootpass@tcp(localhost:3306)/mydb?parseTime=true",
		},
		{
			name:     "mysql without password",
			engine:   MySQLEngine,
			host:     "localhost",
			port:     "3306",
			username: "root",
			database: "mydb",
			expected: "root@tcp(localhost:3306)/mydb?parseTime=true",
		},
		{
			name:       "postgres with connection params",
			engine:     PostgresEngine,
			host:       "localhost",
			port:       "5432",
			username:   "testuser",
			password:   "testpass",
			database:   "testdb",
			connParams: map[string]string{"sslmode": "require", "application_name": "spicedb"},
			expected:   "postgres://testuser:testpass@localhost:5432/testdb?application_name=spicedb&sslmode=require",
		},
		{
			name:       "cockroachdb with connection params",
			engine:     CockroachEngine,
			host:       "localhost",
			port:       "26257",
			username:   "root",
			database:   "defaultdb",
			connParams: map[string]string{"sslmode": "verify-full", "sslrootcert": "/certs/ca.crt"},
			expected:   "postgres://root@localhost:26257/defaultdb?sslmode=verify-full&sslrootcert=%2Fcerts%2Fca.crt",
		},
		{
			name:       "mysql with connection params",
			engine:     MySQLEngine,
			host:       "localhost",
			port:       "3306",
			username:   "root",
			password:   "rootpass",
			database:   "mydb",
			connParams: map[string]string{"tls": "skip-verify", "charset": "utf8mb4"},
			expected:   "root:rootpass@tcp(localhost:3306)/mydb?parseTime=true&charset=utf8mb4&tls=skip-verify",
		},
		{
			name:       "mysql connection params can override parseTime",
			engine:     MySQLEngine,
			host:       "localhost",
			port:       "3306",
			username:   "root",
			database:   "mydb",
			connParams: map[string]string{"parseTime": "false"},
			expected:   "root@tcp(localhost:3306)/mydb",
		},
		{
			name:        "mysql rejects a non-boolean parseTime",
			engine:      MySQLEngine,
			host:        "localhost",
			port:        "3306",
			username:    "root",
			database:    "mydb",
			connParams:  map[string]string{"parseTime": "yes-please"},
			expectedErr: "invalid value for connection parameter `parseTime`",
		},
		{
			name:        "unsupported engine returns an error",
			engine:      MemoryEngine,
			host:        "localhost",
			port:        "1234",
			username:    "user",
			password:    "pass",
			database:    "db",
			expectedErr: `datastore engine "memory" does not support granular connection parameters`,
		},
		{
			name:     "postgres with special characters in credentials",
			engine:   PostgresEngine,
			host:     "localhost",
			port:     "5432",
			username: "user@domain",
			password: "pass@word:with/special",
			database: "testdb",
			expected: "postgres://user%40domain:pass%40word%3Awith%2Fspecial@localhost:5432/testdb",
		},
		{
			name:     "postgres with a space in the password",
			engine:   PostgresEngine,
			host:     "localhost",
			port:     "5432",
			username: "testuser",
			password: "pass word",
			database: "testdb",
			// url.QueryEscape would encode the space as "+", which userinfo reads literally.
			expected: "postgres://testuser:pass%20word@localhost:5432/testdb",
		},
		{
			name:     "mysql credentials are written verbatim",
			engine:   MySQLEngine,
			host:     "localhost",
			port:     "3306",
			username: "user@domain",
			password: "pass@word:with/special",
			database: "mydb",
			// The MySQL DSN is not a URL and the driver does not percent-decode,
			// so encoding the credentials here would change them.
			expected: "user@domain:pass@word:with/special@tcp(localhost:3306)/mydb?parseTime=true",
		},
		{
			name:     "postgres with an IPv6 host",
			engine:   PostgresEngine,
			host:     "::1",
			port:     "5432",
			username: "testuser",
			database: "testdb",
			expected: "postgres://testuser@[::1]:5432/testdb",
		},
		{
			name:     "mysql with an IPv6 host",
			engine:   MySQLEngine,
			host:     "::1",
			port:     "3306",
			username: "root",
			database: "mydb",
			expected: "root@tcp([::1]:3306)/mydb?parseTime=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := buildConnectionURI(tt.engine, tt.host, tt.port, tt.username, tt.password, tt.database, tt.connParams)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestBuildConnectionURIParsesForDriver asserts that the URIs produced from granular
// parameters are actually understood by the drivers that consume them, including the
// credentials, which are the easiest part to encode incorrectly.
func TestBuildConnectionURIParsesForDriver(t *testing.T) {
	const (
		username = "user@domain"
		password = "pass@word:with/special"
	)

	t.Run("postgres", func(t *testing.T) {
		uri, err := buildConnectionURI(PostgresEngine, "localhost", "5432", username, password, "testdb", map[string]string{"sslmode": "require"})
		require.NoError(t, err)

		cfg, err := pgx.ParseConfig(uri)
		require.NoError(t, err)
		require.Equal(t, username, cfg.User)
		require.Equal(t, password, cfg.Password)
		require.Equal(t, "testdb", cfg.Database)
		require.Equal(t, "localhost", cfg.Host)
		require.Equal(t, uint16(5432), cfg.Port)
	})

	t.Run("mysql", func(t *testing.T) {
		dsn, err := buildConnectionURI(MySQLEngine, "localhost", "3306", username, password, "mydb", map[string]string{"charset": "utf8mb4"})
		require.NoError(t, err)

		cfg, err := mysqldriver.ParseDSN(dsn)
		require.NoError(t, err)
		require.Equal(t, username, cfg.User)
		require.Equal(t, password, cfg.Passwd)
		require.Equal(t, "mydb", cfg.DBName)
		require.Equal(t, "localhost:3306", cfg.Addr)
		require.True(t, cfg.ParseTime)
	})
}

//nolint:gosec // the credentials below are fixtures, not real secrets
func TestResolveConnectionURI(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectedURI string
		expectedErr string
	}{
		{
			name:        "no uri and no granular params leaves the uri empty",
			config:      &Config{Engine: PostgresEngine},
			expectedURI: "",
		},
		{
			name:        "uri alone is left untouched",
			config:      &Config{Engine: PostgresEngine, URI: "postgres://user:pass@example.com:5432/spicedb"},
			expectedURI: "postgres://user:pass@example.com:5432/spicedb",
		},
		{
			name:        "granular params alone build the uri",
			config:      &Config{Engine: PostgresEngine, Host: "localhost", Username: "testuser", Password: "testpass", Database: "testdb"},
			expectedURI: "postgres://testuser:testpass@localhost:5432/testdb",
		},
		{
			name:        "uri and host together are rejected",
			config:      &Config{Engine: PostgresEngine, URI: "postgres://example.com/spicedb", Host: "localhost"},
			expectedErr: "cannot specify both --datastore-conn-uri and --datastore-host",
		},
		{
			name:        "uri and password together are rejected",
			config:      &Config{Engine: PostgresEngine, URI: "postgres://example.com/spicedb", Password: "testpass"},
			expectedErr: "cannot specify both --datastore-conn-uri and --datastore-password",
		},
		{
			name:        "uri and conn params together are rejected",
			config:      &Config{Engine: PostgresEngine, URI: "postgres://example.com/spicedb", ConnParams: map[string]string{"sslmode": "require"}},
			expectedErr: "cannot specify both --datastore-conn-uri and --datastore-conn-param",
		},
		{
			name:        "every conflicting flag is named",
			config:      &Config{Engine: PostgresEngine, URI: "postgres://example.com/spicedb", Host: "localhost", Port: "5432", Username: "u", Password: "p", Database: "d", ConnParams: map[string]string{"sslmode": "require"}},
			expectedErr: "cannot specify both --datastore-conn-uri and --datastore-host, --datastore-port, --datastore-username, --datastore-password, --datastore-database, --datastore-conn-param",
		},
		{
			name:        "granular params without a host are rejected",
			config:      &Config{Engine: PostgresEngine, Username: "testuser", Password: "testpass"},
			expectedErr: "--datastore-host is required when configuring the connection with --datastore-username, --datastore-password",
		},
		{
			name:        "an engine without granular support is rejected",
			config:      &Config{Engine: SpannerEngine, Host: "localhost"},
			expectedErr: `datastore engine "spanner" does not support granular connection parameters`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := resolveConnectionURI(tt.config)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expectedURI, tt.config.URI)
		})
	}
}

func TestNewDatastoreRejectsURIWithGranularParams(t *testing.T) {
	_, err := NewDatastore(t.Context(),
		WithEngine(MemoryEngine),
		WithURI("memory://test"),
		WithHost("localhost"),
	)
	require.ErrorContains(t, err, "cannot specify both --datastore-conn-uri and --datastore-host")
}

func TestNewDatastoreRejectsUnsupportedEngineForGranularParams(t *testing.T) {
	_, err := NewDatastore(t.Context(),
		WithEngine(MemoryEngine),
		WithHost("localhost"),
	)
	require.ErrorContains(t, err, `datastore engine "memory" does not support granular connection parameters`)
}

func TestRegisterDatastoreFlagsIncludesGranularFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	opts := DefaultDatastoreConfig()
	require.NoError(t, RegisterDatastoreFlagsWithPrefix(flagSet, "", opts))

	for _, name := range []string{
		"datastore-host",
		"datastore-port",
		"datastore-username",
		"datastore-password",
		"datastore-database",
		"datastore-conn-param",
	} {
		require.NotNil(t, flagSet.Lookup(name), "expected flag %s to be registered", name)
	}

	require.NoError(t, flagSet.Parse([]string{
		"--datastore-engine=postgres",
		"--datastore-host=db.example.com",
		"--datastore-port=6432",
		"--datastore-username=spicedb",
		"--datastore-password=secret",
		"--datastore-database=spicedb",
		"--datastore-conn-param=sslmode=require",
	}))

	require.NoError(t, resolveConnectionURI(opts))
	require.Equal(t, "postgres://spicedb:secret@db.example.com:6432/spicedb?sslmode=require", opts.URI)
}
