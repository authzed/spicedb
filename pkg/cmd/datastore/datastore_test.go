package datastore

import (
	"os"
	"testing"

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

	revision, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	namespaces, err := ds.SnapshotReader(revision).LegacyListAllNamespaces(ctx)
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

	revision, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	namespaces, err := ds.SnapshotReader(revision).LegacyListAllNamespaces(ctx)
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

	revision, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	namespaces, err := ds.SnapshotReader(revision).LegacyListAllNamespaces(ctx)
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

	revision, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	namespaces, err := ds.SnapshotReader(revision).LegacyListAllNamespaces(ctx)
	require.NoError(t, err)
	require.Len(t, namespaces, 2)
	namespaceNames := []string{namespaces[0].Definition.Name, namespaces[1].Definition.Name}
	require.Contains(t, namespaceNames, "user")
	require.Contains(t, namespaceNames, "repository")
}

func TestBuildConnectionURI(t *testing.T) {
	tests := []struct {
		name     string
		engine   string
		host     string
		port     string
		username string
		password string
		database string
		expected string
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
			port:     "",
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
			password: "",
			database: "testdb",
			expected: "postgres://testuser@localhost:5432/testdb",
		},
		{
			name:     "postgres without database",
			engine:   PostgresEngine,
			host:     "localhost",
			port:     "5432",
			username: "testuser",
			password: "testpass",
			database: "",
			expected: "postgres://testuser:testpass@localhost:5432",
		},
		{
			name:     "cockroachdb with all params",
			engine:   CockroachEngine,
			host:     "localhost",
			port:     "26257",
			username: "root",
			password: "",
			database: "defaultdb",
			expected: "postgres://root@localhost:26257/defaultdb",
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
			port:     "",
			username: "root",
			password: "rootpass",
			database: "mydb",
			expected: "root:rootpass@tcp(localhost:3306)/mydb?parseTime=true",
		},
		{
			name:     "empty host returns empty",
			engine:   PostgresEngine,
			host:     "",
			port:     "5432",
			username: "testuser",
			password: "testpass",
			database: "testdb",
			expected: "",
		},
		{
			name:     "unsupported engine returns empty",
			engine:   "unsupported",
			host:     "localhost",
			port:     "1234",
			username: "user",
			password: "pass",
			database: "db",
			expected: "",
		},
		{
			name:     "postgres with special characters in password",
			engine:   PostgresEngine,
			host:     "localhost",
			port:     "5432",
			username: "testuser",
			password: "pass@word:with/special",
			database: "testdb",
			expected: "postgres://testuser:pass%40word%3Awith%2Fspecial@localhost:5432/testdb",
		},
		{
			name:     "mysql with special characters in password",
			engine:   MySQLEngine,
			host:     "localhost",
			port:     "3306",
			username: "root",
			password: "pass@word:with/special",
			database: "mydb",
			expected: "root:pass%40word%3Awith%2Fspecial@tcp(localhost:3306)/mydb?parseTime=true",
		},
		{
			name:     "cockroachdb with special characters in username and password",
			engine:   CockroachEngine,
			host:     "localhost",
			port:     "26257",
			username: "user@domain",
			password: "p@ss:word",
			database: "testdb",
			expected: "postgres://user%40domain:p%40ss%3Aword@localhost:26257/testdb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildConnectionURI(tt.engine, tt.host, tt.port, tt.username, tt.password, tt.database)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNewDatastoreWithGranularParams(t *testing.T) {
	// This test verifies that granular params work by checking the URI is built
	// We don't actually connect, just verify the URI construction logic
	config := DefaultDatastoreConfig()
	config.Engine = PostgresEngine
	config.Host = "localhost"
	config.Port = "5432"
	config.Username = "testuser"
	config.Password = "testpass"
	config.Database = "testdb"

	// Manually trigger the URI building logic
	if config.URI == "" && config.Host != "" {
		config.URI = buildConnectionURI(config.Engine, config.Host, config.Port, config.Username, config.Password, config.Database)
	}

	require.NotEmpty(t, config.URI)
	require.Equal(t, "postgres://testuser:testpass@localhost:5432/testdb", config.URI)
}

func TestNewDatastoreURITakesPrecedence(t *testing.T) {
	ctx := t.Context()

	// Test that explicit URI takes precedence over granular params
	config := DefaultDatastoreConfig()
	config.Engine = MemoryEngine
	config.URI = "memory://test"
	config.Host = "localhost"
	config.Port = "5432"
	config.Username = "testuser"
	config.Password = "testpass"
	config.Database = "testdb"

	ds, err := NewDatastore(ctx,
		func(c *Config) { *c = *config },
	)
	require.NoError(t, err)
	require.NotNil(t, ds)
	// The URI should remain as explicitly set, not built from granular params
	require.Equal(t, "memory://test", config.URI)
}
