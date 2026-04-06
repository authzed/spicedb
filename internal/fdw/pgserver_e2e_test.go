//go:build ci && !skipintegrationtests

package fdw_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/fdw"
	"github.com/authzed/spicedb/pkg/cmd/datastore"
	spicedbserver "github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/util"
)

const (
	pgVersion                  = "17.2"
	dockerBootTimeout          = 10 * time.Second
	postgresTestUser           = "postgres"
	postgresTestPassword       = "secret"
	postgresTestPort           = "5432"
	postgresTestMaxConnections = "3000"
	pgbouncerTestPort          = "6432"
	fdwPassword                = "proxypassword"
)

type qar struct {
	name                string
	query               string
	args                []any
	expectedRows        [][]any
	expectedResponseTag string
	expectedError       string
}

type e2eTestCase struct {
	name          string
	schema        string
	relationships []string
	queries       []qar
}

func TestEndToEnd(t *testing.T) {
	tcs := []e2eTestCase{
		{
			name: "simple",
			schema: `definition user {}

			definition document {
				relation viewer: user
			}
			`,
			relationships: []string{
				"document:firstdoc#viewer@user:tom",
			},
			queries: []qar{
				{
					name:  "single result",
					query: `SELECT resource_id FROM relationships WHERE resource_type = $1`,
					args:  []any{"document"},
					expectedRows: [][]any{
						{"firstdoc"},
					},
					expectedResponseTag: "SELECT 1",
				},
				{
					name:                "no results",
					query:               `SELECT resource_id FROM relationships WHERE resource_id = $1`,
					args:                []any{"notfound"},
					expectedRows:        [][]any{},
					expectedResponseTag: "SELECT 0",
				},
				{
					name:          "no results with unknown type",
					query:         `SELECT resource_id FROM relationships WHERE resource_type = $1`,
					args:          []any{"unknown"},
					expectedError: "object definition `unknown` not found",
				},
			},
		},
		{
			name: "schema read/write",
			schema: `definition user {}

definition document {
	relation reader: user
	relation writer: user
	permission edit = writer
	permission view = reader + edit
}`,
			queries: []qar{
				{
					name:  "read schema",
					query: `SELECT * FROM schema`,
					expectedRows: [][]any{
						{`definition document {
	relation reader: user
	relation writer: user
	permission edit = writer
	permission view = reader + edit
}

definition user {}`},
					},
					expectedResponseTag: "SELECT 1",
				},
				{
					name:  "write new schema",
					query: `INSERT INTO schema VALUES ($1)`,
					args: []any{`definition user {}

definition document {
	relation reader: user
	relation writer: user
	relation owner: user
	permission edit = writer + owner
	permission view = reader + edit
}`},
					expectedRows:        [][]any{},
					expectedResponseTag: "INSERT 0 1",
				},
				{
					name:  "read updated schema",
					query: `SELECT * FROM schema`,
					expectedRows: [][]any{
						{`definition document {
	relation reader: user
	relation writer: user
	relation owner: user
	permission edit = writer + owner
	permission view = reader + edit
}

definition user {}`},
					},
					expectedResponseTag: "SELECT 1",
				},
			},
		},
		{
			name: "relationships read/write/delete",
			schema: `definition user {}

definition document {
	relation reader: user
	relation writer: user
}`,
			relationships: []string{
				"document:firstdoc#reader@user:tom",
				"document:firstdoc#reader@user:jerry",
				"document:seconddoc#reader@user:jerry",
			},
			queries: []qar{
				{
					name:  "select all relationships by resource_type",
					query: `SELECT resource_type, resource_id, relation, subject_type, subject_id, optional_subject_relation, caveat_name, caveat_context FROM relationships WHERE resource_type = $1 ORDER BY resource_id, subject_id`,
					args:  []any{"document"},
					expectedRows: [][]any{
						{"document", "firstdoc", "reader", "user", "jerry", "", "", ""},
						{"document", "firstdoc", "reader", "user", "tom", "", "", ""},
						{"document", "seconddoc", "reader", "user", "jerry", "", "", ""},
					},
					expectedResponseTag: "SELECT 3",
				},
				{
					name:  "select relationships by resource_id",
					query: `SELECT subject_id FROM relationships WHERE resource_type = $1 AND resource_id = $2 ORDER BY subject_id`,
					args:  []any{"document", "firstdoc"},
					expectedRows: [][]any{
						{"jerry"},
						{"tom"},
					},
					expectedResponseTag: "SELECT 2",
				},
				{
					name:                "insert new relationship",
					query:               `INSERT INTO relationships VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
					args:                []any{"document", "thirddoc", "reader", "user", "sarah", "", "", "", ""},
					expectedRows:        [][]any{},
					expectedResponseTag: "INSERT 0 1",
				},
				{
					name:  "verify inserted relationship",
					query: `SELECT subject_id FROM relationships WHERE resource_type = $1 AND resource_id = $2`,
					args:  []any{"document", "thirddoc"},
					expectedRows: [][]any{
						{"sarah"},
					},
					expectedResponseTag: "SELECT 1",
				},
				{
					name:                "delete relationship",
					query:               `DELETE FROM relationships WHERE resource_type = $1 AND resource_id = $2 AND relation = $3 AND subject_type = $4 AND subject_id = $5`,
					args:                []any{"document", "seconddoc", "reader", "user", "jerry"},
					expectedRows:        [][]any{},
					expectedResponseTag: "DELETE 1",
				},
				{
					name:                "verify deleted relationship",
					query:               `SELECT resource_id FROM relationships WHERE resource_type = $1 AND resource_id = $2`,
					args:                []any{"document", "seconddoc"},
					expectedRows:        [][]any{},
					expectedResponseTag: "SELECT 0",
				},
			},
		},
		{
			name: "insert returning zedtoken",
			schema: `definition user {}

definition document {
	relation reader: user
}`,
			queries: []qar{
				{
					name:                "insert with returning",
					query:               `INSERT INTO relationships VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING consistency`,
					args:                []any{"document", "testdoc", "reader", "user", "alice", "", "", ""},
					expectedResponseTag: "INSERT 0 1",
					// We expect one row with a non-empty consistency token
					// The actual value will vary, so we'll check in a separate test below
				},
			},
		},
		{
			name: "permissions check",
			schema: `definition user {}

definition document {
	relation reader: user
	relation writer: user
	permission edit = writer
	permission view = reader + edit
}`,
			relationships: []string{
				"document:firstdoc#reader@user:tom",
				"document:firstdoc#writer@user:jerry",
			},
			queries: []qar{
				{
					name:  "check permission - has permission",
					query: `SELECT resource_type, resource_id, permission, subject_type, subject_id FROM permissions WHERE resource_type = $1 AND resource_id = $2 AND permission = $3 AND subject_type = $4 AND subject_id = $5`,
					args:  []any{"document", "firstdoc", "view", "user", "tom"},
					expectedRows: [][]any{
						{"document", "firstdoc", "view", "user", "tom"},
					},
					expectedResponseTag: "SELECT 1",
				},
				{
					name:  "check permission - no permission",
					query: `SELECT resource_type, resource_id, permission, subject_type, subject_id, has_permission FROM permissions WHERE resource_type = $1 AND resource_id = $2 AND permission = $3 AND subject_type = $4 AND subject_id = $5`,
					args:  []any{"document", "firstdoc", "view", "user", "alice"},
					expectedRows: [][]any{
						{"document", "firstdoc", "view", "user", "alice", false},
					},
					expectedResponseTag: "SELECT 1",
				},
				{
					name:  "check permission - derived from writer",
					query: `SELECT resource_type, resource_id, permission, subject_type, subject_id FROM permissions WHERE resource_type = $1 AND resource_id = $2 AND permission = $3 AND subject_type = $4 AND subject_id = $5`,
					args:  []any{"document", "firstdoc", "view", "user", "jerry"},
					expectedRows: [][]any{
						{"document", "firstdoc", "view", "user", "jerry"},
					},
					expectedResponseTag: "SELECT 1",
				},
			},
		},
		{
			name: "permissions lookupresources",
			schema: `definition user {}

definition document {
	relation reader: user
	relation writer: user
	permission view = reader + writer
}`,
			relationships: []string{
				"document:firstdoc#reader@user:tom",
				"document:seconddoc#reader@user:tom",
				"document:thirddoc#reader@user:jerry",
			},
			queries: []qar{
				{
					name:  "lookup resources for tom",
					query: `SELECT resource_id FROM permissions WHERE resource_type = $1 AND permission = $2 AND subject_type = $3 AND subject_id = $4 ORDER BY resource_id`,
					args:  []any{"document", "view", "user", "tom"},
					expectedRows: [][]any{
						{"firstdoc"},
						{"seconddoc"},
					},
					expectedResponseTag: "SELECT 2",
				},
				{
					name:  "lookup resources for jerry",
					query: `SELECT resource_id FROM permissions WHERE resource_type = $1 AND permission = $2 AND subject_type = $3 AND subject_id = $4`,
					args:  []any{"document", "view", "user", "jerry"},
					expectedRows: [][]any{
						{"thirddoc"},
					},
					expectedResponseTag: "SELECT 1",
				},
				{
					name:                "lookup resources for user with no access",
					query:               `SELECT resource_id FROM permissions WHERE resource_type = $1 AND permission = $2 AND subject_type = $3 AND subject_id = $4`,
					args:                []any{"document", "view", "user", "alice"},
					expectedRows:        [][]any{},
					expectedResponseTag: "SELECT 0",
				},
			},
		},
		{
			name: "permissions lookupsubjects",
			schema: `definition user {}

definition document {
	relation reader: user
	permission view = reader
}`,
			relationships: []string{
				"document:firstdoc#reader@user:tom",
				"document:firstdoc#reader@user:jerry",
				"document:firstdoc#reader@user:sarah",
			},
			queries: []qar{
				{
					name:  "lookup subjects for resource",
					query: `SELECT subject_id FROM permissions WHERE resource_type = $1 AND resource_id = $2 AND permission = $3 AND subject_type = $4 ORDER BY subject_id`,
					args:  []any{"document", "firstdoc", "view", "user"},
					expectedRows: [][]any{
						{"jerry"},
						{"sarah"},
						{"tom"},
					},
					expectedResponseTag: "SELECT 3",
				},
				{
					name:                "lookup subjects for non-existent resource",
					query:               `SELECT subject_id FROM permissions WHERE resource_type = $1 AND resource_id = $2 AND permission = $3 AND subject_type = $4`,
					args:                []any{"document", "notfound", "view", "user"},
					expectedRows:        [][]any{},
					expectedResponseTag: "SELECT 0",
				},
			},
		},
		{
			name: "consistency fully_consistent",
			schema: `definition user {}

definition document {
	relation reader: user
	permission view = reader
}`,
			relationships: []string{
				"document:firstdoc#reader@user:tom",
			},
			queries: []qar{
				{
					name:  "check with fully_consistent",
					query: `SELECT resource_type, resource_id, permission, subject_type, subject_id FROM permissions WHERE resource_type = $1 AND resource_id = $2 AND permission = $3 AND subject_type = $4 AND subject_id = $5 AND consistency = $6`,
					args:  []any{"document", "firstdoc", "view", "user", "tom", "fully_consistent"},
					expectedRows: [][]any{
						{"document", "firstdoc", "view", "user", "tom"},
					},
					expectedResponseTag: "SELECT 1",
				},
			},
		},
		{
			name: "consistency at_least_as_fresh",
			schema: `definition user {}

definition document {
	relation reader: user
	permission view = reader
}`,
			queries: []qar{
				{
					name:                "insert relationship and get zedtoken",
					query:               `INSERT INTO relationships VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING consistency`,
					args:                []any{"document", "firstdoc", "reader", "user", "tom", "", "", ""},
					expectedResponseTag: "INSERT 0 1",
				},
				{
					name:  "verify at_least_as_fresh - will use zedtoken from previous query in test runner",
					query: `SELECT resource_type, resource_id, permission, subject_type, subject_id FROM permissions WHERE resource_type = $1 AND resource_id = $2 AND permission = $3 AND subject_type = $4 AND subject_id = $5`,
					args:  []any{"document", "firstdoc", "view", "user", "tom"},
					expectedRows: [][]any{
						{"document", "firstdoc", "view", "user", "tom"},
					},
					expectedResponseTag: "SELECT 1",
				},
			},
		},
		{
			name: "consistency at_exact_snapshot",
			schema: `definition user {}

definition document {
	relation reader: user
	permission view = reader
}`,
			queries: []qar{
				{
					name:                "insert relationship and get zedtoken",
					query:               `INSERT INTO relationships VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING consistency`,
					args:                []any{"document", "firstdoc", "reader", "user", "tom", "", "", ""},
					expectedResponseTag: "INSERT 0 1",
				},
				{
					name:  "verify at_exact_snapshot - will use @zedtoken from previous query in test runner",
					query: `SELECT resource_type, resource_id, permission, subject_type, subject_id FROM permissions WHERE resource_type = $1 AND resource_id = $2 AND permission = $3 AND subject_type = $4 AND subject_id = $5`,
					args:  []any{"document", "firstdoc", "view", "user", "tom"},
					expectedRows: [][]any{
						{"document", "firstdoc", "view", "user", "tom"},
					},
					expectedResponseTag: "SELECT 1",
				},
			},
		},
		{
			name: "join with local table",
			schema: `definition user {}

definition document {
	relation reader: user
	permission view = reader
}`,
			relationships: []string{
				"document:firstdoc#reader@user:jerry",
				"document:seconddoc#reader@user:jerry",
				"document:thirddoc#reader@user:tom",
			},
			queries: []qar{
				{
					name: "create local documents table",
					query: `CREATE TABLE document (
						id text PRIMARY KEY,
						title text NOT NULL,
						contents text NOT NULL
					)`,
					expectedRows:        [][]any{},
					expectedResponseTag: "CREATE TABLE",
				},
				{
					name:                "insert local documents",
					query:               `INSERT INTO document (id, title, contents) VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9)`,
					args:                []any{"firstdoc", "Document 1", "Contents of document 1", "seconddoc", "Document 2", "Contents of document 2", "thirddoc", "Document 3", "Contents of document 3"},
					expectedRows:        [][]any{},
					expectedResponseTag: "INSERT 0 3",
				},
				{
					name:  "join documents with permissions for jerry",
					query: `SELECT document.id, document.title FROM document JOIN permissions ON permissions.resource_id = document.id WHERE permissions.resource_type = $1 AND permissions.permission = $2 AND permissions.subject_type = $3 AND permissions.subject_id = $4 ORDER BY document.title DESC`,
					args:  []any{"document", "view", "user", "jerry"},
					expectedRows: [][]any{
						{"seconddoc", "Document 2"},
						{"firstdoc", "Document 1"},
					},
					expectedResponseTag: "SELECT 2",
				},
				{
					name:  "join documents with permissions for tom",
					query: `SELECT document.id, document.title FROM document JOIN permissions ON permissions.resource_id = document.id WHERE permissions.resource_type = $1 AND permissions.permission = $2 AND permissions.subject_type = $3 AND permissions.subject_id = $4`,
					args:  []any{"document", "view", "user", "tom"},
					expectedRows: [][]any{
						{"thirddoc", "Document 3"},
					},
					expectedResponseTag: "SELECT 1",
				},
			},
		},
		{
			name: "cursor support",
			schema: `definition user {}

definition document {
	relation reader: user
	permission view = reader
}`,
			relationships: []string{
				"document:doc1#reader@user:alice",
				"document:doc2#reader@user:alice",
				"document:doc3#reader@user:alice",
				"document:doc4#reader@user:alice",
				"document:doc5#reader@user:alice",
			},
			queries: []qar{
				{
					name:                "begin transaction",
					query:               `BEGIN`,
					expectedRows:        [][]any{},
					expectedResponseTag: "BEGIN",
				},
				{
					name:                "declare cursor",
					query:               `DECLARE my_cursor CURSOR FOR SELECT resource_id FROM permissions WHERE resource_type = $1 AND permission = $2 AND subject_type = $3 AND subject_id = $4`,
					args:                []any{"document", "view", "user", "alice"},
					expectedRows:        [][]any{},
					expectedResponseTag: "DECLARE CURSOR",
				},
				{
					name:                "fetch from cursor - first batch",
					query:               `FETCH 2 FROM my_cursor`,
					expectedResponseTag: "FETCH 2",
				},
				{
					name:                "fetch from cursor - second batch",
					query:               `FETCH 2 FROM my_cursor`,
					expectedResponseTag: "FETCH 2",
				},
				{
					name:                "close cursor",
					query:               `CLOSE my_cursor`,
					expectedRows:        [][]any{},
					expectedResponseTag: "CLOSE CURSOR",
				},
				{
					name:                "commit transaction",
					query:               `COMMIT`,
					expectedRows:        [][]any{},
					expectedResponseTag: "COMMIT",
				},
			},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			runEndToEndTest(t, tc)
		})
	}
}

func runEndToEndTest(t *testing.T, _ e2eTestCase) {
	require.Fail(t, "fail on purpose")
}

func runPGServer(t *testing.T, client *authzed.Client) int {
	pgserver := fdw.NewPgBackend(client, postgresTestUser, fdwPassword)

	port, err := GetFreePort()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() {
		cancel()
		require.NoError(t, pgserver.Close())
	})

	go func() {
		_ = pgserver.Run(ctx, fmt.Sprintf("localhost:%d", port))
	}()

	// Give PGServer time to start
	time.Sleep(50 * time.Millisecond)

	return port
}

// GetFreePort asks the kernel for a free open port that is ready to use.
// From: https://gist.github.com/sevkin/96bdae9274465b2d09191384f86ef39d
func GetFreePort() (port int, err error) {
	var a *net.TCPAddr
	if a, err = net.ResolveTCPAddr("tcp", "localhost:0"); err == nil {
		var l *net.TCPListener
		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer l.Close()
			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}
	return port, err
}

func runSpiceDB(t *testing.T) *authzed.Client {
	port, err := GetFreePort()
	require.NoError(t, err)
	address := fmt.Sprintf("localhost:%d", port)

	config := spicedbserver.Config{
		GRPCServer: util.GRPCServerConfig{
			Address: address,
			Network: "tcp",
			Enabled: true,
		},
		PresharedSecureKey: []string{"sometestkey"},
		DatastoreConfig: datastore.Config{
			Engine:               "memory",
			RevisionQuantization: 5 * time.Second,
			GCWindow:             5 * time.Minute,
			FilterMaximumIDCount: 100,
		},
	}

	ctx := t.Context()
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		cancel()
	})

	runnableServer, err := config.Complete(ctx)
	require.NoError(t, err)

	serverReady := make(chan bool)
	go (func() {
		serverReady <- true
		_ = runnableServer.Run(ctx)
	})()

	// Wait for server goroutine to start
	<-serverReady

	// Verify server is ready
	var client *authzed.Client
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var err error
		client, err = authzed.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpcutil.WithInsecureBearerToken("sometestkey"),
		)
		if err != nil {
			collect.Errorf("failed to create client: %v", err)
			return
		}

		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		_, schemaErr := client.ReadSchema(ctx, &v1.ReadSchemaRequest{})

		// Check for the NotFound, which is expected since no schema is set yet.
		require.Error(collect, schemaErr, "expected an error from ReadSchema")
		require.Contains(collect, schemaErr.Error(), "NotFound", "expected NotFound error")
	}, 5*time.Second, 50*time.Millisecond, "SpiceDB server did not become ready")

	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

func runPostgres(t *testing.T) (conn *pgx.Conn) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	postgresContainerHostname := fmt.Sprintf("postgres-%s", uuid.New().String())
	postgres, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       postgresContainerHostname,
		Repository: "postgres",
		Tag:        pgVersion,
		Env: []string{
			"POSTGRES_USER=" + postgresTestUser,
			"POSTGRES_PASSWORD=" + postgresTestPassword,
		},
		ExposedPorts: []string{postgresTestPort + "/tcp"},
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, pool.Purge(postgres))
	})

	hostname := "localhost"
	port := postgres.GetPort(postgresTestPort + "/tcp")

	creds := postgresTestUser + ":" + postgresTestPassword
	uri := fmt.Sprintf("postgresql://%s@%s:%s/?sslmode=disable", creds, hostname, port)
	err = pool.Retry(func() error {
		var err error
		ctx, cancelConnect := context.WithTimeout(t.Context(), dockerBootTimeout)
		defer cancelConnect()
		conn, err = pgx.Connect(ctx, uri)
		if err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)
	return conn
}
