//go:build datastore

package mysql

import (
	"bytes"
	"context"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"fmt"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/internal/datastore/mysql/version"
	"github.com/authzed/spicedb/internal/spiffetls"
	"github.com/authzed/spicedb/internal/testutil/spiffetest"
	"github.com/authzed/spicedb/pkg/migrate"
)

const (
	spiffeClientID = "spiffe://client.test/spicedb"
	spiffeServerID = "spiffe://db.test/mysql"

	// The subject SPIRE gives an SVID whose registration entry has the DNS
	// name "spicedb" first, and the grant that authenticates it.
	spiffeClientSubject = "/C=US/O=SPIRE/CN=spicedb"

	spiffeServerConfig = `[mysqld]
ssl_ca=/etc/mysql/certs/ca.pem
ssl_cert=/etc/mysql/certs/server-cert.pem
ssl_key=/etc/mysql/certs/server-key.pem
require_secure_transport=ON
`
)

// TestMySQLSPIFFEIdentity runs the datastore and its migrations against a
// MySQL server that accepts only TLS and authenticates the SpiceDB user by the
// subject of its client certificate, with the certificate obtained from the
// SPIFFE Workload API. It then rotates the SVID and checks that new
// connections use it without rebuilding anything.
func TestMySQLSPIFFEIdentity(t *testing.T) {
	ctx := t.Context()

	clientRoot := spiffetest.NewCA(t, "client.test")
	clientCA := clientRoot.Intermediate(t)
	serverCA := spiffetest.NewCA(t, "db.test")

	api := spiffetest.NewWorkloadAPI(t)
	api.SetX509SVIDs(t, []*x509svid.SVID{clientCA.MintX509SVID(t, spiffeClientID, "spicedb")}, clientCA, serverCA)
	opts := spiffetls.Options{Enabled: true, Socket: api.Addr, ID: spiffeClientID, ServerID: spiffeServerID}

	endpoint := runSPIFFEMySQL(t, clientRoot, serverCA.MintX509SVID(t, spiffeServerID))
	dsn := fmt.Sprintf("spicedb@tcp(%s)/spicedb?parseTime=true", endpoint)

	// Migrations authenticate the same way, as `spicedb migrate` would.
	driver, err := migrations.NewMySQLDriverFromDSN(ctx, dsn, "", nil, opts)
	require.NoError(t, err)
	require.NoError(t, migrations.Manager.Run(ctx, driver, migrate.Head, migrate.LiveRun))
	require.NoError(t, driver.Close(ctx))

	ds, err := newMySQLDatastore(ctx, dsn, primaryInstanceID, WithSPIFFE(opts), GCEnabled(false))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	// Every query below opens a new connection, so each one is a handshake.
	ds.db.SetMaxIdleConns(0)

	user, err := currentUser(ctx, ds.db)
	require.NoError(t, err)
	require.Equal(t, "spicedb@%", user)

	// An SVID from an authority the server does not trust: if new connections
	// keep working, they are not presenting the SVID the Workload API serves.
	untrusted := spiffetest.NewCA(t, "client.test")
	api.SetX509SVIDs(t, []*x509svid.SVID{untrusted.MintX509SVID(t, spiffeClientID, "spicedb")}, untrusted, serverCA)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := currentUser(ctx, ds.db)
		assert.Error(c, err)
	}, 30*time.Second, 100*time.Millisecond, "new connections should present the SVID most recently served")

	rotated := clientCA.MintX509SVID(t, spiffeClientID, "spicedb")
	api.SetX509SVIDs(t, []*x509svid.SVID{rotated}, clientCA, serverCA)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		user, err := currentUser(ctx, ds.db)
		assert.NoError(c, err)
		assert.Equal(c, "spicedb@%", user)
	}, 30*time.Second, 100*time.Millisecond, "new connections should recover once a trusted SVID is served")
}

// TestMySQLSPIFFEDefaults only enables the SPIFFE identity, as
// --datastore-spiffe alone does: the Workload API is located through
// SPIFFE_ENDPOINT_SOCKET, the default SVID is presented, and a server in the
// client's own trust domain is accepted.
func TestMySQLSPIFFEDefaults(t *testing.T) {
	ctx := t.Context()
	ca := spiffetest.NewCA(t, "client.test")

	api := spiffetest.NewWorkloadAPI(t)
	api.SetX509SVIDs(t, []*x509svid.SVID{ca.MintX509SVID(t, spiffeClientID, "spicedb")}, ca)

	endpoint := runSPIFFEMySQL(t, ca, ca.MintX509SVID(t, "spiffe://client.test/mysql"))
	dsn := fmt.Sprintf("spicedb@tcp(%s)/spicedb?parseTime=true", endpoint)
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", api.Addr)
	opts := spiffetls.Options{Enabled: true}

	driver, err := migrations.NewMySQLDriverFromDSN(ctx, dsn, "", nil, opts)
	require.NoError(t, err)
	require.NoError(t, migrations.Manager.Run(ctx, driver, migrate.Head, migrate.LiveRun))
	require.NoError(t, driver.Close(ctx))

	ds, err := newMySQLDatastore(ctx, dsn, primaryInstanceID, WithSPIFFE(opts), GCEnabled(false))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	user, err := currentUser(ctx, ds.db)
	require.NoError(t, err)
	require.Equal(t, "spicedb@%", user)
}

func TestMySQLSPIFFEServerIDMismatch(t *testing.T) {
	clientCA := spiffetest.NewCA(t, "client.test")
	serverCA := spiffetest.NewCA(t, "db.test")

	api := spiffetest.NewWorkloadAPI(t)
	api.SetX509SVIDs(t, []*x509svid.SVID{clientCA.MintX509SVID(t, spiffeClientID, "spicedb")}, clientCA, serverCA)

	endpoint := runSPIFFEMySQL(t, clientCA, serverCA.MintX509SVID(t, "spiffe://db.test/impostor"))
	dsn := fmt.Sprintf("spicedb@tcp(%s)/spicedb?parseTime=true", endpoint)

	_, err := NewMySQLDatastore(t.Context(), dsn,
		WithSPIFFE(spiffetls.Options{Enabled: true, Socket: api.Addr, ID: spiffeClientID, ServerID: spiffeServerID}))
	require.ErrorContains(t, err, "unexpected ID")
}

// runSPIFFEMySQL starts a MySQL server presenting serverSVID, trusting client
// certificates issued under clientRoot and requiring TLS on every connection,
// with a passwordless user spicedb authenticated by spiffeClientSubject. It
// returns the server's host:port.
func runSPIFFEMySQL(t *testing.T, clientRoot *spiffetest.CA, serverSVID *x509svid.SVID) string {
	t.Helper()
	ctx := t.Context()

	serverKey, err := x509.MarshalPKCS8PrivateKey(serverSVID.PrivateKey)
	require.NoError(t, err)

	file := func(path string, contents []byte) testcontainers.ContainerFile {
		return testcontainers.ContainerFile{ContainerFilePath: path, Reader: bytes.NewReader(contents), FileMode: 0o644}
	}
	container, err := tcmysql.Run(ctx, "mirror.gcr.io/library/mysql:"+version.MinimumSupportedMySQLVersion,
		tcmysql.WithPassword("secret"),
		testcontainers.WithFiles(
			file("/etc/mysql/conf.d/spiffe.cnf", []byte(spiffeServerConfig)),
			file("/etc/mysql/certs/ca.pem", encodeCertificates(clientRoot.Bundle())),
			file("/etc/mysql/certs/server-cert.pem", encodeCertificates(serverSVID.Certificates)),
			file("/etc/mysql/certs/server-key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: serverKey})),
		),
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)

	endpoint, err := container.PortEndpoint(ctx, "3306/tcp", "")
	require.NoError(t, err)

	// The administrator authenticates with a password; TLS is required of it
	// too, but it does not verify the server, whose identity is a SPIFFE ID.
	admin, err := sql.Open("mysql", fmt.Sprintf("root:secret@tcp(%s)/?tls=skip-verify", endpoint))
	require.NoError(t, err)
	defer admin.Close()

	for _, stmt := range []string{
		"CREATE DATABASE spicedb",
		"CREATE USER 'spicedb'@'%' REQUIRE SUBJECT '" + spiffeClientSubject + "'",
		"GRANT ALL PRIVILEGES ON spicedb.* TO 'spicedb'@'%'",
	} {
		_, err := admin.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	return endpoint
}

func currentUser(ctx context.Context, db *sql.DB) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var user string
	err := db.QueryRowContext(ctx, "SELECT CURRENT_USER()").Scan(&user)
	return user, err
}

func encodeCertificates(certs []*x509.Certificate) []byte {
	var buf bytes.Buffer
	for _, cert := range certs {
		_ = pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	}
	return buf.Bytes()
}
