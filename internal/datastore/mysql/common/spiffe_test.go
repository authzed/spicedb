package common

import (
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/spiffetls"
	"github.com/authzed/spicedb/internal/testutil/spiffetest"
)

func spiffeOptions(t *testing.T) spiffetls.Options {
	t.Helper()

	api := spiffetest.NewWorkloadAPI(t)
	clientCA := spiffetest.NewCA(t, "client.test")
	serverCA := spiffetest.NewCA(t, "db.test")
	api.SetX509SVIDs(t, []*x509svid.SVID{clientCA.MintX509SVID(t, "spiffe://client.test/spicedb")}, clientCA, serverCA)

	return spiffetls.Options{Enabled: true, Socket: api.Addr, ID: "spiffe://client.test/spicedb", ServerID: "spiffe://db.test/mysql"}
}

func parseDSN(t *testing.T, dsn string) *mysql.Config {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	return cfg
}

func TestMaybeConfigureSPIFFEUnset(t *testing.T) {
	cfg := parseDSN(t, "user@tcp(db:3306)/spicedb?parseTime=true")

	source, err := MaybeConfigureSPIFFE(t.Context(), cfg, spiffetls.Options{})
	require.NoError(t, err)
	require.Nil(t, source)
	require.Nil(t, cfg.TLS, "an unset identity must leave TLS to the connection URI")
}

func TestMaybeConfigureSPIFFE(t *testing.T) {
	for _, dsn := range []string{
		"user@tcp(db:3306)/spicedb?parseTime=true",
		"user@tcp(db:3306)/spicedb?parseTime=true&tls=true",
	} {
		t.Run(dsn, func(t *testing.T) {
			cfg := parseDSN(t, dsn)

			source, err := MaybeConfigureSPIFFE(t.Context(), cfg, spiffeOptions(t))
			require.NoError(t, err)
			t.Cleanup(func() { _ = source.Close() })

			require.Empty(t, cfg.TLSConfig)
			require.NotNil(t, cfg.TLS)
			require.NotNil(t, cfg.TLS.GetClientCertificate, "the SVID must be resolved per handshake")
			require.NotNil(t, cfg.TLS.VerifyPeerCertificate, "the server must be verified by its SPIFFE ID")
			require.False(t, cfg.AllowFallbackToPlaintext)

			// The driver clones the configuration for each connection; the
			// clone has to keep resolving through the source.
			require.NotNil(t, cfg.Clone().TLS.GetClientCertificate)
		})
	}
}

func TestMaybeConfigureSPIFFEConflicts(t *testing.T) {
	for _, tls := range []string{"false", "skip-verify", "preferred"} {
		t.Run(tls, func(t *testing.T) {
			cfg := parseDSN(t, "user@tcp(db:3306)/spicedb?parseTime=true&tls="+tls)

			// No Workload API is needed: the conflict is reported first.
			_, err := MaybeConfigureSPIFFE(t.Context(), cfg, spiffetls.Options{
				Enabled:  true,
				Socket:   "unix:///nonexistent.sock",
				ID:       "spiffe://client.test/spicedb",
				ServerID: "spiffe://db.test/mysql",
			})
			require.ErrorContains(t, err, "the connection URI sets tls="+tls)
		})
	}
}

func TestMaybeConfigureSPIFFEInvalidOptions(t *testing.T) {
	cfg := parseDSN(t, "user@tcp(db:3306)/spicedb?parseTime=true")

	_, err := MaybeConfigureSPIFFE(t.Context(), cfg, spiffetls.Options{Socket: "unix:///run/spire/agent.sock"})
	require.ErrorContains(t, err, "set --datastore-spiffe")
	require.Nil(t, cfg.TLS)
}
