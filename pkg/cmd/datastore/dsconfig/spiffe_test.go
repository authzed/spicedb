package dsconfig

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/spiffetls"
)

func TestValidateSPIFFE(t *testing.T) {
	valid := spiffetls.Options{
		Enabled:  true,
		Socket:   "unix:///run/spire/agent.sock",
		ID:       "spiffe://client.test/spicedb",
		ServerID: "spiffe://db.test/mysql",
	}

	for _, engine := range []string{MemoryEngine, PostgresEngine, CockroachEngine, SpannerEngine, MySQLEngine} {
		require.NoError(t, ValidateSPIFFE(engine, spiffetls.Options{}), "an unset identity is valid for %s", engine)
	}

	require.NoError(t, ValidateSPIFFE(MySQLEngine, valid))

	for _, engine := range []string{MemoryEngine, PostgresEngine, CockroachEngine, SpannerEngine} {
		require.ErrorContains(t, ValidateSPIFFE(engine, valid), "not supported by the "+engine+" datastore engine")
	}

	t.Setenv("SPIFFE_ENDPOINT_SOCKET", valid.Socket)
	require.NoError(t, ValidateSPIFFE(MySQLEngine, spiffetls.Options{Enabled: true}), "enabling it is enough when the environment locates the Workload API")
	require.ErrorContains(t, ValidateSPIFFE(MySQLEngine, spiffetls.Options{Socket: valid.Socket}), "set --datastore-spiffe")
	require.ErrorContains(t, ValidateSPIFFE(PostgresEngine, spiffetls.Options{Socket: valid.Socket}), "not supported by the postgres datastore engine")
}

func TestSPIFFEOptions(t *testing.T) {
	cfg := DefaultDatastoreConfig()
	WithSPIFFEEnabled(true)(cfg)
	WithSPIFFESocket("unix:///run/spire/agent.sock")(cfg)
	WithSPIFFEID("spiffe://client.test/spicedb")(cfg)
	WithSPIFFEServerID("spiffe://db.test/mysql")(cfg)

	require.Equal(t, spiffetls.Options{
		Enabled:  true,
		Socket:   "unix:///run/spire/agent.sock",
		ID:       "spiffe://client.test/spicedb",
		ServerID: "spiffe://db.test/mysql",
	}, cfg.SPIFFEOptions())
}
