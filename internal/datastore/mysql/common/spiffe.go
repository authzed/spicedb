package common

import (
	"context"
	"fmt"

	"github.com/go-sql-driver/mysql"

	"github.com/authzed/spicedb/internal/spiffetls"
)

// MaybeConfigureSPIFFE configures dbConfig to connect over mutual TLS with the
// X.509-SVID and trust bundles of the SPIFFE Workload API described by opts,
// and returns the source to close along with the connections. It is a no-op
// returning nil when no option is set.
//
// The source resolves the SVID and bundles on every handshake, so the
// configuration is set once and every new connection uses the current ones.
// It must run before MaybeAddCredentialsProviderHook, which checks whether
// TLS is configured.
func MaybeConfigureSPIFFE(ctx context.Context, dbConfig *mysql.Config, opts spiffetls.Options) (*spiffetls.Source, error) {
	if !opts.IsSet() {
		return nil, nil
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	// tls=true is accepted since it asks for no more than this does. Any other
	// value would be silently replaced, which is never what was meant.
	switch dbConfig.TLSConfig {
	case "", "true":
	default:
		return nil, fmt.Errorf("the connection URI sets tls=%s, which conflicts with the datastore SPIFFE options; remove the tls parameter from the URI", dbConfig.TLSConfig)
	}

	source, err := spiffetls.New(ctx, opts)
	if err != nil {
		return nil, err
	}

	dbConfig.TLSConfig = ""
	dbConfig.TLS = source.ClientConfig()
	dbConfig.AllowFallbackToPlaintext = false
	return source, nil
}
