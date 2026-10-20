// Package spiffetls provides TLS configurations that authenticate with an
// X.509-SVID obtained from the SPIFFE Workload API, and authenticate the peer
// by its SPIFFE ID: for a datastore client verifying its server, and for a
// gRPC server verifying its clients.
//
// The SVID and the trust bundles are streamed from the Workload API and
// resolved on every handshake, so a rotated SVID or bundle is used by the next
// connection without a restart and without any file on disk.
package spiffetls

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"

	log "github.com/authzed/spicedb/internal/logging"
)

// startupTimeout bounds how long New waits for the Workload API to issue the
// SVID. It is a variable so that tests can shorten it.
var startupTimeout = 60 * time.Second

// Options configures the SPIFFE identity of a datastore client. Only Enabled
// is needed to use it; the other options override the defaults.
type Options struct {
	// Enabled turns the SPIFFE identity on. The other options are an error
	// without it, rather than being ignored or turning it on themselves.
	Enabled bool

	// Socket is the address of the SPIFFE Workload API, for example
	// unix:///spiffe-workload-api/spire-agent.sock. When unset, the
	// SPIFFE_ENDPOINT_SOCKET environment variable is used, as by other SPIFFE
	// clients. The environment variable only locates the Workload API: it
	// never enables the SPIFFE identity by itself.
	Socket string

	// ID is the SPIFFE ID of the X.509-SVID presented to the datastore. When
	// set and the workload is entitled to several SVIDs, this one is used, and
	// no other. When unset, the Workload API's default SVID is used, as the
	// SPIRE agent's Envoy SDS does for its "default" resource.
	ID string

	// ServerID is what the datastore server must present: a SPIFFE ID, for
	// that workload only, or a trust domain such as spiffe://example.org, for
	// any of its workloads. When unset, the server must belong to the trust
	// domain of the client's own SVID; a federated trust domain is never
	// accepted implicitly. The server's chain is verified against the bundle
	// of its trust domain from the Workload API, federated trust domains
	// included.
	ServerID string
}

// IsSet returns whether any option is set, so that callers validate stray
// options even when the SPIFFE identity is not enabled.
func (o Options) IsSet() bool {
	return o.Enabled || o.Socket != "" || o.ID != "" || o.ServerID != ""
}

// Validate checks that the options are unset, or enabled with a Workload API
// address and well-formed IDs.
func (o Options) Validate() error {
	if !o.IsSet() {
		return nil
	}
	if err := validateIdentity("datastore", o.Enabled, o.Socket, o.ID); err != nil {
		return err
	}
	if o.ServerID != "" {
		if _, err := spiffeid.FromString(o.ServerID); err != nil {
			return fmt.Errorf("invalid datastore server SPIFFE ID %q: %w", o.ServerID, err)
		}
	}
	return nil
}

// socketOrEnv returns the Workload API address to use, and whether it came
// from the environment.
func socketOrEnv(socket string) (string, bool) {
	if socket != "" {
		return socket, false
	}
	return os.Getenv(workloadapi.SocketEnv), true
}

// validateIdentity checks the options shared by every use of the package,
// naming the flags of the given prefix in its errors.
func validateIdentity(prefix string, enabled bool, socketFlag, id string) error {
	if !enabled {
		return fmt.Errorf("the %[1]s SPIFFE options require the SPIFFE identity to be enabled: set --%[1]s-spiffe", prefix)
	}

	socket, fromEnv := socketOrEnv(socketFlag)
	if socket == "" {
		return fmt.Errorf("no SPIFFE Workload API address for the %[1]s: set --%[1]s-spiffe-socket or %[2]s", prefix, workloadapi.SocketEnv)
	}
	if err := workloadapi.ValidateAddress(socket); err != nil {
		if fromEnv {
			return fmt.Errorf("invalid SPIFFE Workload API address %q in %s: %w", socket, workloadapi.SocketEnv, err)
		}
		return fmt.Errorf("invalid %s SPIFFE socket %q: %w", prefix, socket, err)
	}
	if id != "" {
		parsed, err := spiffeid.FromString(id)
		if err != nil {
			return fmt.Errorf("invalid %s SPIFFE ID %q: %w", prefix, id, err)
		}
		if parsed.Path() == "" {
			return fmt.Errorf("invalid %s SPIFFE ID %q: a trust domain is not the ID of an SVID", prefix, id)
		}
	}
	return nil
}

// Source holds a connection to the Workload API and the identities a datastore
// client authenticates with. It must be closed when the datastore is closed.
type Source struct {
	x509 *workloadapi.X509Source

	// ownID is the SPIFFE ID of the SVID the source started with.
	ownID spiffeid.ID

	// authorizer decides which peers are accepted.
	authorizer tlsconfig.Authorizer
}

// New connects to the Workload API and waits until it has issued an SVID, the
// one for opts.ID when set, so that a process started before its registration
// entry has propagated, such as a migration job, does not fail on its first
// connection.
func New(ctx context.Context, opts Options) (*Source, error) {
	if !opts.Enabled {
		return nil, errors.New("the datastore SPIFFE identity is not enabled")
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	x509Source, svid, logEvent, err := fetch(ctx, opts.Socket, opts.ID)
	if err != nil {
		return nil, err
	}

	authorizer, allowed := serverAuthorizer(opts.ServerID, svid.ID.TrustDomain())
	logEvent.Str("server_allowed", allowed).Msg("using X.509-SVID from the SPIFFE Workload API for the datastore")
	return &Source{x509: x509Source, ownID: svid.ID, authorizer: authorizer}, nil
}

// fetch connects to the Workload API, located by socketFlag or the
// environment, and waits, bounded, until it has issued an SVID: the one for id
// when set, or else the default one. The returned log event describes the SVID
// and is left for the caller to complete and send.
func fetch(ctx context.Context, socketFlag, id string) (*workloadapi.X509Source, *x509svid.SVID, *zerolog.Event, error) {
	socket, fromEnv := socketOrEnv(socketFlag)

	// The context bounds only the wait for the first update: the Workload API
	// stream itself runs until Close.
	startupCtx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()

	sourceOpts := []workloadapi.X509SourceOption{
		workloadapi.WithClientOptions(workloadapi.WithAddr(socket)),
	}
	wanted := "an X.509-SVID"
	if id != "" {
		parsed := spiffeid.RequireFromString(id)
		sourceOpts = append(sourceOpts, workloadapi.WithDefaultX509SVIDPicker(pickByID(parsed)))
		wanted = "an X.509-SVID for " + parsed.String()
	}

	x509Source, err := workloadapi.NewX509Source(startupCtx, sourceOpts...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to receive X.509-SVIDs from the SPIFFE Workload API at %s: %w", socket, err)
	}

	for {
		svid, err := x509Source.GetX509SVID()
		if err == nil {
			leaf := svid.Certificates[0]
			event := log.Ctx(ctx).Info().
				Str("workload_api", socket).
				Bool("workload_api_from_env", fromEnv).
				Str("spiffe_id", svid.ID.String()).
				Bool("default_svid", id == "").
				Str("subject", leaf.Subject.String()).
				Time("not_after", leaf.NotAfter)
			return x509Source, svid, event, nil
		}

		if err := x509Source.WaitUntilUpdated(startupCtx); err != nil {
			_ = x509Source.Close()
			return nil, nil, nil, fmt.Errorf("the SPIFFE Workload API at %s did not issue %s: %w", socket, wanted, err)
		}
	}
}

// serverAuthorizer returns what the server must present, and a description of
// it for the log: the given SPIFFE ID, any member of the given trust domain,
// or, when serverID is unset, any member of the client's own trust domain.
func serverAuthorizer(serverID string, own spiffeid.TrustDomain) (tlsconfig.Authorizer, string) {
	if serverID == "" {
		return tlsconfig.AuthorizeMemberOf(own), "any workload in " + own.IDString()
	}

	id := spiffeid.RequireFromString(serverID)
	if id.Path() == "" {
		return tlsconfig.AuthorizeMemberOf(id.TrustDomain()), "any workload in " + id.String()
	}
	return tlsconfig.AuthorizeID(id), id.String()
}

// pickByID selects the SVID for id. When there is none, the source holds no
// SVID and every handshake fails, rather than presenting another identity.
func pickByID(id spiffeid.ID) func([]*x509svid.SVID) *x509svid.SVID {
	return func(svids []*x509svid.SVID) *x509svid.SVID {
		for _, svid := range svids {
			if svid.ID == id {
				return svid
			}
		}
		return nil
	}
}

// ClientConfig returns a TLS configuration that presents the current SVID and
// accepts only a server allowed by the options. Both are resolved on every
// handshake, so the configuration can be shared by every connection of a
// datastore.
//
// The configuration has InsecureSkipVerify set because go-spiffe replaces the
// standard verification, which checks a host name, with its own: the server's
// chain is verified against the bundle of its trust domain, and its SPIFFE ID
// must be allowed. Verification is never skipped.
func (s *Source) ClientConfig() *tls.Config {
	cfg := tlsconfig.MTLSClientConfig(s.x509, s.x509, s.authorizer)
	cfg.MinVersion = tls.VersionTLS12
	return cfg
}

// Collectors returns the metrics describing the source, labelled with the
// datastore instance it serves. The caller registers and unregisters them.
func (s *Source) Collectors(instance string) []prometheus.Collector {
	return []prometheus.Collector{
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace:   "spicedb",
			Subsystem:   "datastore",
			Name:        "spiffe_svid_expiry_timestamp_seconds",
			Help:        "Expiry time of the X.509-SVID the datastore client authenticates with, in seconds since the Unix epoch, or 0 when it holds none.",
			ConstLabels: prometheus.Labels{"instance": instance},
		}, s.expiry),
	}
}

func (s *Source) expiry() float64 {
	svid, err := s.x509.GetX509SVID()
	if err != nil {
		return 0
	}
	return float64(svid.Certificates[0].NotAfter.Unix())
}

// Close ends the Workload API stream.
func (s *Source) Close() error {
	return s.x509.Close()
}
