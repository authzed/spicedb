package spiffetls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"slices"
	"strings"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
)

// ServerOptions configures the SPIFFE identity of a server that requires its
// clients to authenticate with an X.509-SVID. Only Enabled is needed to use
// it; the other options override the defaults.
//
// It authenticates clients and restricts which ones may connect at all. It
// says nothing about what a client may do once connected.
type ServerOptions struct {
	// FlagPrefix is the prefix of the flags that set these options, such as
	// "grpc", and names them in errors.
	FlagPrefix string

	// Enabled turns the SPIFFE identity on. The other options are an error
	// without it, rather than being ignored or turning it on themselves.
	Enabled bool

	// Socket is the address of the SPIFFE Workload API. When unset, the
	// SPIFFE_ENDPOINT_SOCKET environment variable is used. The environment
	// variable only locates the Workload API: it never enables the SPIFFE
	// identity by itself.
	Socket string

	// ID is the SPIFFE ID of the X.509-SVID the server presents. When unset,
	// the Workload API's default SVID is used.
	ID string

	// AllowedIDs are the clients that may connect: each is a SPIFFE ID, for
	// that workload only, or a trust domain such as spiffe://example.org, for
	// any of its workloads. When empty, any workload in the trust domain of the
	// server's own SVID may connect; a federated trust domain is never accepted
	// implicitly. The server's own SPIFFE ID is always accepted, so that a
	// process can reach its own listener.
	AllowedIDs []string
}

func (o ServerOptions) prefix() string {
	if o.FlagPrefix == "" {
		return "grpc"
	}
	return o.FlagPrefix
}

// IsSet returns whether any option is set, so that callers validate stray
// options even when the SPIFFE identity is not enabled.
func (o ServerOptions) IsSet() bool {
	return o.Enabled || o.Socket != "" || o.ID != "" || len(o.AllowedIDs) > 0
}

// Validate checks that the options are unset, or enabled with a Workload API
// address and well-formed IDs.
func (o ServerOptions) Validate() error {
	if !o.IsSet() {
		return nil
	}
	if err := validateIdentity(o.prefix(), o.Enabled, o.Socket, o.ID); err != nil {
		return err
	}
	for _, allowed := range o.AllowedIDs {
		if _, err := spiffeid.FromString(allowed); err != nil {
			return fmt.Errorf("invalid %s allowed SPIFFE ID %q: %w", o.prefix(), allowed, err)
		}
	}
	return nil
}

// NewServer connects to the Workload API and waits until it has issued the
// server's SVID.
func NewServer(ctx context.Context, opts ServerOptions) (*Source, error) {
	if !opts.Enabled {
		return nil, fmt.Errorf("the %s SPIFFE identity is not enabled", opts.prefix())
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	x509Source, svid, logEvent, err := fetch(ctx, opts.Socket, opts.ID)
	if err != nil {
		return nil, err
	}

	authorizer, allowed := clientAuthorizer(opts.AllowedIDs, svid.ID)
	logEvent.Str("service", opts.prefix()).
		Strs("clients_allowed", allowed).
		Msg("serving with X.509-SVID from the SPIFFE Workload API, requiring client X.509-SVIDs")
	return &Source{x509: x509Source, ownID: svid.ID, authorizer: authorizer}, nil
}

// clientAuthorizer returns which clients may connect, and a description of
// them for the log: the server's own ID, plus the allowed IDs and trust
// domains or, when none are given, the server's own trust domain.
func clientAuthorizer(allowedIDs []string, own spiffeid.ID) (tlsconfig.Authorizer, []string) {
	ids := map[spiffeid.ID]struct{}{own: {}}
	var domains []spiffeid.TrustDomain
	if len(allowedIDs) == 0 {
		domains = append(domains, own.TrustDomain())
	}
	for _, allowed := range allowedIDs {
		id := spiffeid.RequireFromString(allowed)
		if id.Path() == "" {
			domains = append(domains, id.TrustDomain())
			continue
		}
		ids[id] = struct{}{}
	}

	described := make([]string, 0, len(ids)+len(domains))
	for _, td := range domains {
		described = append(described, "any workload in "+td.IDString())
	}
	idStrings := make([]string, 0, len(ids))
	for id := range ids {
		idStrings = append(idStrings, id.String())
	}
	slices.Sort(idStrings)
	described = append(described, idStrings...)

	return func(id spiffeid.ID, _ [][]*x509.Certificate) error {
		if _, ok := ids[id]; ok {
			return nil
		}
		for _, td := range domains {
			if id.MemberOf(td) {
				return nil
			}
		}
		return fmt.Errorf("client %s is not allowed to connect; allowed: %s", id, strings.Join(described, ", "))
	}, described
}

// ServerConfig returns a TLS configuration that presents the current SVID and
// requires each client to present an X.509-SVID allowed by the options,
// verified against the bundle of its trust domain. Both are resolved on every
// handshake.
//
// It is meant for a source made by NewServer; for one made by New, it would
// accept the clients that source accepts as servers.
func (s *Source) ServerConfig() *tls.Config {
	cfg := tlsconfig.MTLSServerConfig(s.x509, s.x509, s.authorizer)
	cfg.MinVersion = tls.VersionTLS12
	return cfg
}

// SelfClientConfig returns a TLS configuration for connecting to the server
// this source serves: it presents the same SVID, and accepts only a server
// presenting it too. A process uses it to reach its own listener.
func (s *Source) SelfClientConfig() *tls.Config {
	cfg := tlsconfig.MTLSClientConfig(s.x509, s.x509, tlsconfig.AuthorizeID(s.ownID))
	cfg.MinVersion = tls.VersionTLS12
	return cfg
}
