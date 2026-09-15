package x509util

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc/credentials"

	log "github.com/authzed/spicedb/internal/logging"
)

const defaultCAReloadInterval = 10 * time.Second

// CAPool holds the certificate authorities read from a path and re-reads them
// when the contents of that path change. Unlike CertWatcher it runs no
// background goroutine: the path is re-read on demand, at most once per
// interval.
type CAPool struct {
	path     string
	interval time.Duration

	mu       sync.Mutex
	pool     *x509.CertPool // GUARDED_BY(mu)
	contents [][]byte       // GUARDED_BY(mu)
	lastRead time.Time      // GUARDED_BY(mu)
}

// NewCAPool returns a new CAPool for the certificate authorities at caPath,
// which may be a single PEM file or a directory of them. The path is read
// eagerly, so a missing or malformed bundle is reported here rather than at the
// first handshake.
func NewCAPool(caPath string) (*CAPool, error) {
	contents, err := readCertFiles(caPath)
	if err != nil {
		return nil, err
	}

	pool, err := certPoolFromPEM(contents)
	if err != nil {
		return nil, err
	}

	return &CAPool{
		path:     caPath,
		interval: defaultCAReloadInterval,
		pool:     pool,
		contents: contents,
		lastRead: time.Now(),
	}, nil
}

// WithReloadInterval sets the minimum time between reads of the path and
// returns the CAPool pointer
func (p *CAPool) WithReloadInterval(interval time.Duration) *CAPool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.interval = interval
	return p
}

// CertPool returns the current authorities, re-reading the path if the reload
// interval has elapsed. A failed re-read keeps the previously loaded
// authorities, so a bundle that is briefly absent or truncated partway through
// a rotation does not empty the trust store.
func (p *CAPool) CertPool() *x509.CertPool {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	if now.Sub(p.lastRead) < p.interval {
		return p.pool
	}
	p.lastRead = now

	contents, err := readCertFiles(p.path)
	if err != nil {
		log.Warn().Err(err).Str("path", p.path).Msg("failed to re-read certificate authorities")
		return p.pool
	}

	if slices.EqualFunc(p.contents, contents, bytes.Equal) {
		return p.pool
	}

	pool, err := certPoolFromPEM(contents)
	if err != nil {
		log.Warn().Err(err).Str("path", p.path).Msg("failed to parse re-read certificate authorities")
		return p.pool
	}

	log.Info().Str("path", p.path).Msg("reloaded certificate authorities")
	p.pool = pool
	p.contents = contents
	return p.pool
}

// DialTLSContext returns a dial function that verifies the server against the
// current authorities. It satisfies http.Transport.DialTLSContext, which is
// called per connection, unlike TLSClientConfig.
func (p *CAPool) DialTLSContext(base *tls.Config) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		cfg := base.Clone()
		cfg.RootCAs = p.CertPool()
		return (&tls.Dialer{Config: cfg}).DialContext(ctx, network, addr)
	}
}

// reloadingCreds resolves its trust store from a CAPool on every handshake and
// delegates the handshake itself to the standard TLS credentials.
type reloadingCreds struct {
	ca   *CAPool
	base *tls.Config
}

var _ credentials.TransportCredentials = (*reloadingCreds)(nil)

// NewReloadingTLSCreds returns credentials that verify peers against the
// authorities in ca, picking up a rotation of those authorities without a
// restart. base supplies the rest of the TLS configuration; its RootCAs and
// ClientCAs are ignored.
func NewReloadingTLSCreds(ca *CAPool, base *tls.Config) credentials.TransportCredentials {
	return &reloadingCreds{ca: ca, base: base.Clone()}
}

func (c *reloadingCreds) clientConfig() *tls.Config {
	cfg := c.base.Clone()
	cfg.RootCAs = c.ca.CertPool()
	return cfg
}

func (c *reloadingCreds) serverConfig() *tls.Config {
	cfg := c.base.Clone()
	cfg.ClientCAs = c.ca.CertPool()
	return cfg
}

func (c *reloadingCreds) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return credentials.NewTLS(c.clientConfig()).ClientHandshake(ctx, authority, rawConn)
}

func (c *reloadingCreds) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return credentials.NewTLS(c.serverConfig()).ServerHandshake(rawConn)
}

func (c *reloadingCreds) Info() credentials.ProtocolInfo {
	return credentials.NewTLS(c.base).Info()
}

// Clone returns credentials backed by the same CAPool, so that both share one
// view of the trust store.
func (c *reloadingCreds) Clone() credentials.TransportCredentials {
	return &reloadingCreds{ca: c.ca, base: c.base.Clone()}
}

func (c *reloadingCreds) OverrideServerName(name string) error {
	c.base.ServerName = name
	return nil
}
