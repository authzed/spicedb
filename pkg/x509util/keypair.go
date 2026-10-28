package x509util

import (
	"crypto/tls"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	ReadTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "certwatcher_read_certificate_total",
		Help: "Total number of certificate reads",
	})

	ReadErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "certwatcher_read_certificate_errors_total",
		Help: "Total number of certificate read errors",
	})
)

// KeyPair holds a certificate and private key read from two files and re-reads
// them when their contents change. The files are re-read on demand, at most
// once per interval.
type KeyPair struct {
	cert *reloading[*tls.Certificate]
}

// NewKeyPair returns a new KeyPair for the PEM certificate at certPath and the
// PEM private key at keyPath. Both files are read eagerly, so a missing or
// mismatched pair is reported here rather than at the first handshake. It
// registers prometheus metrics for certificate read counts and errors.
func NewKeyPair(certPath, keyPath string) (*KeyPair, error) {
	// ignore "duplicate metric registration" errors
	_ = prometheus.Register(ReadTotal)
	_ = prometheus.Register(ReadErrors)

	cert, err := newReloading("key pair", certPath,
		func() ([][]byte, error) { return readKeyPairFiles(certPath, keyPath) },
		parseKeyPair)
	if err != nil {
		return nil, err
	}
	return &KeyPair{cert: cert}, nil
}

// WithReloadInterval sets the minimum time between reads of the files and
// returns the KeyPair pointer
func (k *KeyPair) WithReloadInterval(interval time.Duration) *KeyPair {
	k.cert.setInterval(interval)
	return k
}

// GetCertificate returns the current certificate, re-reading the files if the
// reload interval has elapsed. It satisfies tls.Config.GetCertificate, which is
// called on every handshake.
func (k *KeyPair) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	return k.cert.get(), nil
}

// readKeyPairFiles reads the PEM certificate and key, counting each read.
func readKeyPairFiles(certPath, keyPath string) ([][]byte, error) {
	ReadTotal.Inc()
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		ReadErrors.Inc()
		return nil, err
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		ReadErrors.Inc()
		return nil, err
	}
	return [][]byte{certPEM, keyPEM}, nil
}

// parseKeyPair parses the contents returned by readKeyPairFiles.
func parseKeyPair(contents [][]byte) (*tls.Certificate, error) {
	cert, err := tls.X509KeyPair(contents[0], contents[1])
	if err != nil {
		ReadErrors.Inc()
		return nil, err
	}
	return &cert, nil
}
