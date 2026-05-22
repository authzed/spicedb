/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package x509util

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/prometheus/client_golang/prometheus"

	log "github.com/authzed/spicedb/internal/logging"
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

const defaultWatchInterval = 10 * time.Second

// CertWatcher watches certificate and key files for changes.
// It always returns the cached version,
// but periodically reads and parses certificate and key for changes
// and calls an optional callback with the new certificate.
type CertWatcher struct {
	mu                sync.RWMutex
	currentCert       *tls.Certificate // GUARDED_BY(mu)
	watcher           *fsnotify.Watcher
	interval          time.Duration
	certPath          string
	keyPath           string
	cachedKeyPEMBlock []byte

	// started is closed once Start() has registered fsnotify watches for
	// both files. Callers can block on Started() before mutating the watched
	// paths to avoid racing the Start goroutine.
	started chan error

	// callback is a function to be invoked when the certificate changes.
	callback func(tls.Certificate)

	// metrics
	ReadCertificateTotal  prometheus.Counter
	ReadCertificateErrors prometheus.Counter
}

// NewTLSCertWatcher returns a new CertWatcher watching the given certificate and key.
// It registers prometheus metrics for certificate read counts and errors.
// The metrics are unregistered when Start returns.
func NewTLSCertWatcher(certPath, keyPath string) (*CertWatcher, error) {
	var err error

	cw := &CertWatcher{
		certPath:              certPath,
		keyPath:               keyPath,
		interval:              defaultWatchInterval,
		started:               make(chan error),
		ReadCertificateTotal:  ReadTotal,
		ReadCertificateErrors: ReadErrors,
	}

	// ignore "duplicate metric registration" errors
	_ = prometheus.Register(cw.ReadCertificateTotal)
	_ = prometheus.Register(cw.ReadCertificateErrors)

	// Initial read of certificate and key.
	if err := cw.ReadCertificate(); err != nil {
		cw.unregisterMetrics()
		return nil, err
	}

	cw.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		cw.unregisterMetrics()
		return nil, err
	}

	return cw, nil
}

// unregisterMetrics removes the prometheus metrics registered by this CertWatcher.
func (cw *CertWatcher) unregisterMetrics() {
	prometheus.Unregister(cw.ReadCertificateTotal)
	prometheus.Unregister(cw.ReadCertificateErrors)
}

// WithWatchInterval sets the watch interval and returns the CertWatcher pointer
func (cw *CertWatcher) WithWatchInterval(interval time.Duration) *CertWatcher {
	cw.interval = interval
	return cw
}

// RegisterCallback registers a callback to be invoked when the certificate changes.
func (cw *CertWatcher) RegisterCallback(callback func(tls.Certificate)) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	// If the current certificate is not nil, invoke the callback immediately.
	if cw.currentCert != nil {
		callback(*cw.currentCert)
	}
	cw.callback = callback
}

// GetCertificate fetches the currently loaded certificate, which may be nil.
func (cw *CertWatcher) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	cw.mu.RLock()
	defer cw.mu.RUnlock()
	return cw.currentCert, nil
}

// Started returns a channel that is closed when Start() has finished
// registering the fsnotify watches for the certificate and key files.
// Callers that need to mutate the watched files should block on this
// channel after calling Start() in a goroutine to avoid races.
func (cw *CertWatcher) Started() <-chan error {
	return cw.started
}

// Start starts the watch on the certificate and key files.
// Any startup errors will be sent through the Started() channel.
// When Start returns, it unregisters the prometheus metrics that were registered
// in NewTLSCertWatcher.
func (cw *CertWatcher) Start(ctx context.Context) {
	defer cw.unregisterMetrics()

	for _, f := range []string{cw.certPath, cw.keyPath} {
		if err := cw.watcher.Add(f); err != nil {
			cw.started <- fmt.Errorf("failed to add watch for %s: %w", f, err)
			close(cw.started)
			return
		}
	}
	close(cw.started)

	go cw.Watch()

	ticker := time.NewTicker(cw.interval)
	defer ticker.Stop()

	log.Info().Dur("interval", cw.interval).Msg("Starting certificate poll+watcher")
	for {
		select {
		case <-ctx.Done():
			_ = cw.watcher.Close()
			return
		case <-ticker.C:
			if err := cw.ReadCertificate(); err != nil {
				log.Err(err).Msg("failed to read certificate")
			}
		}
	}
}

// Watch reads events from the watcher's channel and reacts to changes.
func (cw *CertWatcher) Watch() {
	for {
		select {
		case event, ok := <-cw.watcher.Events:
			// Channel is closed.
			if !ok {
				return
			}

			cw.handleEvent(event)
		case err, ok := <-cw.watcher.Errors:
			// Channel is closed.
			if !ok {
				return
			}

			log.Err(err).Msg("certificate watch error")
		}
	}
}

// updateCachedCertificate checks if the new certificate differs from the cache,
// updates it and returns the result if it was updated or not
func (cw *CertWatcher) updateCachedCertificate(cert *tls.Certificate, keyPEMBlock []byte) bool {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	if cw.currentCert != nil &&
		bytes.Equal(cw.currentCert.Certificate[0], cert.Certificate[0]) &&
		bytes.Equal(cw.cachedKeyPEMBlock, keyPEMBlock) {
		log.Debug().Msg("certificate already cached")
		return false
	}
	cw.currentCert = cert
	cw.cachedKeyPEMBlock = keyPEMBlock
	return true
}

// ReadCertificate reads the certificate and key files from disk, parses them,
// and updates the current certificate on the watcher if updated. If a callback is set, it
// is invoked with the new certificate.
func (cw *CertWatcher) ReadCertificate() error {
	cw.ReadCertificateTotal.Inc()
	certPEMBlock, err := os.ReadFile(cw.certPath)
	if err != nil {
		cw.ReadCertificateErrors.Inc()
		return err
	}
	keyPEMBlock, err := os.ReadFile(cw.keyPath)
	if err != nil {
		cw.ReadCertificateErrors.Inc()
		return err
	}

	cert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
	if err != nil {
		cw.ReadCertificateErrors.Inc()
		return err
	}

	if !cw.updateCachedCertificate(&cert, keyPEMBlock) {
		return nil
	}

	log.Debug().Msg("Updated current TLS certificate")

	// If a callback is registered, invoke it with the new certificate.
	cw.mu.RLock()
	defer cw.mu.RUnlock()
	if cw.callback != nil {
		go func() {
			cw.callback(cert)
		}()
	}
	return nil
}

func (cw *CertWatcher) handleEvent(event fsnotify.Event) {
	// Only care about events which may modify the contents of the file.
	switch {
	case event.Op.Has(fsnotify.Write):
	case event.Op.Has(fsnotify.Create):
	case event.Op.Has(fsnotify.Chmod), event.Op.Has(fsnotify.Remove):
		// If the file was removed or renamed, re-add the watch to the previous name
		if err := cw.watcher.Add(event.Name); err != nil {
			log.Err(err).Msg("error re-watching file")
		}
	default:
		return
	}

	log.Debug().Any("event", event).Msg("certificate event")
	if err := cw.ReadCertificate(); err != nil {
		log.Err(err).Msg("error re-reading certificate")
	}
}
