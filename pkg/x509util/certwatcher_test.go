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
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestCertWatcherNew(t *testing.T) {
	t.Run("should error without cert/key", func(t *testing.T) {
		_, err := NewTLSCertWatcher("", "")
		require.Error(t, err)
	})
}

func TestCertWatcherSequentialMetricRegistration(t *testing.T) {
	dir := t.TempDir()
	certPath := dir + "/tls.crt"
	keyPath := dir + "/tls.key"

	require.NoError(t, writeCerts(certPath, keyPath, "127.0.0.1"))

	// First watcher: start and stop.
	ctx1, cancel1 := context.WithCancel(t.Context())
	watcher1, err := NewTLSCertWatcher(certPath, keyPath)
	require.NoError(t, err)

	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		_ = watcher1.Start(ctx1)
	}()
	cancel1()
	<-done1

	// Second watcher: should not fail due to duplicate metric registration.
	ctx2, cancel2 := context.WithCancel(t.Context())
	watcher2, err := NewTLSCertWatcher(certPath, keyPath)
	require.NoError(t, err)

	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		_ = watcher2.Start(ctx2)
	}()
	cancel2()
	<-done2
}

// setupWatcher creates a temporary certificate/key pair for the given IP address,
// constructs a CertWatcher, and returns a startWatcher function that launches the
// watcher in a background goroutine. The startWatcher function blocks until the
// first certificate is successfully read, and registers a t.Cleanup that cancels
// the watcher's context and waits for it to stop.
func setupWatcher(t *testing.T, ip string) (certPath, keyPath string, watcher *CertWatcher, startWatcher func(interval time.Duration) (done <-chan struct{})) {
	t.Helper()

	dir := t.TempDir()
	certPath = dir + "/tls.crt"
	keyPath = dir + "/tls.key"

	// write files
	err := writeCerts(certPath, keyPath, ip)
	require.NoError(t, err)

	// wait until they exist on disk
	require.Eventually(t, func() bool {
		for _, file := range []string{certPath, keyPath} {
			if _, err := os.ReadFile(file); err != nil {
				return false
			}
		}
		return true
	}, 10*time.Second, 100*time.Millisecond)

	watcher, err = NewTLSCertWatcher(certPath, keyPath)
	require.NoError(t, err)

	startWatcher = func(interval time.Duration) (done <-chan struct{}) {
		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			_ = watcher.WithWatchInterval(interval).Start(t.Context())
		}()
		// wait till we read first cert
		require.Eventually(t, func() bool {
			return watcher.ReadCertificate() == nil
		}, 10*time.Second, 100*time.Millisecond)
		// Register cleanup: wait for watcher to stop
		t.Cleanup(func() {
			require.Eventually(t, func() bool {
				select {
				case <-doneCh:
					return true
				default:
					return false
				}
			}, 4*time.Second, 100*time.Millisecond)
		})
		return doneCh
	}

	return certPath, keyPath, watcher, startWatcher
}

func TestCertWatcherStart(t *testing.T) {
	t.Run("should read the initial cert/key", func(t *testing.T) {
		_, _, _, startWatcher := setupWatcher(t, "127.0.0.1")
		startWatcher(10 * time.Second)
	})

	t.Run("should reload currentCert when changed", func(t *testing.T) {
		certPath, keyPath, watcher, startWatcher := setupWatcher(t, "127.0.0.1")
		startWatcher(10 * time.Second)
		called := atomic.Int64{}
		watcher.RegisterCallback(func(crt tls.Certificate) {
			called.Add(1)
			require.NotEmpty(t, crt.Certificate)
		})

		firstcert, _ := watcher.GetCertificate(nil)

		err := writeCerts(certPath, keyPath, "192.168.0.1")
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			secondcert, _ := watcher.GetCertificate(nil)
			first := firstcert.PrivateKey.(*rsa.PrivateKey)
			return !first.Equal(secondcert.PrivateKey) && firstcert.Leaf.SerialNumber.Cmp(secondcert.Leaf.SerialNumber) != 0
		}, 10*time.Second, 100*time.Millisecond)

		require.GreaterOrEqual(t, called.Load(), int64(1))
	})

	t.Run("should reload currentCert when changed with rename", func(t *testing.T) {
		certPath, keyPath, watcher, startWatcher := setupWatcher(t, "127.0.0.1")
		startWatcher(10 * time.Second)
		called := atomic.Int64{}
		watcher.RegisterCallback(func(crt tls.Certificate) {
			called.Add(1)
			require.NotEmpty(t, crt.Certificate)
		})

		firstcert, _ := watcher.GetCertificate(nil)

		err := writeCerts(certPath+".new", keyPath+".new", "192.168.0.2")
		require.NoError(t, err)

		require.NoError(t, os.Link(certPath, certPath+".old"))
		require.NoError(t, os.Rename(certPath+".new", certPath))

		require.NoError(t, os.Link(keyPath, keyPath+".old"))
		require.NoError(t, os.Rename(keyPath+".new", keyPath))

		require.Eventually(t, func() bool {
			secondcert, _ := watcher.GetCertificate(nil)
			first := firstcert.PrivateKey.(*rsa.PrivateKey)
			return !first.Equal(secondcert.PrivateKey) && firstcert.Leaf.SerialNumber.Cmp(secondcert.Leaf.SerialNumber) != 0
		}, 10*time.Second, 100*time.Millisecond)

		require.GreaterOrEqual(t, called.Load(), int64(1))
	})

	t.Run("should reload currentCert after move out", func(t *testing.T) {
		certPath, keyPath, watcher, startWatcher := setupWatcher(t, "127.0.0.1")
		startWatcher(100 * time.Millisecond)
		called := atomic.Int64{}
		watcher.RegisterCallback(func(crt tls.Certificate) {
			called.Add(1)
			require.NotEmpty(t, crt.Certificate)
		})

		firstcert, _ := watcher.GetCertificate(nil)

		require.NoError(t, os.Rename(certPath, certPath+".old"))
		require.NoError(t, os.Rename(keyPath, keyPath+".old"))

		err := writeCerts(certPath, keyPath, "192.168.0.3")
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			if err := watcher.ReadCertificate(); err != nil {
				return false
			}
			secondcert, _ := watcher.GetCertificate(nil)
			first := firstcert.PrivateKey.(*rsa.PrivateKey)
			return !first.Equal(secondcert.PrivateKey) && firstcert.Leaf.SerialNumber.Cmp(secondcert.Leaf.SerialNumber) != 0
		}, 10*time.Second, 100*time.Millisecond)

		require.GreaterOrEqual(t, called.Load(), int64(1))
	})

	t.Run("prometheus metric read_certificate_total on successful read", func(t *testing.T) {
		_, _, watcher, startWatcher := setupWatcher(t, "127.0.0.1")
		readCertificateTotalBefore := testutil.ToFloat64(watcher.ReadCertificateTotal)

		startWatcher(100 * time.Millisecond)

		require.Eventually(t, func() bool {
			readCertificateTotalAfter := testutil.ToFloat64(watcher.ReadCertificateTotal)
			return readCertificateTotalAfter >= readCertificateTotalBefore+1.0
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("prometheus metric read_certificate_total on read errors", func(t *testing.T) {
		_, keyPath, watcher, startWatcher := setupWatcher(t, "127.0.0.1")
		readCertificateTotalBefore := testutil.ToFloat64(watcher.ReadCertificateTotal)
		readCertificateErrorsBefore := testutil.ToFloat64(watcher.ReadCertificateErrors)

		startWatcher(100 * time.Millisecond)

		require.Eventually(t, func() bool {
			readCertificateTotalAfter := testutil.ToFloat64(watcher.ReadCertificateTotal)
			if readCertificateTotalAfter >= readCertificateTotalBefore+1.0 {
				readCertificateTotalBefore = readCertificateTotalAfter
				return true
			}
			return false
		}, 10*time.Second, 100*time.Millisecond)

		// remove the cert to generate an error
		require.NoError(t, os.Remove(keyPath))

		require.Error(t, watcher.ReadCertificate())

		readCertificateTotalAfter := testutil.ToFloat64(watcher.ReadCertificateTotal)
		require.GreaterOrEqual(t, readCertificateTotalAfter, readCertificateTotalBefore+1.0)

		readCertificateErrorsAfter := testutil.ToFloat64(watcher.ReadCertificateErrors)
		require.GreaterOrEqual(t, readCertificateErrorsAfter, readCertificateErrorsBefore+1.0)
	})
}

// writeCerts generates a self-signed certificate and private key for the given
// IP address and writes them in PEM format to certPath and keyPath.
func writeCerts(certPath, keyPath, ip string) error {
	var priv any
	var err error
	priv, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	keyUsage := x509.KeyUsageDigitalSignature
	if _, isRSA := priv.(*rsa.PrivateKey); isRSA {
		keyUsage |= x509.KeyUsageKeyEncipherment
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(1 * time.Hour)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Kubernetes"},
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:              keyUsage,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	template.IPAddresses = append(template.IPAddresses, net.ParseIP(ip))

	privkey := priv.(*rsa.PrivateKey)

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privkey.PublicKey, priv)
	if err != nil {
		return err
	}

	certOut, err := os.Create(certPath)
	if err != nil {
		return err
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		return err
	}
	if err := certOut.Close(); err != nil {
		return err
	}

	keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return err
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}); err != nil {
		return err
	}
	return keyOut.Close()
}
