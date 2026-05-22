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
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
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

	go func() {
		watcher1.Start(ctx1)
	}()
	cancel1()

	// Second watcher: should not fail due to duplicate metric registration.
	ctx2, cancel2 := context.WithCancel(t.Context())
	watcher2, err := NewTLSCertWatcher(certPath, keyPath)
	require.NoError(t, err)

	go func() {
		watcher2.Start(ctx2)
	}()
	cancel2()
}

// setupWatcher creates a temporary certificate/key pair for the given IP address,
// constructs a CertWatcher, and returns a startWatcher function that launches the
// watcher in a background goroutine.
func setupWatcher(t *testing.T, ip string) (certPath, keyPath string, watcher *CertWatcher, startWatcher func(interval time.Duration)) {
	t.Helper()

	dir := t.TempDir()
	certPath = filepath.Join(dir, "/tls.crt")
	keyPath = filepath.Join(dir, "/tls.key")

	// write files
	err := writeCerts(certPath, keyPath, ip)
	require.NoError(t, err)

	watcher, err = NewTLSCertWatcher(certPath, keyPath)
	require.NoError(t, err)

	startWatcher = func(interval time.Duration) {
		go func() {
			watcher.WithWatchInterval(interval).Start(t.Context())
		}()
		err := <-watcher.Started()
		assert.NoError(t, err)
		assert.NoError(t, watcher.ReadCertificate())
	}

	return certPath, keyPath, watcher, startWatcher
}

func TestCertWatcherStart(t *testing.T) {
	t.Run("should read the initial cert/key", func(t *testing.T) {
		_, _, _, startWatcher := setupWatcher(t, "127.0.0.1")
		// This invokes ReadCertificate indirectly
		startWatcher(5 * time.Second)
	})

	t.Run("should reload currentCert when changed", func(t *testing.T) {
		certPath, keyPath, watcher, startWatcher := setupWatcher(t, "127.0.0.1")
		startWatcher(10 * time.Second)
		called := atomic.Int64{}
		watcher.RegisterCallback(func(crt tls.Certificate) {
			called.Add(1)
			assert.NotEmpty(t, crt.Certificate)
		})

		firstcert, _ := watcher.GetCertificate(nil)

		err := writeCerts(certPath, keyPath, "192.168.0.1")
		require.NoError(t, err)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			secondcert, _ := watcher.GetCertificate(nil)
			first := firstcert.PrivateKey.(*rsa.PrivateKey)
			assert.False(collect, first.Equal(secondcert.PrivateKey), "first and second privatekeys should not be equal")
			assert.NotZero(collect, firstcert.Leaf.SerialNumber.Cmp(secondcert.Leaf.SerialNumber), "the serial numbers should compare as different")
		}, 10*time.Second, 1*time.Second)

		require.GreaterOrEqual(t, called.Load(), int64(1))
	})

	t.Run("should reload currentCert when changed with rename", func(t *testing.T) {
		certPath, keyPath, watcher, startWatcher := setupWatcher(t, "127.0.0.1")
		startWatcher(10 * time.Second)
		called := atomic.Int64{}
		watcher.RegisterCallback(func(crt tls.Certificate) {
			called.Add(1)
			assert.NotEmpty(t, crt.Certificate)
		})

		firstcert, _ := watcher.GetCertificate(nil)

		err := writeCerts(certPath+".new", keyPath+".new", "192.168.0.2")
		require.NoError(t, err)

		require.NoError(t, os.Link(certPath, certPath+".old"))
		require.NoError(t, os.Rename(certPath+".new", certPath))

		require.NoError(t, os.Link(keyPath, keyPath+".old"))
		require.NoError(t, os.Rename(keyPath+".new", keyPath))

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			secondcert, _ := watcher.GetCertificate(nil)
			first := firstcert.PrivateKey.(*rsa.PrivateKey)
			assert.False(collect, first.Equal(secondcert.PrivateKey), "first and second privatekeys should not be equal")
			assert.NotZero(collect, firstcert.Leaf.SerialNumber.Cmp(secondcert.Leaf.SerialNumber), "the serial numbers should compare as different")
		}, 10*time.Second, 1*time.Second)

		require.GreaterOrEqual(t, called.Load(), int64(1))
	})

	t.Run("should reload currentCert after move out", func(t *testing.T) {
		certPath, keyPath, watcher, startWatcher := setupWatcher(t, "127.0.0.1")
		startWatcher(1 * time.Second)
		called := atomic.Int64{}
		watcher.RegisterCallback(func(crt tls.Certificate) {
			called.Add(1)
			assert.NotEmpty(t, crt.Certificate)
		})

		firstcert, err := watcher.GetCertificate(nil)
		require.NoError(t, err)
		require.NotNil(t, firstcert)

		require.NoError(t, os.Rename(certPath, certPath+".old"))
		require.NoError(t, os.Rename(keyPath, keyPath+".old"))

		err = writeCerts(certPath, keyPath, "192.168.0.3")
		require.NoError(t, err)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			secondcert, err := watcher.GetCertificate(nil)
			if !assert.NoError(collect, err) {
				return
			}
			assert.NotNil(collect, secondcert)
			first := firstcert.PrivateKey.(*rsa.PrivateKey)
			assert.False(collect, first.Equal(secondcert.PrivateKey), "first and second privatekeys should not be equal")
			assert.NotZero(collect, firstcert.Leaf.SerialNumber.Cmp(secondcert.Leaf.SerialNumber), "the serial numbers should compare as different")
		}, 10*time.Second, 1*time.Second)

		require.GreaterOrEqual(t, called.Load(), int64(1))
	})

	t.Run("prometheus metric read_certificate_total on successful read", func(t *testing.T) {
		_, _, watcher, startWatcher := setupWatcher(t, "127.0.0.1")
		readCertificateTotalBefore := testutil.ToFloat64(watcher.ReadCertificateTotal)

		startWatcher(1 * time.Second)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			readCertificateTotalAfter := testutil.ToFloat64(watcher.ReadCertificateTotal)
			assert.GreaterOrEqual(collect, readCertificateTotalAfter, readCertificateTotalBefore+1.0)
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("prometheus metric read_certificate_total on read errors", func(t *testing.T) {
		_, keyPath, watcher, startWatcher := setupWatcher(t, "127.0.0.1")
		readCertificateTotalBefore := testutil.ToFloat64(watcher.ReadCertificateTotal)
		readCertificateErrorsBefore := testutil.ToFloat64(watcher.ReadCertificateErrors)

		startWatcher(1 * time.Second)

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

		// os.Remove may generate one or two fsnotify events depending on the platform
		// (e.g. Chmod + Remove on Linux, just Remove on macOS).
		require.Eventually(t, func() bool {
			readCertificateTotalAfter := testutil.ToFloat64(watcher.ReadCertificateTotal)
			return readCertificateTotalAfter >= readCertificateTotalBefore+1.0
		}, 10*time.Second, 100*time.Millisecond)

		require.Eventually(t, func() bool {
			readCertificateErrorsAfter := testutil.ToFloat64(watcher.ReadCertificateErrors)
			return readCertificateErrorsAfter >= readCertificateErrorsBefore+1.0
		}, 10*time.Second, 100*time.Millisecond)
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
