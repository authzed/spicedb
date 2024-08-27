package grpcutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"

	"github.com/authzed/spicedb/pkg/x509util"
)

// BufferedNetwork is a gRPC network that operates purely in memory.
const BufferedNetwork string = "buffnet"

type (
	DialFunc    func(context.Context, ...grpc.DialOption) (*grpc.ClientConn, error)
	NetDialFunc func(ctx context.Context, addr string) (net.Conn, error)
)

// NewBuffNet creates functions for binding a gRPC server to an
// in-memory buffer connection.
//
// This type of connection is useful for tests or in-process communication.
func NewBuffNet(bufferSize int) (net.Listener, DialFunc, NetDialFunc, error) {
	if bufferSize == 0 {
		bufferSize = 1024 * 1024
	}

	l := bufconn.Listen(bufferSize)
	return l, func(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
			opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
				return l.DialContext(ctx)
			}))

			return grpc.DialContext(ctx, BufferedNetwork, opts...)
		}, func(ctx context.Context, s string) (net.Conn, error) {
			return l.DialContext(ctx)
		}, nil
}

// ListenerDialers returns functions for binding a gRPC server to the network
// and creating clients to that server.
//
// This function includes support for buffer connections.
func ListenerDialers(bufferSize int, network, addr string) (net.Listener, DialFunc, NetDialFunc, error) {
	if network == BufferedNetwork {
		return NewBuffNet(bufferSize)
	}
	l, err := net.Listen(network, addr)
	if err != nil {
		return nil, nil, nil, err
	}
	return l, func(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
		return grpc.DialContext(ctx, addr, opts...)
	}, nil, nil
}

// TLSServerCreds constructs TransportCredentials for a gRPC server using the
// provided filepaths.
//
// Certificates are watched and reloaded upon change.
func TLSServerCreds(certPath, keyPath string) (credentials.TransportCredentials, *certwatcher.CertWatcher, error) {
	switch {
	case certPath != "" && keyPath != "":
		watcher, err := certwatcher.New(certPath, keyPath)
		if err != nil {
			return nil, nil, err
		}
		return credentials.NewTLS(&tls.Config{
			GetCertificate: watcher.GetCertificate,
			MinVersion:     tls.VersionTLS12,
		}), watcher, nil
	default:
		return nil, nil, nil
	}
}

// TLSClientCreds constructs TransportCredentials for a gRPC connection with
// the provided filepaths.
//
// If the caPath is not provided, the system certifcate pool will be used.
// If both certPath and keyPath are not provided, an insecure transport is
// returned.
func TLSClientCreds(caPath, certPath, keyPath string) (credentials.TransportCredentials, error) {
	switch {
	case certPath == "" && keyPath == "":
		return insecure.NewCredentials(), nil
	case certPath != "" && keyPath != "":
		var err error
		var pool *x509.CertPool
		if caPath != "" {
			pool, err = x509util.CustomCertPool(caPath)
		} else {
			pool, err = x509.SystemCertPool()
		}
		if err != nil {
			return nil, err
		}

		return credentials.NewTLS(&tls.Config{
			RootCAs:    pool,
			MinVersion: tls.VersionTLS12,
		}), nil
	default:
		return nil, nil
	}
}
