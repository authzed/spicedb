// Package grpcutil implements various utilities to simplify common gRPC APIs.
package grpcutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"

	"github.com/certifi/gocertifi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	// SkipVerifyCA is a constant that imrpoves the readability of `WithSystemCerts`
	SkipVerifyCA = true

	// VerifyCA is a constant that improves the readability of `WithSystemCerts`.
	VerifyCA = false
)

// WithSystemCerts is a dial option for requiring TLS with the system
// certificate pool.
//
// This function panics if the system pool cannot be loaded.
func WithSystemCerts(insecureSkipVerify bool) grpc.DialOption {
	certPool, err := x509.SystemCertPool()
	if err != nil {
		// Fall back to Mozilla collection of root CAs.
		certPool, err = gocertifi.CACerts()
		if err != nil {
			// This library promises that this should never occur.
			panic("gocertifi returned an error: " + err.Error())
		}
	}

	return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: insecureSkipVerify,
	}))
}

// WithCustomCerts is a dial option for requiring TLS from a specified path.
// If the path is a directory, all certs are loaded. If it is an individual
// file only the directly specified cert is loaded.
//
// This function panics if custom certificate pool cannot be instantiated.
func WithCustomCerts(certPath string, insecureSkipVerify bool) grpc.DialOption {
	certPool, err := customCertPool(certPath)
	if err != nil {
		panic(err)
	}

	return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: insecureSkipVerify,
	}))
}

type secureMetadataCreds map[string]string

func (c secureMetadataCreds) RequireTransportSecurity() bool { return true }
func (c secureMetadataCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return c, nil
}

// WithBearerToken is a dial option to add a standard HTTP Bearer token to all
// requests sent from a client.
func WithBearerToken(token string) grpc.DialOption {
	return grpc.WithPerRPCCredentials(secureMetadataCreds{"authorization": "Bearer " + token})
}

type insecureMetadataCreds map[string]string

func (c insecureMetadataCreds) RequireTransportSecurity() bool { return false }
func (c insecureMetadataCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return c, nil
}

// WithInsecureBearerToken is a dial option to add a standard HTTP Bearer token
// to all requests sent from an insecure client.
//
// Must be used in conjunction with `grpc.WithInsecure()`.
func WithInsecureBearerToken(token string) grpc.DialOption {
	return grpc.WithPerRPCCredentials(insecureMetadataCreds{"authorization": "Bearer " + token})
}
