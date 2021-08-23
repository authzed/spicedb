package smartclient

import (
	"context"
	"crypto/tls"

	"github.com/authzed/grpcutil"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/x509util"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// EndpointResolverConfig contains configuration for establishing a connection to
// a dynamic endpoint resolver service.
type EndpointResolverConfig struct {
	endpoint    string
	dialOptions []grpc.DialOption
	err         error
}

// NewEndpointResolver configures a TLS protected endpoint resolver connection.
func NewEndpointResolver(resolverEndpoint, caCertPath string) *EndpointResolverConfig {
	return &EndpointResolverConfig{
		resolverEndpoint,
		[]grpc.DialOption{
			grpcutil.WithCustomCerts(caCertPath, false),
			grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		},
		nil,
	}
}

// NewEndpointResolverNoTLS configures a plaintext endpoint resolver connection.
func NewEndpointResolverNoTLS(resolverEndpoint string) *EndpointResolverConfig {
	return &EndpointResolverConfig{
		resolverEndpoint,
		[]grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		},
		nil,
	}
}

// EndpointConfig contains configuration for establishing connections with a
// specific backend service.
type EndpointConfig struct {
	serviceName string
	dnsName     string
	dialOptions []grpc.DialOption
	err         error
}

// NewEndpointConfig configures TLS protected backend connections.
func NewEndpointConfig(serviceName, dnsName, token, caCertPath string) *EndpointConfig {
	pool, err := x509util.CustomCertPool(caCertPath)
	if err != nil {
		return &EndpointConfig{err: err}
	}
	creds := credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: dnsName})

	return &EndpointConfig{
		serviceName,
		dnsName,
		[]grpc.DialOption{
			grpc.WithTransportCredentials(creds),
			grpcutil.WithBearerToken(token),
			grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		},
		nil,
	}
}

// NewEndpointConfigNoTLS configures plaintext backend connections.
func NewEndpointConfigNoTLS(serviceName, dnsName, token string) *EndpointConfig {
	return &EndpointConfig{
		serviceName,
		dnsName,
		[]grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithPerRPCCredentials(InsecureCreds{"authorization": "Bearer " + token}),
			grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		},
		nil,
	}
}

// FallbackEndpointConfig contains configuration for a static fallback endpoint
// to be used when no resolved endpoints are available.
type FallbackEndpointConfig struct {
	backend *backend
	err     error
}

// NewFallbackEndpoint configures a TLS protected fallback endpoint connection.
func NewFallbackEndpoint(endpoint, token, caCertPath string) *FallbackEndpointConfig {
	endpointClientDialOptions := []grpc.DialOption{
		grpcutil.WithCustomCerts(caCertPath, false),
		grpcutil.WithBearerToken(token),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
	}

	return commonFallbackEndpoint(endpoint, endpointClientDialOptions)
}

// NewFallbackEndpointNoTLS configures a plaintext fallback endpoint connection.
func NewFallbackEndpointNoTLS(endpoint, token string) *FallbackEndpointConfig {
	endpointClientDialOptions := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(InsecureCreds{"authorization": "Bearer " + token}),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
	}

	return commonFallbackEndpoint(endpoint, endpointClientDialOptions)
}

func commonFallbackEndpoint(endpoint string, endpointClientDialOptions []grpc.DialOption) *FallbackEndpointConfig {
	conn, err := grpc.Dial(endpoint, endpointClientDialOptions...)
	if err != nil {
		return &FallbackEndpointConfig{err: err}
	}

	fallback_client := v1.NewDispatchServiceClient(conn)

	fallbackBackend := &backend{
		key:    endpoint,
		client: fallback_client,
	}

	return &FallbackEndpointConfig{
		fallbackBackend,
		nil,
	}
}

// NoFallbackEndpoint disables the fallback backend option.
func NoFallbackEndpoint() *FallbackEndpointConfig {
	return &FallbackEndpointConfig{}
}

type InsecureCreds map[string]string

func (c InsecureCreds) RequireTransportSecurity() bool { return false }
func (c InsecureCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return c, nil
}
