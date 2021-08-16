package smartclient

import (
	"context"
	"crypto/tls"

	client_v0 "github.com/authzed/authzed-go/v0"
	client_v1alpha1 "github.com/authzed/authzed-go/v1alpha1"
	"github.com/authzed/grpcutil"
	"github.com/authzed/spicedb/pkg/x509util"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type EndpointResolverConfig struct {
	endpoint    string
	dialOptions []grpc.DialOption
	err         error
}

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

type EndpointConfig struct {
	dnsName     string
	dialOptions []grpc.DialOption
	err         error
}

func NewEndpointConfig(dnsName, token, caCertPath string) *EndpointConfig {
	pool, err := x509util.CustomCertPool(caCertPath)
	if err != nil {
		return &EndpointConfig{err: err}
	}
	creds := credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: dnsName})

	return &EndpointConfig{
		dnsName,
		[]grpc.DialOption{
			grpc.WithTransportCredentials(creds),
			grpcutil.WithBearerToken(token),
			grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		},
		nil,
	}
}

func NewEndpointConfigNoTLS(dnsName, token string) *EndpointConfig {
	return &EndpointConfig{
		dnsName,
		[]grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithPerRPCCredentials(InsecureCreds{"authorization": "Bearer " + token}),
			grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		},
		nil,
	}
}

type FallbackEndpointConfig struct {
	backend *backend
	err     error
}

func NewFallbackEndpoint(endpoint, token, caCertPath string) *FallbackEndpointConfig {
	endpointClientDialOptions := []grpc.DialOption{
		grpcutil.WithCustomCerts(caCertPath, false),
		grpcutil.WithBearerToken(token),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
	}

	return commonFallbackEndpoint(endpoint, endpointClientDialOptions)
}

func NewFallbackEndpointNoTLS(endpoint, token string) *FallbackEndpointConfig {
	endpointClientDialOptions := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(InsecureCreds{"authorization": "Bearer " + token}),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
	}

	return commonFallbackEndpoint(endpoint, endpointClientDialOptions)
}

func commonFallbackEndpoint(endpoint string, endpointClientDialOptions []grpc.DialOption) *FallbackEndpointConfig {
	fallback_v0, err := client_v0.NewClient(endpoint, endpointClientDialOptions...)
	if err != nil {
		return &FallbackEndpointConfig{err: err}
	}

	fallback_v1alpha1, err := client_v1alpha1.NewClient(endpoint, endpointClientDialOptions...)
	if err != nil {
		return &FallbackEndpointConfig{err: err}
	}

	fallbackBackend := &backend{
		key:             endpoint,
		client_v0:       fallback_v0,
		client_v1alpha1: fallback_v1alpha1,
	}

	return &FallbackEndpointConfig{
		fallbackBackend,
		nil,
	}
}

func NoFallbackEndpoint() *FallbackEndpointConfig {
	return &FallbackEndpointConfig{}
}

type InsecureCreds map[string]string

func (c InsecureCreds) RequireTransportSecurity() bool { return false }
func (c InsecureCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return c, nil
}
