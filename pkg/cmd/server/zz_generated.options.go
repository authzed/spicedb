// Code generated by github.com/ecordell/optgen. DO NOT EDIT.
package server

import (
	datastore "github.com/authzed/spicedb/pkg/cmd/datastore"
	util "github.com/authzed/spicedb/pkg/cmd/util"
	grpc "google.golang.org/grpc"
	"time"
)

type ConfigOption func(c *Config)

// NewConfigWithOptions creates a new Config with the passed in options set
func NewConfigWithOptions(opts ...ConfigOption) *Config {
	c := &Config{}
	for _, o := range opts {
		o(c)
	}
	return c
}

// ConfigWithOptions configures an existing Config with the passed in options set
func ConfigWithOptions(c *Config, opts ...ConfigOption) *Config {
	for _, o := range opts {
		o(c)
	}
	return c
}

// WithGRPCServer returns an option that can set GRPCServer on a Config
func WithGRPCServer(gRPCServer util.GRPCServerConfig) ConfigOption {
	return func(c *Config) {
		c.GRPCServer = gRPCServer
	}
}

// WithPresharedKey returns an option that can set PresharedKey on a Config
func WithPresharedKey(presharedKey string) ConfigOption {
	return func(c *Config) {
		c.PresharedKey = presharedKey
	}
}

// WithShutdownGracePeriod returns an option that can set ShutdownGracePeriod on a Config
func WithShutdownGracePeriod(shutdownGracePeriod time.Duration) ConfigOption {
	return func(c *Config) {
		c.ShutdownGracePeriod = shutdownGracePeriod
	}
}

// WithHTTPGateway returns an option that can set HTTPGateway on a Config
func WithHTTPGateway(hTTPGateway util.HTTPServerConfig) ConfigOption {
	return func(c *Config) {
		c.HTTPGateway = hTTPGateway
	}
}

// WithHTTPGatewayUpstreamAddr returns an option that can set HTTPGatewayUpstreamAddr on a Config
func WithHTTPGatewayUpstreamAddr(hTTPGatewayUpstreamAddr string) ConfigOption {
	return func(c *Config) {
		c.HTTPGatewayUpstreamAddr = hTTPGatewayUpstreamAddr
	}
}

// WithHTTPGatewayUpstreamTLSCertPath returns an option that can set HTTPGatewayUpstreamTLSCertPath on a Config
func WithHTTPGatewayUpstreamTLSCertPath(hTTPGatewayUpstreamTLSCertPath string) ConfigOption {
	return func(c *Config) {
		c.HTTPGatewayUpstreamTLSCertPath = hTTPGatewayUpstreamTLSCertPath
	}
}

// WithHTTPGatewayCorsEnabled returns an option that can set HTTPGatewayCorsEnabled on a Config
func WithHTTPGatewayCorsEnabled(hTTPGatewayCorsEnabled bool) ConfigOption {
	return func(c *Config) {
		c.HTTPGatewayCorsEnabled = hTTPGatewayCorsEnabled
	}
}

// WithHTTPGatewayCorsAllowedOrigins returns an option that can append HTTPGatewayCorsAllowedOriginss to Config.HTTPGatewayCorsAllowedOrigins
func WithHTTPGatewayCorsAllowedOrigins(hTTPGatewayCorsAllowedOrigins string) ConfigOption {
	return func(c *Config) {
		c.HTTPGatewayCorsAllowedOrigins = append(c.HTTPGatewayCorsAllowedOrigins, hTTPGatewayCorsAllowedOrigins)
	}
}

// SetHTTPGatewayCorsAllowedOrigins returns an option that can set HTTPGatewayCorsAllowedOrigins on a Config
func SetHTTPGatewayCorsAllowedOrigins(hTTPGatewayCorsAllowedOrigins []string) ConfigOption {
	return func(c *Config) {
		c.HTTPGatewayCorsAllowedOrigins = hTTPGatewayCorsAllowedOrigins
	}
}

// WithDatastore returns an option that can set Datastore on a Config
func WithDatastore(datastore datastore.Config) ConfigOption {
	return func(c *Config) {
		c.Datastore = datastore
	}
}

// WithNamespaceCacheExpiration returns an option that can set NamespaceCacheExpiration on a Config
func WithNamespaceCacheExpiration(namespaceCacheExpiration time.Duration) ConfigOption {
	return func(c *Config) {
		c.NamespaceCacheExpiration = namespaceCacheExpiration
	}
}

// WithSchemaPrefixesRequired returns an option that can set SchemaPrefixesRequired on a Config
func WithSchemaPrefixesRequired(schemaPrefixesRequired bool) ConfigOption {
	return func(c *Config) {
		c.SchemaPrefixesRequired = schemaPrefixesRequired
	}
}

// WithDispatchServer returns an option that can set DispatchServer on a Config
func WithDispatchServer(dispatchServer util.GRPCServerConfig) ConfigOption {
	return func(c *Config) {
		c.DispatchServer = dispatchServer
	}
}

// WithDispatchMaxDepth returns an option that can set DispatchMaxDepth on a Config
func WithDispatchMaxDepth(dispatchMaxDepth uint32) ConfigOption {
	return func(c *Config) {
		c.DispatchMaxDepth = dispatchMaxDepth
	}
}

// WithDispatchUpstreamAddr returns an option that can set DispatchUpstreamAddr on a Config
func WithDispatchUpstreamAddr(dispatchUpstreamAddr string) ConfigOption {
	return func(c *Config) {
		c.DispatchUpstreamAddr = dispatchUpstreamAddr
	}
}

// WithDispatchUpstreamCAPath returns an option that can set DispatchUpstreamCAPath on a Config
func WithDispatchUpstreamCAPath(dispatchUpstreamCAPath string) ConfigOption {
	return func(c *Config) {
		c.DispatchUpstreamCAPath = dispatchUpstreamCAPath
	}
}

// WithDisableV1SchemaAPI returns an option that can set DisableV1SchemaAPI on a Config
func WithDisableV1SchemaAPI(disableV1SchemaAPI bool) ConfigOption {
	return func(c *Config) {
		c.DisableV1SchemaAPI = disableV1SchemaAPI
	}
}

// WithDashboardAPI returns an option that can set DashboardAPI on a Config
func WithDashboardAPI(dashboardAPI util.HTTPServerConfig) ConfigOption {
	return func(c *Config) {
		c.DashboardAPI = dashboardAPI
	}
}

// WithMetricsAPI returns an option that can set MetricsAPI on a Config
func WithMetricsAPI(metricsAPI util.HTTPServerConfig) ConfigOption {
	return func(c *Config) {
		c.MetricsAPI = metricsAPI
	}
}

// WithUnaryMiddleware returns an option that can append UnaryMiddlewares to Config.UnaryMiddleware
func WithUnaryMiddleware(unaryMiddleware grpc.UnaryServerInterceptor) ConfigOption {
	return func(c *Config) {
		c.UnaryMiddleware = append(c.UnaryMiddleware, unaryMiddleware)
	}
}

// SetUnaryMiddleware returns an option that can set UnaryMiddleware on a Config
func SetUnaryMiddleware(unaryMiddleware []grpc.UnaryServerInterceptor) ConfigOption {
	return func(c *Config) {
		c.UnaryMiddleware = unaryMiddleware
	}
}

// WithStreamingMiddleware returns an option that can append StreamingMiddlewares to Config.StreamingMiddleware
func WithStreamingMiddleware(streamingMiddleware grpc.StreamServerInterceptor) ConfigOption {
	return func(c *Config) {
		c.StreamingMiddleware = append(c.StreamingMiddleware, streamingMiddleware)
	}
}

// SetStreamingMiddleware returns an option that can set StreamingMiddleware on a Config
func SetStreamingMiddleware(streamingMiddleware []grpc.StreamServerInterceptor) ConfigOption {
	return func(c *Config) {
		c.StreamingMiddleware = streamingMiddleware
	}
}

// WithDispatchUnaryMiddleware returns an option that can append DispatchUnaryMiddlewares to Config.DispatchUnaryMiddleware
func WithDispatchUnaryMiddleware(dispatchUnaryMiddleware grpc.UnaryServerInterceptor) ConfigOption {
	return func(c *Config) {
		c.DispatchUnaryMiddleware = append(c.DispatchUnaryMiddleware, dispatchUnaryMiddleware)
	}
}

// SetDispatchUnaryMiddleware returns an option that can set DispatchUnaryMiddleware on a Config
func SetDispatchUnaryMiddleware(dispatchUnaryMiddleware []grpc.UnaryServerInterceptor) ConfigOption {
	return func(c *Config) {
		c.DispatchUnaryMiddleware = dispatchUnaryMiddleware
	}
}

// WithDispatchStreamingMiddleware returns an option that can append DispatchStreamingMiddlewares to Config.DispatchStreamingMiddleware
func WithDispatchStreamingMiddleware(dispatchStreamingMiddleware grpc.StreamServerInterceptor) ConfigOption {
	return func(c *Config) {
		c.DispatchStreamingMiddleware = append(c.DispatchStreamingMiddleware, dispatchStreamingMiddleware)
	}
}

// SetDispatchStreamingMiddleware returns an option that can set DispatchStreamingMiddleware on a Config
func SetDispatchStreamingMiddleware(dispatchStreamingMiddleware []grpc.StreamServerInterceptor) ConfigOption {
	return func(c *Config) {
		c.DispatchStreamingMiddleware = dispatchStreamingMiddleware
	}
}
