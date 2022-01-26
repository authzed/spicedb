package cmd

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

type GRPCServerOption func(*ServerConfig)

type GRPCServerConfig struct {
	Address     string
	Network     string
	TLSCertPath string
	TLSKeyPath  string
	MaxConnAge  time.Duration
	Enabled     bool

	flagPrefix string
}

// RegisterGRPCServerFlags adds the following flags for use with
// GrpcServerFromFlags:
// - "$PREFIX-addr"
// - "$PREFIX-tls-cert-path"
// - "$PREFIX-tls-key-path"
// - "$PREFIX-max-conn-age"
func RegisterGRPCServerFlags(flags *pflag.FlagSet, config *GRPCServerConfig, flagPrefix, serviceName, defaultAddr string, defaultEnabled bool) {
	flagPrefix = stringz.DefaultEmpty(flagPrefix, "grpc")
	serviceName = stringz.DefaultEmpty(serviceName, "grpc")
	defaultAddr = stringz.DefaultEmpty(defaultAddr, ":50051")
	config.flagPrefix = flagPrefix

	flags.StringVar(&config.Address, flagPrefix+"-addr", defaultAddr, "address to listen on to serve "+serviceName)
	flags.StringVar(&config.Network, flagPrefix+"-network", "tcp", "network type to serve "+serviceName+` ("tcp", "tcp4", "tcp6", "unix", "unixpacket")`)
	flags.StringVar(&config.TLSCertPath, flagPrefix+"-tls-cert-path", "", "local path to the TLS certificate used to serve "+serviceName)
	flags.StringVar(&config.TLSKeyPath, flagPrefix+"-tls-key-path", "", "local path to the TLS key used to serve "+serviceName)
	flags.DurationVar(&config.MaxConnAge, flagPrefix+"-max-conn-age", 30*time.Second, "how long a connection serving "+serviceName+" should be able to live")
	flags.BoolVar(&config.Enabled, flagPrefix+"-enabled", defaultEnabled, "enable "+serviceName+" gRPC server")
}

// Complete takes a set of default options and returns a completed server
func (c *GRPCServerConfig) Complete(level zerolog.Level, svcRegistrationFn func(server *grpc.Server), opts ...grpc.ServerOption) (RunnableGRPCServer, error) {
	opts = append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionAge: c.MaxConnAge,
	}))
	switch {
	case c.TLSCertPath == "" && c.TLSKeyPath == "":
		log.Warn().Str("prefix", c.flagPrefix).Msg("grpc server serving plaintext")
	case c.TLSCertPath != "" && c.TLSKeyPath != "":
		creds, err := credentials.NewServerTLSFromFile(c.TLSCertPath, c.TLSKeyPath)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.Creds(creds))
	}
	l, err := net.Listen(c.Network, c.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on addr for gRPC server: %w", err)
	}
	log.WithLevel(level).Str("addr", c.Address).Str("network", c.Network).
		Str("prefix", c.flagPrefix).Msg("grpc server started listening")

	srv := grpc.NewServer(opts...)
	svcRegistrationFn(srv)
	return &completedGRPCServer{
		opts:              opts,
		listener:          l,
		svcRegistrationFn: svcRegistrationFn,
		listenFunc: func() error {
			return srv.Serve(l)
		},
		prestopFunc: func() {
			log.WithLevel(level).Str("addr", c.Address).Str("network", c.Network).
				Str("prefix", c.flagPrefix).Msg("grpc server stopped listening")
		},
		stopFunc: srv.GracefulStop,
		enabled:  c.Enabled,
	}, nil
}

type RunnableGRPCServer interface {
	WithOpts(opts ...grpc.ServerOption) RunnableGRPCServer
	Listen() error
	GracefulStop()
}

type completedGRPCServer struct {
	opts              []grpc.ServerOption
	listener          net.Listener
	svcRegistrationFn func(server *grpc.Server)
	listenFunc        func() error
	prestopFunc       func()
	stopFunc          func()
	enabled           bool
}

// WithOpts adds to the options for running the server
func (c *completedGRPCServer) WithOpts(opts ...grpc.ServerOption) RunnableGRPCServer {
	c.opts = append(c.opts, opts...)
	srv := grpc.NewServer(c.opts...)
	c.svcRegistrationFn(srv)
	c.listenFunc = func() error {
		return srv.Serve(c.listener)
	}
	c.stopFunc = srv.GracefulStop
	return c
}

// Listen runs a configured server
func (c *completedGRPCServer) Listen() error {
	if !c.enabled {
		return nil
	}
	return c.listenFunc()
}

// GracefulStop stops a running server
func (c *completedGRPCServer) GracefulStop() {
	c.prestopFunc()
	c.stopFunc()
}

type HTTPServerConfig struct {
	Address     string
	TLSCertPath string
	TLSKeyPath  string
	Enabled     bool

	flagPrefix string
}

func (c *HTTPServerConfig) Complete(level zerolog.Level, handler http.Handler) (RunnableHTTPServer, error) {
	srv := &http.Server{
		Addr:    c.Address,
		Handler: handler,
	}
	var serveFunc func() error
	switch {
	case c.TLSCertPath == "" && c.TLSKeyPath == "":
		serveFunc = func() error {
			log.Warn().Str("addr", srv.Addr).Str("prefix", c.flagPrefix).Msg("http server serving plaintext")
			return srv.ListenAndServe()
		}

	case c.TLSCertPath != "" && c.TLSKeyPath != "":
		serveFunc = func() error {
			log.WithLevel(level).Str("addr", srv.Addr).Str("prefix", c.flagPrefix).Msg("https server started serving")
			return srv.ListenAndServeTLS(c.TLSCertPath, c.TLSKeyPath)
		}
	default:
		return nil, fmt.Errorf("failed to start http server: must provide both --%s-tls-cert-path and --%s-tls-key-path",
			c.flagPrefix,
			c.flagPrefix,
		)
	}

	return &completedHTTPServer{
		srvFunc: func() error {
			if err := serveFunc(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("failed while serving https: %w", err)
			}
			return nil
		},
		closeFunc: func() {
			if err := srv.Close(); err != nil {
				log.Warn().Str("addr", srv.Addr).Str("prefix", c.flagPrefix).Err(err).Msg("error stopping http server")
			}
			log.WithLevel(level).Str("addr", srv.Addr).Str("prefix", c.flagPrefix).Msg("http server stopped serving")
		},
		enabled: c.Enabled,
	}, nil
}

type RunnableHTTPServer interface {
	ListenAndServe() error
	Close()
}

type completedHTTPServer struct {
	srvFunc   func() error
	closeFunc func()
	enabled   bool
}

func (c *completedHTTPServer) ListenAndServe() error {
	if !c.enabled {
		return nil
	}
	return c.srvFunc()
}

func (c *completedHTTPServer) Close() {
	c.closeFunc()
}

// RegisterHTTPServerFlags adds the following flags for use with
// HttpServerFromFlags:
// - "$PREFIX-addr"
// - "$PREFIX-tls-cert-path"
// - "$PREFIX-tls-key-path"
// - "$PREFIX-enabled"
func RegisterHTTPServerFlags(flags *pflag.FlagSet, config *HTTPServerConfig, flagPrefix, serviceName, defaultAddr string, defaultEnabled bool) {
	flagPrefix = stringz.DefaultEmpty(flagPrefix, "http")
	serviceName = stringz.DefaultEmpty(serviceName, "http")
	defaultAddr = stringz.DefaultEmpty(defaultAddr, ":8443")
	config.flagPrefix = flagPrefix
	flags.StringVar(&config.Address, flagPrefix+"-addr", defaultAddr, "address to listen on to serve "+serviceName)
	flags.StringVar(&config.TLSCertPath, flagPrefix+"-tls-cert-path", "", "local path to the TLS certificate used to serve "+serviceName)
	flags.StringVar(&config.TLSKeyPath, flagPrefix+"-tls-key-path", "", "local path to the TLS key used to serve "+serviceName)
	flags.BoolVar(&config.Enabled, flagPrefix+"-enabled", defaultEnabled, "enable "+serviceName+" http server")
}
