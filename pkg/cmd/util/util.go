package util

//go:generate go run github.com/ecordell/optgen -output zz_generated.options.go . GRPCServerConfig HTTPServerConfig

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/jzelinskie/cobrautil/v2/cobraotel"
	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/test/bufconn"

	// Register Snappy S2 compression
	_ "github.com/mostynb/go-grpc-compression/experimental/s2"

	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	// Register cert watcher metrics
	_ "sigs.k8s.io/controller-runtime/pkg/certwatcher/metrics"

	"github.com/authzed/spicedb/internal/grpchelpers"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cmd/termination"
	"github.com/authzed/spicedb/pkg/runtime"
	"github.com/authzed/spicedb/pkg/x509util"
)

const BufferedNetwork string = "buffnet"

type GRPCServerConfig struct {
	Address      string        `debugmap:"visible"`
	Network      string        `debugmap:"visible"`
	TLSCertPath  string        `debugmap:"visible"`
	TLSKeyPath   string        `debugmap:"visible"`
	MaxConnAge   time.Duration `debugmap:"visible"`
	Enabled      bool          `debugmap:"visible"`
	BufferSize   int           `debugmap:"visible"`
	ClientCAPath string        `debugmap:"visible"`
	MaxWorkers   uint32        `debugmap:"visible"`

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
	flags.Uint32Var(&config.MaxWorkers, flagPrefix+"-max-workers", 0, "set the number of workers for this server (0 value means 1 worker per request)")
}

type (
	DialFunc    func(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error)
	NetDialFunc func(ctx context.Context, s string) (net.Conn, error)
)

// Complete takes a set of default options and returns a completed server
func (c *GRPCServerConfig) Complete(level zerolog.Level, svcRegistrationFn func(server *grpc.Server), opts ...grpc.ServerOption) (RunnableGRPCServer, error) {
	if !c.Enabled {
		return &disabledGrpcServer{}, nil
	}
	if c.BufferSize == 0 {
		c.BufferSize = 1024 * 1024
	}
	opts = append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionAge: c.MaxConnAge,
	}), grpc.NumStreamWorkers(c.MaxWorkers))

	tlsOpts, certWatcher, err := c.tlsOpts()
	if err != nil {
		return nil, err
	}
	opts = append(opts, tlsOpts...)

	clientCreds, err := c.clientCreds()
	if err != nil {
		return nil, err
	}

	l, dial, netDial, err := c.listenerAndDialer()
	if err != nil {
		return nil, fmt.Errorf("failed to listen on addr for gRPC server: %w", err)
	}
	log.WithLevel(level).
		Str("addr", c.Address).
		Str("network", c.Network).
		Str("service", c.flagPrefix).
		Uint32("workers", c.MaxWorkers).
		Bool("insecure", c.TLSCertPath == "" && c.TLSKeyPath == "").
		Msg("grpc server started serving")

	srv := grpc.NewServer(opts...)
	svcRegistrationFn(srv)
	return &completedGRPCServer{
		opts:              opts,
		listener:          l,
		svcRegistrationFn: svcRegistrationFn,
		listenFunc: func() error {
			return srv.Serve(l)
		},
		dial:    dial,
		netDial: netDial,
		prestopFunc: func() {
			log.WithLevel(level).
				Str("addr", c.Address).
				Str("network", c.Network).
				Str("service", c.flagPrefix).
				Msg("grpc server stopped serving")
		},
		stopFunc:    srv.GracefulStop,
		creds:       clientCreds,
		certWatcher: certWatcher,
	}, nil
}

func (c *GRPCServerConfig) listenerAndDialer() (net.Listener, DialFunc, NetDialFunc, error) {
	if c.Network == BufferedNetwork {
		bl := bufconn.Listen(c.BufferSize)
		return bl, func(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
				opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
					return bl.DialContext(ctx)
				}))

				return grpchelpers.Dial(ctx, BufferedNetwork, opts...)
			}, func(ctx context.Context, s string) (net.Conn, error) {
				return bl.DialContext(ctx)
			}, nil
	}
	l, err := net.Listen(c.Network, c.Address)
	if err != nil {
		return nil, nil, nil, err
	}
	return l, func(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
		return grpchelpers.Dial(ctx, c.Address, opts...)
	}, nil, nil
}

func (c *GRPCServerConfig) tlsOpts() ([]grpc.ServerOption, *certwatcher.CertWatcher, error) {
	switch {
	case c.TLSCertPath == "" && c.TLSKeyPath == "":
		return nil, nil, nil
	case c.TLSCertPath != "" && c.TLSKeyPath != "":
		watcher, err := certwatcher.New(c.TLSCertPath, c.TLSKeyPath)
		if err != nil {
			return nil, nil, err
		}
		creds := credentials.NewTLS(&tls.Config{
			GetCertificate: watcher.GetCertificate,
			MinVersion:     tls.VersionTLS12,
		})
		return []grpc.ServerOption{grpc.Creds(creds)}, watcher, nil
	default:
		return nil, nil, nil
	}
}

func (c *GRPCServerConfig) clientCreds() (credentials.TransportCredentials, error) {
	switch {
	case c.TLSCertPath == "" && c.TLSKeyPath == "":
		return insecure.NewCredentials(), nil
	case c.TLSCertPath != "" && c.TLSKeyPath != "":
		var err error
		var pool *x509.CertPool
		if c.ClientCAPath != "" {
			pool, err = x509util.CustomCertPool(c.ClientCAPath)
		} else {
			pool, err = x509.SystemCertPool()
		}
		if err != nil {
			return nil, err
		}

		return credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}), nil
	default:
		return nil, nil
	}
}

type RunnableGRPCServer interface {
	WithOpts(opts ...grpc.ServerOption) RunnableGRPCServer
	Listen(ctx context.Context) func() error
	DialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error)
	NetDialContext(ctx context.Context, s string) (net.Conn, error)
	Insecure() bool
	GracefulStop()
}

type completedGRPCServer struct {
	opts              []grpc.ServerOption
	listener          net.Listener
	svcRegistrationFn func(*grpc.Server)
	listenFunc        func() error
	prestopFunc       func()
	stopFunc          func()
	dial              func(context.Context, ...grpc.DialOption) (*grpc.ClientConn, error)
	netDial           func(ctx context.Context, s string) (net.Conn, error)
	creds             credentials.TransportCredentials
	certWatcher       *certwatcher.CertWatcher
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
func (c *completedGRPCServer) Listen(ctx context.Context) func() error {
	if c.certWatcher != nil {
		go func() {
			if err := c.certWatcher.Start(ctx); err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("error watching tls certs")
			}
		}()
	}
	return c.listenFunc
}

// DialContext starts a connection to grpc server
func (c *completedGRPCServer) DialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts = append(opts, grpc.WithTransportCredentials(c.creds))
	return c.dial(ctx, opts...)
}

// NetDialContext returns a low level net.Conn connection to the server
func (c *completedGRPCServer) NetDialContext(ctx context.Context, s string) (net.Conn, error) {
	return c.netDial(ctx, s)
}

// Insecure returns true if the server is configured without TLS enabled
func (c *completedGRPCServer) Insecure() bool {
	return c.creds.Info().SecurityProtocol == "insecure"
}

// GracefulStop stops a running server
func (c *completedGRPCServer) GracefulStop() {
	c.prestopFunc()
	c.stopFunc()
}

type disabledGrpcServer struct{}

// WithOpts adds to the options for running the server
func (d *disabledGrpcServer) WithOpts(_ ...grpc.ServerOption) RunnableGRPCServer {
	return d
}

// Listen runs a configured server
func (d *disabledGrpcServer) Listen(_ context.Context) func() error {
	return func() error {
		return nil
	}
}

// Insecure returns true if the server is configured without TLS enabled
func (d *disabledGrpcServer) Insecure() bool {
	return true
}

// DialContext starts a connection to grpc server
func (d *disabledGrpcServer) DialContext(_ context.Context, _ ...grpc.DialOption) (*grpc.ClientConn, error) {
	return nil, nil
}

// NetDialContext starts a connection to grpc server
func (d *disabledGrpcServer) NetDialContext(_ context.Context, _ string) (net.Conn, error) {
	return nil, nil
}

// GracefulStop stops a running server
func (d *disabledGrpcServer) GracefulStop() {}

type HTTPServerConfig struct {
	HTTPAddress     string `debugmap:"visible"`
	HTTPTLSCertPath string `debugmap:"visible"`
	HTTPTLSKeyPath  string `debugmap:"visible"`
	HTTPEnabled     bool   `debugmap:"visible"`

	flagPrefix string
}

func (c *HTTPServerConfig) Complete(level zerolog.Level, handler http.Handler) (RunnableHTTPServer, error) {
	if !c.HTTPEnabled {
		return &disabledHTTPServer{}, nil
	}
	srv := &http.Server{
		Addr:              c.HTTPAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	var serveFunc func() error
	switch {
	case c.HTTPTLSCertPath == "" && c.HTTPTLSKeyPath == "":
		serveFunc = func() error {
			log.WithLevel(level).
				Str("addr", srv.Addr).
				Str("service", c.flagPrefix).
				Bool("insecure", c.HTTPTLSCertPath == "" && c.HTTPTLSKeyPath == "").
				Msg("http server started serving")
			return srv.ListenAndServe()
		}

	case c.HTTPTLSCertPath != "" && c.HTTPTLSKeyPath != "":
		watcher, err := certwatcher.New(c.HTTPTLSCertPath, c.HTTPTLSKeyPath)
		if err != nil {
			return nil, err
		}

		listener, err := tls.Listen("tcp", srv.Addr, &tls.Config{
			GetCertificate: watcher.GetCertificate,
			MinVersion:     tls.VersionTLS12,
		})
		if err != nil {
			return nil, err
		}
		serveFunc = func() error {
			log.WithLevel(level).
				Str("addr", srv.Addr).
				Str("prefix", c.flagPrefix).
				Bool("insecure", c.HTTPTLSCertPath == "" && c.HTTPTLSKeyPath == "").
				Msg("http server started serving")
			return srv.Serve(listener)
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
				return fmt.Errorf("failed while serving http: %w", err)
			}
			return nil
		},
		closeFunc: func() {
			if err := srv.Close(); err != nil {
				log.Error().Str("addr", srv.Addr).Str("service", c.flagPrefix).Err(err).Msg("error stopping http server")
			}
			log.WithLevel(level).Str("addr", srv.Addr).Str("service", c.flagPrefix).Msg("http server stopped serving")
		},
		enabled: c.HTTPEnabled,
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
	flags.StringVar(&config.HTTPAddress, flagPrefix+"-addr", defaultAddr, "address to listen on to serve "+serviceName)
	flags.StringVar(&config.HTTPTLSCertPath, flagPrefix+"-tls-cert-path", "", "local path to the TLS certificate used to serve "+serviceName)
	flags.StringVar(&config.HTTPTLSKeyPath, flagPrefix+"-tls-key-path", "", "local path to the TLS key used to serve "+serviceName)
	flags.BoolVar(&config.HTTPEnabled, flagPrefix+"-enabled", defaultEnabled, "enable http "+serviceName+" server")
}

// RegisterDeprecatedHTTPServerFlags registers a set of HTTP server flags as fully deprecated, for a removed HTTP service.
func RegisterDeprecatedHTTPServerFlags(cmd *cobra.Command, flagPrefix, serviceName string) error {
	ignored1 := ""
	ignored2 := ""
	ignored3 := ""
	ignored4 := false
	flags := cmd.Flags()

	flags.StringVar(&ignored1, flagPrefix+"-addr", "", "address to listen on to serve "+serviceName)
	flags.StringVar(&ignored2, flagPrefix+"-tls-cert-path", "", "local path to the TLS certificate used to serve "+serviceName)
	flags.StringVar(&ignored3, flagPrefix+"-tls-key-path", "", "local path to the TLS key used to serve "+serviceName)
	flags.BoolVar(&ignored4, flagPrefix+"-enabled", false, "enable http "+serviceName+" server")

	if err := cmd.Flags().MarkDeprecated(flagPrefix+"-addr", "service has been removed; flag is a no-op"); err != nil {
		return fmt.Errorf("failed to mark flag as deprecated: %w", err)
	}
	if err := cmd.Flags().MarkDeprecated(flagPrefix+"-tls-cert-path", "service has been removed; flag is a no-op"); err != nil {
		return fmt.Errorf("failed to mark flag as deprecated: %w", err)
	}
	if err := cmd.Flags().MarkDeprecated(flagPrefix+"-tls-key-path", "service has been removed; flag is a no-op"); err != nil {
		return fmt.Errorf("failed to mark flag as deprecated: %w", err)
	}
	if err := cmd.Flags().MarkDeprecated(flagPrefix+"-enabled", "service has been removed; flag is a no-op"); err != nil {
		return fmt.Errorf("failed to mark flag as deprecated: %w", err)
	}

	return nil
}

type disabledHTTPServer struct{}

func (d *disabledHTTPServer) ListenAndServe() error {
	return nil
}

func (d *disabledHTTPServer) Close() {}

// Registers flags that are common to many commands.
// NOTE: these used to be registered in the root command
// so that they were shared across all commands, but this
// made it difficult to organize the flags, so we lifted them here.
func RegisterCommonFlags(cmd *cobra.Command) {
	otel := cobraotel.New("spicedb")
	otel.RegisterFlags(cmd.Flags())
	termination.RegisterFlags(cmd.Flags())
	runtime.RegisterFlags(cmd.Flags())
}
