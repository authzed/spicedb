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

// Server stamps out a server according to the config and passed options
// A server created this way should be started with Listen
func (c *GRPCServerConfig) Server(opts ...grpc.ServerOption) (*grpc.Server, error) {
	opts = append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionAge: c.MaxConnAge,
	}))
	switch {
	case c.TLSCertPath == "" && c.TLSKeyPath == "":
		log.Warn().Str("prefix", c.flagPrefix).Msg("grpc server serving plaintext")
		return grpc.NewServer(opts...), nil
	case c.TLSCertPath != "" && c.TLSKeyPath != "":
		creds, err := credentials.NewServerTLSFromFile(c.TLSCertPath, c.TLSKeyPath)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.Creds(creds))
		return grpc.NewServer(opts...), nil
	default:
		return nil, fmt.Errorf(
			"failed to start gRPC server: must provide both --%s-tls-cert-path and --%s-tls-key-path",
			c.flagPrefix,
			c.flagPrefix,
		)
	}
}

// Listen starts grpc server created by Server
func (c *GRPCServerConfig) Listen(srv *grpc.Server, level zerolog.Level) error {
	if !c.Enabled {
		return nil
	}

	l, err := net.Listen(c.Network, c.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on addr for gRPC server: %w", err)
	}

	log.WithLevel(level).Str("addr", c.Address).Str("network", c.Network).
		Str("prefix", c.flagPrefix).Msg("grpc server started listening")

	if err := srv.Serve(l); err != nil {
		return fmt.Errorf("failed to serve gRPC: %w", err)
	}
	return nil
}

type HTTPServerConfig struct {
	Address     string
	TLSCertPath string
	TLSKeyPath  string
	Enabled     bool

	Handler    http.Handler
	flagPrefix string
}

func (c *HTTPServerConfig) ListenAndServe(level zerolog.Level) (*http.Server, error) {
	srv := &http.Server{
		Addr: c.Address,
	}
	srv.Handler = c.Handler

	if !c.Enabled {
		return srv, nil
	}

	switch {
	case c.TLSCertPath == "" && c.TLSKeyPath == "":
		log.Warn().Str("addr", srv.Addr).Str("prefix", c.flagPrefix).Msg("http server serving plaintext")

		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return srv, fmt.Errorf("failed while serving http: %w", err)
		}
		return srv, nil
	case c.TLSCertPath != "" && c.TLSKeyPath != "":
		log.WithLevel(level).Str("addr", srv.Addr).Str("prefix", c.flagPrefix).Msg("https server started serving")
		if err := srv.ListenAndServeTLS(c.TLSCertPath, c.TLSKeyPath); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return srv, fmt.Errorf("failed while serving https: %w", err)
		}
		return srv, nil
	default:
		return srv, fmt.Errorf("failed to start http server: must provide both --%s-tls-cert-path and --%s-tls-key-path",
			c.flagPrefix,
			c.flagPrefix,
		)
	}
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
