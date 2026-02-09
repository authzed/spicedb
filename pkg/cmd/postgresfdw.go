package cmd

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/go-logr/zerologr"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/jzelinskie/cobrautil/v2/cobrazerolog"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/fdw"
	"github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cmd/termination"
	"github.com/authzed/spicedb/pkg/releases"
)

// PostgresFDWConfig is the configuration for the Postgres FDW command.
type PostgresFDWConfig struct {
	// SpiceDB connection config
	SpiceDBEndpoint          string
	SecureSpiceDBAccessToken string
	SpiceDBInsecure          bool

	// Postgres FDW server config
	PostgresEndpoint  string
	PostgresUsername  string // Username that Postgres will use to connect to the FDW proxy
	SecureAccessToken string // Password that Postgres will use to authenticate to the FDW proxy
}

// Complete adapts the PostgresFDWConfig into a runnable Postgres FDW server.
func (c *PostgresFDWConfig) Complete(ctx context.Context) (RunnablePostgresFDWServer, error) {
	systemCerts, err := grpcutil.WithSystemCerts(grpcutil.VerifyCA)
	if err != nil {
		return nil, fmt.Errorf("unable to load system certs: %w", err)
	}

	if c.SpiceDBEndpoint == "" {
		return nil, errors.New("missing SpiceDB endpoint")
	}
	if c.SecureSpiceDBAccessToken == "" {
		return nil, errors.New("missing SpiceDB access token")
	}

	var client *authzed.Client
	if c.SpiceDBInsecure {
		logging.Info().Str("endpoint", c.SpiceDBEndpoint).Msg("connecting to SpiceDB insecurely")
		spicedbClient, err := authzed.NewClient(
			c.SpiceDBEndpoint,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpcutil.WithInsecureBearerToken(c.SecureSpiceDBAccessToken),
		)
		if err != nil {
			return nil, fmt.Errorf("unable to create client: %w", err)
		}
		client = spicedbClient
	} else {
		logging.Info().Str("endpoint", c.SpiceDBEndpoint).Msg("connecting to SpiceDB securely")
		spicedbClient, err := authzed.NewClient(
			c.SpiceDBEndpoint,
			systemCerts,
			grpcutil.WithBearerToken(c.SecureSpiceDBAccessToken),
		)
		if err != nil {
			return nil, fmt.Errorf("unable to create client: %w", err)
		}
		client = spicedbClient
	}

	if c.SecureAccessToken == "" {
		return nil, errors.New("missing Postgres secure access token")
	}

	if c.PostgresUsername == "" {
		return nil, errors.New("missing Postgres username")
	}

	pg := fdw.NewPgBackend(client, c.PostgresUsername, c.SecureAccessToken)
	return &postgresFDWServer{pg: pg, address: c.PostgresEndpoint}, nil
}

type postgresFDWServer struct {
	pg      *fdw.PgBackend
	address string
}

func (s *postgresFDWServer) Run(ctx context.Context) error {
	serverError := make(chan error)

	go func() {
		err := s.pg.Run(ctx, s.address)
		serverError <- err
	}()

	select {
	case err := <-serverError:
		return err
	case <-ctx.Done():
		return s.pg.Close()
	}
}

func (s *postgresFDWServer) Close() error {
	return s.pg.Close()
}

// RunnablePostgresFDWServer is a Postgres FDW server ready to run
type RunnablePostgresFDWServer interface {
	// Run runs the server until the context is cancelled or until the server returns an error.
	Run(ctx context.Context) error
	Close() error
}

func RegisterPostgresFDWFlags(cmd *cobra.Command, config *PostgresFDWConfig) error {
	nfs := cobrautil.NewNamedFlagSets(cmd)

	spicedbFlagSet := nfs.FlagSet(BoldBlue("spicedb"))
	spicedbFlagSet.StringVar(&config.SpiceDBEndpoint, "spicedb-api-endpoint", "localhost:50051", "SpiceDB API endpoint")
	spicedbFlagSet.StringVar(&config.SecureSpiceDBAccessToken, "spicedb-access-token-secret", "", "(required) Access token for calling the SpiceDB API")
	spicedbFlagSet.BoolVar(&config.SpiceDBInsecure, "spicedb-insecure", false, "Use insecure connection to SpiceDB API")
	if err := cobra.MarkFlagRequired(spicedbFlagSet, "spicedb-access-token-secret"); err != nil {
		return fmt.Errorf("failed to mark flag as required: %w", err)
	}

	serviceFlags := nfs.FlagSet(BoldBlue("service"))
	serviceFlags.StringVar(&config.PostgresEndpoint, "postgres-endpoint", ":5432", "The endpoint at which to serve the Postgres protocol")
	serviceFlags.StringVar(&config.PostgresUsername, "postgres-username", "postgres", "The username that Postgres will use to connect to the FDW proxy")
	serviceFlags.StringVar(&config.SecureAccessToken, "postgres-access-token-secret", "", "(required) The password that Postgres will use to authenticate to the FDW proxy (configured in the Postgres FDW extension's OPTIONS)")
	if err := cobra.MarkFlagRequired(serviceFlags, "postgres-access-token-secret"); err != nil {
		return fmt.Errorf("failed to mark flag as required: %w", err)
	}

	// Attach the created flagsets to the command - they're created but not registered
	// until this function is called.
	nfs.AddFlagSets(cmd)

	return nil
}

func NewPostgresFDWCommand(programName string, config *PostgresFDWConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "postgres-fdw",
		Short: "serve a Postgres Foreign Data Wrapper for SpiceDB (EXPERIMENTAL)",
		Long:  "EXPERIMENTAL: Serves a Postgres-compatible interface for querying SpiceDB data using foreign data wrappers. This feature is experimental and subject to change.",
		PreRunE: cobrautil.CommandStack(
			cobrautil.SyncViperDotEnvPreRunE(programName, "spicedb.env", zerologr.New(&logging.Logger)),
			cobrazerolog.New(
				cobrazerolog.WithTarget(func(logger zerolog.Logger) {
					logging.SetGlobalLogger(logger)
				}),
			).RunE(),
			releases.CheckAndLogRunE(),
		),
		RunE: termination.PublishError(func(cmd *cobra.Command, args []string) error {
			srv, err := config.Complete(cmd.Context())
			if err != nil {
				return err
			}

			signalctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			return srv.Run(signalctx)
		}),
	}
}
