// Package sdbtestcontainer provides a testcontainers module for SpiceDB.
//
// It runs `spicedb serve` (in-memory datastore by default) with a randomized
// preshared key, exposing the gRPC endpoint and preshared key via methods.
// Pass WithHTTP to additionally enable and expose the HTTP gateway.
package sdbtestcontainer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	// DefaultImage is the SpiceDB Docker image repository.
	DefaultImage = "authzed/spicedb"
	// DefaultTag is the default SpiceDB Docker image tag.
	DefaultTag = "latest"
	// DefaultImageReference is the fully-qualified default image.
	DefaultImageReference = DefaultImage + ":" + DefaultTag

	grpcPort = "50051/tcp"
	httpPort = "8443/tcp"
)

// Container is a running SpiceDB server. Call Terminate to stop it.
type Container struct {
	testcontainers.Container
	presharedKey string
	grpcEndpoint string
	httpEndpoint string
	httpEnabled  bool
}

// PresharedKey returns the gRPC preshared key the server was started with.
// Use it as a bearer token when authenticating API calls.
func (c *Container) PresharedKey() string {
	return c.presharedKey
}

// Endpoint returns the gRPC endpoint as "host:port", reachable from the host.
func (c *Container) Endpoint() string {
	return c.grpcEndpoint
}

// HTTPEndpoint returns the HTTP gateway endpoint as "host:port", reachable from
// the host. It returns an error if the HTTP gateway was not enabled via WithHTTP.
func (c *Container) HTTPEndpoint() (string, error) {
	if !c.httpEnabled {
		return "", errors.New("http gateway is not enabled; pass WithHTTP() to Run")
	}
	return c.httpEndpoint, nil
}

// Run starts a SpiceDB server and waits for it to accept connections.
//
// img is the image to run (use DefaultImageReference for the default). opts may
// be any testcontainers.ContainerCustomizer; the WithXxx options in this package
// configure SpiceDB-specific behavior, while generic customizers (e.g.
// testcontainers.WithEnv) are applied to the underlying container request.
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	cfg := &options{
		datastoreEngine: "memory",
		logLevel:        "info",
	}
	for _, opt := range opts {
		if o, ok := opt.(customizer); ok {
			o.apply(cfg)
		}
	}

	if cfg.presharedKey == "" {
		key, err := randomKey()
		if err != nil {
			return nil, fmt.Errorf("generate preshared key: %w", err)
		}
		cfg.presharedKey = key
	}

	exposed := []string{grpcPort}
	// Wait for both the port to accept connections and the "serving" log line.
	// The port alone can accept before the gRPC server is actually serving
	// (leading to "connection reset" on an immediate dial); the dispatch-cluster
	// gRPC server is disabled by default, so this log line is emitted exactly
	// once, by the main gRPC server.
	waits := []wait.Strategy{
		wait.ForListeningPort(grpcPort),
		wait.ForLog("grpc server started serving"),
	}
	if cfg.httpEnabled {
		exposed = append(exposed, httpPort)
		waits = append(waits, wait.ForListeningPort(httpPort))
	}

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        img,
			Cmd:          buildArgs(cfg),
			ExposedPorts: exposed,
			WaitingFor:   wait.ForAll(waits...),
		},
		Started: true,
	}
	for _, opt := range filterOpts(opts) {
		if err := opt.Customize(&req); err != nil {
			return nil, fmt.Errorf("apply option: %w", err)
		}
	}

	ctr, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		return nil, err
	}

	c := &Container{
		Container:    ctr,
		presharedKey: cfg.presharedKey,
		httpEnabled:  cfg.httpEnabled,
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("get container host: %w", err), ctr.Terminate(ctx))
	}

	grpcMapped, err := ctr.MappedPort(ctx, grpcPort)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("get mapped gRPC port: %w", err), ctr.Terminate(ctx))
	}
	c.grpcEndpoint = net.JoinHostPort(host, grpcMapped.Port())

	if cfg.httpEnabled {
		httpMapped, err := ctr.MappedPort(ctx, httpPort)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("get mapped HTTP port: %w", err), ctr.Terminate(ctx))
		}
		c.httpEndpoint = net.JoinHostPort(host, httpMapped.Port())
	}

	if cfg.bootstrapSchema != "" || len(cfg.bootstrapRelationships) > 0 {
		if err := c.bootstrap(ctx, cfg); err != nil {
			return nil, errors.Join(err, ctr.Terminate(ctx))
		}
	}

	return c, nil
}

// buildArgs assembles the `spicedb serve` command line from the configuration.
func buildArgs(cfg *options) []string {
	args := []string{
		"serve",
		"--grpc-preshared-key", cfg.presharedKey,
		"--datastore-engine", cfg.datastoreEngine,
		"--log-level", cfg.logLevel,
		// Avoid phoning home from tests.
		"--telemetry-endpoint", "",
	}
	if cfg.datastoreConnURI != "" {
		args = append(args, "--datastore-conn-uri", cfg.datastoreConnURI)
	}
	if cfg.httpEnabled {
		args = append(args, "--http-enabled", "--http-addr", ":8443")
	}
	args = append(args, cfg.extraArgs...)
	return args
}

// bootstrap writes the configured schema and relationships over the gRPC API.
func (c *Container) bootstrap(ctx context.Context, cfg *options) error {
	if cfg.bootstrapSchema == "" {
		return nil
	}
	conn, err := grpc.NewClient(
		c.grpcEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpcutil.WithInsecureBearerToken(c.presharedKey),
	)
	if err != nil {
		return fmt.Errorf("dial for bootstrap: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := v1.NewSchemaServiceClient(conn).WriteSchema(ctx, &v1.WriteSchemaRequest{
		Schema: cfg.bootstrapSchema,
	}); err != nil {
		return fmt.Errorf("write bootstrap schema: %w", err)
	}

	if len(cfg.bootstrapRelationships) > 0 {
		updates := make([]*v1.RelationshipUpdate, 0, len(cfg.bootstrapRelationships))
		for _, rel := range cfg.bootstrapRelationships {
			parsed, err := tuple.Parse(rel)
			if err != nil {
				return fmt.Errorf("parse bootstrap relationship %q: %w", rel, err)
			}
			update, err := tuple.UpdateToV1RelationshipUpdate(tuple.Touch(parsed))
			if err != nil {
				return fmt.Errorf("convert bootstrap relationship %q: %w", rel, err)
			}
			updates = append(updates, update)
		}
		if _, err := v1.NewPermissionsServiceClient(conn).WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
			Updates: updates,
		}); err != nil {
			return fmt.Errorf("write bootstrap relationships: %w", err)
		}
	}

	return nil
}

// randomKey generates a random hex-encoded preshared key.
func randomKey() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
