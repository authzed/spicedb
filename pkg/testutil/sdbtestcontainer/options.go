package sdbtestcontainer

import "github.com/testcontainers/testcontainers-go"

// options holds the SpiceDB-specific configuration assembled from the
// package's functional options before the container is started.
type options struct {
	presharedKeys           []string
	httpEnabled            bool
	datastoreEngine        string
	datastoreConnURI       string
	logLevel               string
	bootstrapSchema        string
	bootstrapRelationships []string
}

// customizer extends testcontainers.ContainerCustomizer with SpiceDB-specific
// configuration. Options that implement it are applied to the options struct
// and filtered out before the remaining customizers touch the container request.
type customizer interface {
	testcontainers.ContainerCustomizer
	apply(*options)
}

// optionFunc adapts a function into a customizer. Its Customize method is a
// no-op; all SpiceDB configuration flows through apply and is materialized into
// the container command in Run.
type optionFunc func(*options)

func (f optionFunc) Customize(_ *testcontainers.GenericContainerRequest) error { return nil }
func (f optionFunc) apply(o *options)                                          { f(o) }

// filterOpts returns only the customizers that are not SpiceDB-specific, so
// they can be applied directly to the container request (e.g. testcontainers.WithEnv).
func filterOpts(opts []testcontainers.ContainerCustomizer) []testcontainers.ContainerCustomizer {
	filtered := make([]testcontainers.ContainerCustomizer, 0, len(opts))
	for _, opt := range opts {
		if _, ok := opt.(customizer); !ok {
			filtered = append(filtered, opt)
		}
	}
	return filtered
}

// WithPresharedKeys overrides the gRPC preshared key. By default Run generates a
// random key, retrievable via Container.PresharedKey.
func WithPresharedKeys(keys ...string) testcontainers.ContainerCustomizer {
	return optionFunc(func(o *options) { o.presharedKeys = keys })
}

// WithHTTP enables the SpiceDB HTTP gateway. The HTTP port is exposed and
// mapped, and Container.HTTPEndpoint returns the reachable host:port.
func WithHTTP() testcontainers.ContainerCustomizer {
	return optionFunc(func(o *options) { o.httpEnabled = true })
}

// WithDatastore overrides the datastore engine and connection URI. By default
// SpiceDB runswith the in-memory datastore and no connection URI is passed.
func WithDatastore(engine, connURI string) testcontainers.ContainerCustomizer {
	return optionFunc(func(o *options) {
		o.datastoreEngine = engine
		o.datastoreConnURI = connURI
	})
}

// WithLogLevel sets the SpiceDB --log-level flag (e.g. "debug", "info", "warn").
func WithLogLevel(level string) testcontainers.ContainerCustomizer {
	return optionFunc(func(o *options) { o.logLevel = level })
}

// WithBootstrapSchema writes the given schema to the server once it is ready,
// over the gRPC API using the configured preshared key.
func WithBootstrapSchema(schema string) testcontainers.ContainerCustomizer {
	return optionFunc(func(o *options) { o.bootstrapSchema = schema })
}

// WithBootstrapRelationships writes the given relationships (in SpiceDB
// relationship string form, e.g. "resource:foo#reader@user:bar") once the
// server is ready. Each is written as a TOUCH. Requires a schema that defines
// the referenced relations, typically supplied via WithBootstrapSchema.
func WithBootstrapRelationships(relationships ...string) testcontainers.ContainerCustomizer {
	return optionFunc(func(o *options) {
		o.bootstrapRelationships = append(o.bootstrapRelationships, relationships...)
	})
}
