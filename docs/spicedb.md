## Reference: `spicedb`

A database that stores and computes permissions

### Examples

```
	No TLS and in-memory datastore:
		spicedb serve --grpc-preshared-key "somerandomkeyhere"

	TLS and HTTP enabled, and a real datastore:
		spicedb serve --grpc-preshared-key "realkeyhere" \
		--grpc-tls-cert-path path/to/tls/cert --grpc-tls-key-path path/to/tls/key \
		--http-enabled http-tls-cert-path path/to/tls/cert --http-tls-key-path path/to/tls/key \
		--datastore-engine postgres \
		--datastore-conn-uri "postgres-connection-string-here"

```

### Options

```
      --log-format string    format of logs ("auto", "console", "json") (default "auto")
      --log-level string     verbosity of logging ("trace", "debug", "info", "warn", "error") (default "info")
      --skip-release-check   if true, skips checking for new SpiceDB releases
```

### Children commands

- [spicedb datastore](#reference-spicedb-datastore)	 - datastore operations
- [spicedb lsp](#reference-spicedb-lsp)	 - serve language server protocol
- [spicedb man](#reference-spicedb-man)	 - Generate man page
- [spicedb serve](#reference-spicedb-serve)	 - serve the permissions database
- [spicedb serve-testing](#reference-spicedb-serve-testing)	 - test server with an in-memory datastore
- [spicedb version](#reference-spicedb-version)	 - displays the version of SpiceDB


## Reference: `spicedb datastore`

Operations against the configured datastore

### Options Inherited From Parent Flags

```
      --log-format string    format of logs ("auto", "console", "json") (default "auto")
      --log-level string     verbosity of logging ("trace", "debug", "info", "warn", "error") (default "info")
      --skip-release-check   if true, skips checking for new SpiceDB releases
```

### Children commands

- [spicedb datastore gc](#reference-spicedb-datastore-gc)	 - executes garbage collection
- [spicedb datastore head](#reference-spicedb-datastore-head)	 - compute the head (latest) database migration revision available
- [spicedb datastore migrate](#reference-spicedb-datastore-migrate)	 - execute datastore schema migrations
- [spicedb datastore repair](#reference-spicedb-datastore-repair)	 - executes datastore repair


## Reference: `spicedb datastore gc`

Executes garbage collection against the datastore. Deletes stale relationships, expired relationships, and stale transactions.

```
spicedb datastore gc [flags]
```

### Options

```
      --datastore-allowed-migrations stringArray                              migration levels that will not fail the health check (in addition to the current head migration)
      --datastore-bootstrap-files strings                                     bootstrap data yaml files to load
      --datastore-bootstrap-overwrite                                         overwrite any existing data with bootstrap data (this can be quite slow)
      --datastore-bootstrap-timeout duration                                  maximum duration before timeout for the bootstrap data to be written (default 10s)
      --datastore-conn-max-lifetime-jitter duration                           waits rand(0, jitter) after a connection is open for max lifetime to actually close the connection (default: 20% of max lifetime, 30m for CockroachDB)
      --datastore-conn-pool-read-healthcheck-interval duration                amount of time between connection health checks in a remote datastore's connection pool (default 30s)
      --datastore-conn-pool-read-max-idletime duration                        maximum amount of time a connection can idle in a remote datastore's connection pool (default 30m0s)
      --datastore-conn-pool-read-max-lifetime duration                        maximum amount of time a connection can live in a remote datastore's connection pool (default 30m0s)
      --datastore-conn-pool-read-max-lifetime-jitter duration                 waits rand(0, jitter) after a connection is open for max lifetime to actually close the connection (default: 20% of max lifetime, 30m for CockroachDB)
      --datastore-conn-pool-read-max-open int                                 number of concurrent connections open in a remote datastore's connection pool (default 20)
      --datastore-conn-pool-read-min-open int                                 number of minimum concurrent connections open in a remote datastore's connection pool (default 20)
      --datastore-conn-pool-write-healthcheck-interval duration               amount of time between connection health checks in a remote datastore's connection pool (default 30s)
      --datastore-conn-pool-write-max-idletime duration                       maximum amount of time a connection can idle in a remote datastore's connection pool (default 30m0s)
      --datastore-conn-pool-write-max-lifetime duration                       maximum amount of time a connection can live in a remote datastore's connection pool (default 30m0s)
      --datastore-conn-pool-write-max-lifetime-jitter duration                waits rand(0, jitter) after a connection is open for max lifetime to actually close the connection (default: 20% of max lifetime, 30m for CockroachDB)
      --datastore-conn-pool-write-max-open int                                number of concurrent connections open in a remote datastore's connection pool (default 10)
      --datastore-conn-pool-write-min-open int                                number of minimum concurrent connections open in a remote datastore's connection pool (default 10)
      --datastore-conn-uri string                                             connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")
      --datastore-connect-rate duration                                       rate at which new connections are allowed to the datastore (at a rate of 1/duration) (cockroach driver only) (default 100ms)
      --datastore-connection-balancing                                        enable connection balancing between database nodes (cockroach driver only) (default true)
      --datastore-credentials-provider-name string                            retrieve datastore credentials dynamically using ("aws-iam")
      --datastore-disable-watch-support                                       disable watch support (only enable if you absolutely do not need watch)
      --datastore-engine string                                               type of datastore to initialize ("cockroachdb", "mysql", "postgres", "spanner") (default "memory")
      --datastore-experimental-column-optimization                            enable experimental column optimization (default true)
      --datastore-follower-read-delay-duration duration                       amount of time to subtract from non-sync revision timestamps to ensure they are sufficiently in the past to enable follower reads (cockroach and spanner drivers only) or read replicas (postgres and mysql drivers only) (default 4.8s)
      --datastore-gc-interval duration                                        amount of time between passes of garbage collection (postgres driver only) (default 3m0s)
      --datastore-gc-max-operation-time duration                              maximum amount of time a garbage collection pass can operate before timing out (postgres driver only) (default 1m0s)
      --datastore-gc-window duration                                          amount of time before revisions are garbage collected (default 24h0m0s)
      --datastore-include-query-parameters-in-traces                          include query parameters in traces (postgres and CRDB drivers only)
      --datastore-max-tx-retries int                                          number of times a retriable transaction should be retried (default 10)
      --datastore-migration-phase string                                      datastore-specific flag that should be used to signal to a datastore which phase of a multi-step migration it is in
      --datastore-mysql-table-prefix string                                   prefix to add to the name of all SpiceDB database tables
      --datastore-prometheus-metrics                                          set to false to disabled metrics from the datastore (do not use for Spanner; setting to false will disable metrics to the configured metrics store in Spanner) (default true)
      --datastore-read-replica-conn-pool-read-healthcheck-interval duration   amount of time between connection health checks in a remote datastore's connection pool (default 30s)
      --datastore-read-replica-conn-pool-read-max-idletime duration           maximum amount of time a connection can idle in a remote datastore's connection pool (default 30m0s)
      --datastore-read-replica-conn-pool-read-max-lifetime duration           maximum amount of time a connection can live in a remote datastore's connection pool (default 30m0s)
      --datastore-read-replica-conn-pool-read-max-lifetime-jitter duration    waits rand(0, jitter) after a connection is open for max lifetime to actually close the connection (default: 20% of max lifetime, 30m for CockroachDB)
      --datastore-read-replica-conn-pool-read-max-open int                    number of concurrent connections open in a remote datastore's connection pool (default 20)
      --datastore-read-replica-conn-pool-read-min-open int                    number of minimum concurrent connections open in a remote datastore's connection pool (default 20)
      --datastore-read-replica-conn-uri stringArray                           connection string used by remote datastores for read replicas (e.g. "postgres://postgres:password@localhost:5432/spicedb"). Only supported for postgres and mysql.
      --datastore-read-replica-credentials-provider-name string               retrieve datastore credentials dynamically using ("aws-iam")
      --datastore-readonly                                                    set the service to read-only mode
      --datastore-relationship-integrity-current-key-filename string          current key filename for relationship integrity checks
      --datastore-relationship-integrity-current-key-id string                current key id for relationship integrity checks
      --datastore-relationship-integrity-enabled                              enables relationship integrity checks. only supported on CRDB
      --datastore-relationship-integrity-expired-keys stringArray             config for expired keys for relationship integrity checks
      --datastore-request-hedging                                             enable request hedging
      --datastore-request-hedging-initial-slow-value duration                 initial value to use for slow datastore requests, before statistics have been collected (default 10ms)
      --datastore-request-hedging-max-requests uint                           maximum number of historical requests to consider (default 1000000)
      --datastore-request-hedging-quantile float                              quantile of historical datastore request time over which a request will be considered slow (default 0.95)
      --datastore-revision-quantization-interval duration                     boundary interval to which to round the quantized revision (default 5s)
      --datastore-revision-quantization-max-staleness-percent float           float percentage (where 1 = 100%) of the revision quantization interval where we may opt to select a stale revision for performance reasons. Defaults to 0.1 (representing 10%) (default 0.1)
      --datastore-spanner-credentials string                                  path to service account key credentials file with access to the cloud spanner instance (omit to use application default credentials)
      --datastore-spanner-emulator-host string                                URI of spanner emulator instance used for development and testing (e.g. localhost:9010)
      --datastore-spanner-max-sessions uint                                   maximum number of sessions across all Spanner gRPC connections the client can have at a given time (default 400)
      --datastore-spanner-metrics string                                      configure the metrics that are emitted by the Spanner datastore ("none", "native", "otel", "deprecated-prometheus") (default "otel")
      --datastore-spanner-min-sessions uint                                   minimum number of sessions across all Spanner gRPC connections the client can have at a given time (default 100)
      --datastore-tx-overlap-key string                                       static key to touch when writing to ensure transactions overlap (only used if --datastore-tx-overlap-strategy=static is set; cockroach driver only) (default "key")
      --datastore-tx-overlap-strategy string                                  strategy to generate transaction overlap keys ("request", "prefix", "static", "insecure") (cockroach driver only - see https://spicedb.dev/d/crdb-overlap for details) (default "static")
      --datastore-watch-buffer-length uint16                                  how large the watch buffer should be before blocking (default 1024)
      --datastore-watch-buffer-write-timeout duration                         how long the watch buffer should queue before forcefully disconnecting the reader (default 1s)
      --datastore-watch-connect-timeout duration                              how long the watch connection should wait before timing out (cockroachdb driver only) (default 1s)
      --otel-endpoint string                                                  OpenTelemetry collector endpoint - the endpoint can also be set by using enviroment variables
      --otel-insecure                                                         connect to the OpenTelemetry collector in plaintext
      --otel-provider string                                                  OpenTelemetry provider for tracing ("none", "otlphttp", "otlpgrpc") (default "none")
      --otel-sample-ratio float                                               ratio of traces that are sampled (default 0.01)
      --otel-service-name string                                              service name for trace data (default "spicedb")
      --otel-trace-propagator string                                          OpenTelemetry trace propagation format ("b3", "w3c", "ottrace"). Add multiple propagators separated by comma. (default "w3c")
      --pprof-block-profile-rate int                                          sets the block profile sampling rate (between 0 and 1)
      --pprof-mutex-profile-rate int                                          sets the mutex profile sampling rate (between 0 and 1)
      --termination-log-path string                                           local path to the termination log file, which contains a JSON payload to surface as reason for termination
      --write-conn-acquisition-timeout duration                               amount of time to wait for a connection to become available, otherwise causes resource exhausted errors (0 means wait indefinitely) (default 30ms)
```

### Options Inherited From Parent Flags

```
      --log-format string    format of logs ("auto", "console", "json") (default "auto")
      --log-level string     verbosity of logging ("trace", "debug", "info", "warn", "error") (default "info")
      --skip-release-check   if true, skips checking for new SpiceDB releases
```



## Reference: `spicedb datastore head`

compute the head (latest) database migration revision available

```
spicedb datastore head [flags]
```

### Options

```
      --datastore-engine string        type of datastore to initialize ("cockroachdb", "mysql", "postgres", "spanner") (default "postgres")
      --otel-endpoint string           OpenTelemetry collector endpoint - the endpoint can also be set by using enviroment variables
      --otel-insecure                  connect to the OpenTelemetry collector in plaintext
      --otel-provider string           OpenTelemetry provider for tracing ("none", "otlphttp", "otlpgrpc") (default "none")
      --otel-sample-ratio float        ratio of traces that are sampled (default 0.01)
      --otel-service-name string       service name for trace data (default "spicedb")
      --otel-trace-propagator string   OpenTelemetry trace propagation format ("b3", "w3c", "ottrace"). Add multiple propagators separated by comma. (default "w3c")
      --pprof-block-profile-rate int   sets the block profile sampling rate (between 0 and 1)
      --pprof-mutex-profile-rate int   sets the mutex profile sampling rate (between 0 and 1)
      --termination-log-path string    local path to the termination log file, which contains a JSON payload to surface as reason for termination
```

### Options Inherited From Parent Flags

```
      --log-format string    format of logs ("auto", "console", "json") (default "auto")
      --log-level string     verbosity of logging ("trace", "debug", "info", "warn", "error") (default "info")
      --skip-release-check   if true, skips checking for new SpiceDB releases
```



## Reference: `spicedb datastore migrate`

Executes datastore schema migrations for the datastore.
The special value "head" can be used to migrate to the latest revision.

```
spicedb datastore migrate [revision] [flags]
```

### Options

```
      --datastore-conn-uri string                    connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")
      --datastore-credentials-provider-name string   retrieve datastore credentials dynamically using ("aws-iam")
      --datastore-engine string                      type of datastore to initialize ("cockroachdb", "mysql", "postgres", "spanner") (default "memory")
      --datastore-mysql-table-prefix string          prefix to add to the name of all mysql database tables
      --datastore-spanner-credentials string         path to service account key credentials file with access to the cloud spanner instance (omit to use application default credentials)
      --datastore-spanner-emulator-host string       URI of spanner emulator instance used for development and testing (e.g. localhost:9010)
      --migration-backfill-batch-size uint           number of items to migrate per iteration of a datastore backfill (default 1000)
      --migration-timeout duration                   defines a timeout for the execution of the migration, set to 1 hour by default (default 1h0m0s)
      --otel-endpoint string                         OpenTelemetry collector endpoint - the endpoint can also be set by using enviroment variables
      --otel-insecure                                connect to the OpenTelemetry collector in plaintext
      --otel-provider string                         OpenTelemetry provider for tracing ("none", "otlphttp", "otlpgrpc") (default "none")
      --otel-sample-ratio float                      ratio of traces that are sampled (default 0.01)
      --otel-service-name string                     service name for trace data (default "spicedb")
      --otel-trace-propagator string                 OpenTelemetry trace propagation format ("b3", "w3c", "ottrace"). Add multiple propagators separated by comma. (default "w3c")
      --pprof-block-profile-rate int                 sets the block profile sampling rate (between 0 and 1)
      --pprof-mutex-profile-rate int                 sets the mutex profile sampling rate (between 0 and 1)
      --termination-log-path string                  local path to the termination log file, which contains a JSON payload to surface as reason for termination
```

### Options Inherited From Parent Flags

```
      --log-format string    format of logs ("auto", "console", "json") (default "auto")
      --log-level string     verbosity of logging ("trace", "debug", "info", "warn", "error") (default "info")
      --skip-release-check   if true, skips checking for new SpiceDB releases
```



## Reference: `spicedb datastore repair`

Executes a repair operation for the datastore

```
spicedb datastore repair [flags]
```

### Options

```
      --datastore-allowed-migrations stringArray                              migration levels that will not fail the health check (in addition to the current head migration)
      --datastore-bootstrap-files strings                                     bootstrap data yaml files to load
      --datastore-bootstrap-overwrite                                         overwrite any existing data with bootstrap data (this can be quite slow)
      --datastore-bootstrap-timeout duration                                  maximum duration before timeout for the bootstrap data to be written (default 10s)
      --datastore-conn-max-lifetime-jitter duration                           waits rand(0, jitter) after a connection is open for max lifetime to actually close the connection (default: 20% of max lifetime, 30m for CockroachDB)
      --datastore-conn-pool-read-healthcheck-interval duration                amount of time between connection health checks in a remote datastore's connection pool (default 30s)
      --datastore-conn-pool-read-max-idletime duration                        maximum amount of time a connection can idle in a remote datastore's connection pool (default 30m0s)
      --datastore-conn-pool-read-max-lifetime duration                        maximum amount of time a connection can live in a remote datastore's connection pool (default 30m0s)
      --datastore-conn-pool-read-max-lifetime-jitter duration                 waits rand(0, jitter) after a connection is open for max lifetime to actually close the connection (default: 20% of max lifetime, 30m for CockroachDB)
      --datastore-conn-pool-read-max-open int                                 number of concurrent connections open in a remote datastore's connection pool (default 20)
      --datastore-conn-pool-read-min-open int                                 number of minimum concurrent connections open in a remote datastore's connection pool (default 20)
      --datastore-conn-pool-write-healthcheck-interval duration               amount of time between connection health checks in a remote datastore's connection pool (default 30s)
      --datastore-conn-pool-write-max-idletime duration                       maximum amount of time a connection can idle in a remote datastore's connection pool (default 30m0s)
      --datastore-conn-pool-write-max-lifetime duration                       maximum amount of time a connection can live in a remote datastore's connection pool (default 30m0s)
      --datastore-conn-pool-write-max-lifetime-jitter duration                waits rand(0, jitter) after a connection is open for max lifetime to actually close the connection (default: 20% of max lifetime, 30m for CockroachDB)
      --datastore-conn-pool-write-max-open int                                number of concurrent connections open in a remote datastore's connection pool (default 10)
      --datastore-conn-pool-write-min-open int                                number of minimum concurrent connections open in a remote datastore's connection pool (default 10)
      --datastore-conn-uri string                                             connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")
      --datastore-connect-rate duration                                       rate at which new connections are allowed to the datastore (at a rate of 1/duration) (cockroach driver only) (default 100ms)
      --datastore-connection-balancing                                        enable connection balancing between database nodes (cockroach driver only) (default true)
      --datastore-credentials-provider-name string                            retrieve datastore credentials dynamically using ("aws-iam")
      --datastore-disable-watch-support                                       disable watch support (only enable if you absolutely do not need watch)
      --datastore-engine string                                               type of datastore to initialize ("cockroachdb", "mysql", "postgres", "spanner") (default "memory")
      --datastore-experimental-column-optimization                            enable experimental column optimization (default true)
      --datastore-follower-read-delay-duration duration                       amount of time to subtract from non-sync revision timestamps to ensure they are sufficiently in the past to enable follower reads (cockroach and spanner drivers only) or read replicas (postgres and mysql drivers only) (default 4.8s)
      --datastore-gc-interval duration                                        amount of time between passes of garbage collection (postgres driver only) (default 3m0s)
      --datastore-gc-max-operation-time duration                              maximum amount of time a garbage collection pass can operate before timing out (postgres driver only) (default 1m0s)
      --datastore-gc-window duration                                          amount of time before revisions are garbage collected (default 24h0m0s)
      --datastore-include-query-parameters-in-traces                          include query parameters in traces (postgres and CRDB drivers only)
      --datastore-max-tx-retries int                                          number of times a retriable transaction should be retried (default 10)
      --datastore-migration-phase string                                      datastore-specific flag that should be used to signal to a datastore which phase of a multi-step migration it is in
      --datastore-mysql-table-prefix string                                   prefix to add to the name of all SpiceDB database tables
      --datastore-prometheus-metrics                                          set to false to disabled metrics from the datastore (do not use for Spanner; setting to false will disable metrics to the configured metrics store in Spanner) (default true)
      --datastore-read-replica-conn-pool-read-healthcheck-interval duration   amount of time between connection health checks in a remote datastore's connection pool (default 30s)
      --datastore-read-replica-conn-pool-read-max-idletime duration           maximum amount of time a connection can idle in a remote datastore's connection pool (default 30m0s)
      --datastore-read-replica-conn-pool-read-max-lifetime duration           maximum amount of time a connection can live in a remote datastore's connection pool (default 30m0s)
      --datastore-read-replica-conn-pool-read-max-lifetime-jitter duration    waits rand(0, jitter) after a connection is open for max lifetime to actually close the connection (default: 20% of max lifetime, 30m for CockroachDB)
      --datastore-read-replica-conn-pool-read-max-open int                    number of concurrent connections open in a remote datastore's connection pool (default 20)
      --datastore-read-replica-conn-pool-read-min-open int                    number of minimum concurrent connections open in a remote datastore's connection pool (default 20)
      --datastore-read-replica-conn-uri stringArray                           connection string used by remote datastores for read replicas (e.g. "postgres://postgres:password@localhost:5432/spicedb"). Only supported for postgres and mysql.
      --datastore-read-replica-credentials-provider-name string               retrieve datastore credentials dynamically using ("aws-iam")
      --datastore-readonly                                                    set the service to read-only mode
      --datastore-relationship-integrity-current-key-filename string          current key filename for relationship integrity checks
      --datastore-relationship-integrity-current-key-id string                current key id for relationship integrity checks
      --datastore-relationship-integrity-enabled                              enables relationship integrity checks. only supported on CRDB
      --datastore-relationship-integrity-expired-keys stringArray             config for expired keys for relationship integrity checks
      --datastore-request-hedging                                             enable request hedging
      --datastore-request-hedging-initial-slow-value duration                 initial value to use for slow datastore requests, before statistics have been collected (default 10ms)
      --datastore-request-hedging-max-requests uint                           maximum number of historical requests to consider (default 1000000)
      --datastore-request-hedging-quantile float                              quantile of historical datastore request time over which a request will be considered slow (default 0.95)
      --datastore-revision-quantization-interval duration                     boundary interval to which to round the quantized revision (default 5s)
      --datastore-revision-quantization-max-staleness-percent float           float percentage (where 1 = 100%) of the revision quantization interval where we may opt to select a stale revision for performance reasons. Defaults to 0.1 (representing 10%) (default 0.1)
      --datastore-spanner-credentials string                                  path to service account key credentials file with access to the cloud spanner instance (omit to use application default credentials)
      --datastore-spanner-emulator-host string                                URI of spanner emulator instance used for development and testing (e.g. localhost:9010)
      --datastore-spanner-max-sessions uint                                   maximum number of sessions across all Spanner gRPC connections the client can have at a given time (default 400)
      --datastore-spanner-metrics string                                      configure the metrics that are emitted by the Spanner datastore ("none", "native", "otel", "deprecated-prometheus") (default "otel")
      --datastore-spanner-min-sessions uint                                   minimum number of sessions across all Spanner gRPC connections the client can have at a given time (default 100)
      --datastore-tx-overlap-key string                                       static key to touch when writing to ensure transactions overlap (only used if --datastore-tx-overlap-strategy=static is set; cockroach driver only) (default "key")
      --datastore-tx-overlap-strategy string                                  strategy to generate transaction overlap keys ("request", "prefix", "static", "insecure") (cockroach driver only - see https://spicedb.dev/d/crdb-overlap for details) (default "static")
      --datastore-watch-buffer-length uint16                                  how large the watch buffer should be before blocking (default 1024)
      --datastore-watch-buffer-write-timeout duration                         how long the watch buffer should queue before forcefully disconnecting the reader (default 1s)
      --datastore-watch-connect-timeout duration                              how long the watch connection should wait before timing out (cockroachdb driver only) (default 1s)
      --otel-endpoint string                                                  OpenTelemetry collector endpoint - the endpoint can also be set by using enviroment variables
      --otel-insecure                                                         connect to the OpenTelemetry collector in plaintext
      --otel-provider string                                                  OpenTelemetry provider for tracing ("none", "otlphttp", "otlpgrpc") (default "none")
      --otel-sample-ratio float                                               ratio of traces that are sampled (default 0.01)
      --otel-service-name string                                              service name for trace data (default "spicedb")
      --otel-trace-propagator string                                          OpenTelemetry trace propagation format ("b3", "w3c", "ottrace"). Add multiple propagators separated by comma. (default "w3c")
      --pprof-block-profile-rate int                                          sets the block profile sampling rate (between 0 and 1)
      --pprof-mutex-profile-rate int                                          sets the mutex profile sampling rate (between 0 and 1)
      --termination-log-path string                                           local path to the termination log file, which contains a JSON payload to surface as reason for termination
      --write-conn-acquisition-timeout duration                               amount of time to wait for a connection to become available, otherwise causes resource exhausted errors (0 means wait indefinitely) (default 30ms)
```

### Options Inherited From Parent Flags

```
      --log-format string    format of logs ("auto", "console", "json") (default "auto")
      --log-level string     verbosity of logging ("trace", "debug", "info", "warn", "error") (default "info")
      --skip-release-check   if true, skips checking for new SpiceDB releases
```



## Reference: `spicedb lsp`

serve language server protocol

```
spicedb lsp [flags]
```

### Options

```
      --addr string   address to listen on to serve LSP (default "-")
      --stdio         enable stdio mode for LSP (default true)
```

### Options Inherited From Parent Flags

```
      --log-format string    format of logs ("auto", "console", "json") (default "auto")
      --log-level string     verbosity of logging ("trace", "debug", "info", "warn", "error") (default "info")
      --skip-release-check   if true, skips checking for new SpiceDB releases
```



## Reference: `spicedb man`

Generate a man page for SpiceDB.
 The output can be redirected to a file and installed to the system:

```
  spicedb man > spicedb.1
  sudo mv spicedb.1 /usr/share/man/man1/
  sudo mandb  # Update man page database
```


```
spicedb man
```

### Options Inherited From Parent Flags

```
      --log-format string    format of logs ("auto", "console", "json") (default "auto")
      --log-level string     verbosity of logging ("trace", "debug", "info", "warn", "error") (default "info")
      --skip-release-check   if true, skips checking for new SpiceDB releases
```



## Reference: `spicedb serve`

start a SpiceDB server

```
spicedb serve [flags]
```

### Examples

```
	No TLS and in-memory datastore:
		spicedb serve --grpc-preshared-key "somerandomkeyhere"

	TLS and HTTP enabled, and a real datastore:
		spicedb serve --grpc-preshared-key "realkeyhere" \
		--grpc-tls-cert-path path/to/tls/cert --grpc-tls-key-path path/to/tls/key \
		--http-enabled http-tls-cert-path path/to/tls/cert --http-tls-key-path path/to/tls/key \
		--datastore-engine postgres \
		--datastore-conn-uri "postgres-connection-string-here"

```

### Options

```
      --datastore-allowed-migrations stringArray                                        migration levels that will not fail the health check (in addition to the current head migration)
      --datastore-bootstrap-files strings                                               bootstrap data yaml files to load
      --datastore-bootstrap-overwrite                                                   overwrite any existing data with bootstrap data (this can be quite slow)
      --datastore-bootstrap-timeout duration                                            maximum duration before timeout for the bootstrap data to be written (default 10s)
      --datastore-conn-max-lifetime-jitter duration                                     waits rand(0, jitter) after a connection is open for max lifetime to actually close the connection (default: 20% of max lifetime, 30m for CockroachDB)
      --datastore-conn-pool-read-healthcheck-interval duration                          amount of time between connection health checks in a remote datastore's connection pool (default 30s)
      --datastore-conn-pool-read-max-idletime duration                                  maximum amount of time a connection can idle in a remote datastore's connection pool (default 30m0s)
      --datastore-conn-pool-read-max-lifetime duration                                  maximum amount of time a connection can live in a remote datastore's connection pool (default 30m0s)
      --datastore-conn-pool-read-max-lifetime-jitter duration                           waits rand(0, jitter) after a connection is open for max lifetime to actually close the connection (default: 20% of max lifetime, 30m for CockroachDB)
      --datastore-conn-pool-read-max-open int                                           number of concurrent connections open in a remote datastore's connection pool (default 20)
      --datastore-conn-pool-read-min-open int                                           number of minimum concurrent connections open in a remote datastore's connection pool (default 20)
      --datastore-conn-pool-write-healthcheck-interval duration                         amount of time between connection health checks in a remote datastore's connection pool (default 30s)
      --datastore-conn-pool-write-max-idletime duration                                 maximum amount of time a connection can idle in a remote datastore's connection pool (default 30m0s)
      --datastore-conn-pool-write-max-lifetime duration                                 maximum amount of time a connection can live in a remote datastore's connection pool (default 30m0s)
      --datastore-conn-pool-write-max-lifetime-jitter duration                          waits rand(0, jitter) after a connection is open for max lifetime to actually close the connection (default: 20% of max lifetime, 30m for CockroachDB)
      --datastore-conn-pool-write-max-open int                                          number of concurrent connections open in a remote datastore's connection pool (default 10)
      --datastore-conn-pool-write-min-open int                                          number of minimum concurrent connections open in a remote datastore's connection pool (default 10)
      --datastore-conn-uri string                                                       connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")
      --datastore-connect-rate duration                                                 rate at which new connections are allowed to the datastore (at a rate of 1/duration) (cockroach driver only) (default 100ms)
      --datastore-connection-balancing                                                  enable connection balancing between database nodes (cockroach driver only) (default true)
      --datastore-credentials-provider-name string                                      retrieve datastore credentials dynamically using ("aws-iam")
      --datastore-disable-watch-support                                                 disable watch support (only enable if you absolutely do not need watch)
      --datastore-engine string                                                         type of datastore to initialize ("cockroachdb", "mysql", "postgres", "spanner") (default "memory")
      --datastore-experimental-column-optimization                                      enable experimental column optimization (default true)
      --datastore-follower-read-delay-duration duration                                 amount of time to subtract from non-sync revision timestamps to ensure they are sufficiently in the past to enable follower reads (cockroach and spanner drivers only) or read replicas (postgres and mysql drivers only) (default 4.8s)
      --datastore-gc-interval duration                                                  amount of time between passes of garbage collection (postgres driver only) (default 3m0s)
      --datastore-gc-max-operation-time duration                                        maximum amount of time a garbage collection pass can operate before timing out (postgres driver only) (default 1m0s)
      --datastore-gc-window duration                                                    amount of time before revisions are garbage collected (default 24h0m0s)
      --datastore-include-query-parameters-in-traces                                    include query parameters in traces (postgres and CRDB drivers only)
      --datastore-max-tx-retries int                                                    number of times a retriable transaction should be retried (default 10)
      --datastore-migration-phase string                                                datastore-specific flag that should be used to signal to a datastore which phase of a multi-step migration it is in
      --datastore-mysql-table-prefix string                                             prefix to add to the name of all SpiceDB database tables
      --datastore-prometheus-metrics                                                    set to false to disabled metrics from the datastore (do not use for Spanner; setting to false will disable metrics to the configured metrics store in Spanner) (default true)
      --datastore-read-replica-conn-pool-read-healthcheck-interval duration             amount of time between connection health checks in a remote datastore's connection pool (default 30s)
      --datastore-read-replica-conn-pool-read-max-idletime duration                     maximum amount of time a connection can idle in a remote datastore's connection pool (default 30m0s)
      --datastore-read-replica-conn-pool-read-max-lifetime duration                     maximum amount of time a connection can live in a remote datastore's connection pool (default 30m0s)
      --datastore-read-replica-conn-pool-read-max-lifetime-jitter duration              waits rand(0, jitter) after a connection is open for max lifetime to actually close the connection (default: 20% of max lifetime, 30m for CockroachDB)
      --datastore-read-replica-conn-pool-read-max-open int                              number of concurrent connections open in a remote datastore's connection pool (default 20)
      --datastore-read-replica-conn-pool-read-min-open int                              number of minimum concurrent connections open in a remote datastore's connection pool (default 20)
      --datastore-read-replica-conn-uri stringArray                                     connection string used by remote datastores for read replicas (e.g. "postgres://postgres:password@localhost:5432/spicedb"). Only supported for postgres and mysql.
      --datastore-read-replica-credentials-provider-name string                         retrieve datastore credentials dynamically using ("aws-iam")
      --datastore-readonly                                                              set the service to read-only mode
      --datastore-relationship-integrity-current-key-filename string                    current key filename for relationship integrity checks
      --datastore-relationship-integrity-current-key-id string                          current key id for relationship integrity checks
      --datastore-relationship-integrity-enabled                                        enables relationship integrity checks. only supported on CRDB
      --datastore-relationship-integrity-expired-keys stringArray                       config for expired keys for relationship integrity checks
      --datastore-request-hedging                                                       enable request hedging
      --datastore-request-hedging-initial-slow-value duration                           initial value to use for slow datastore requests, before statistics have been collected (default 10ms)
      --datastore-request-hedging-max-requests uint                                     maximum number of historical requests to consider (default 1000000)
      --datastore-request-hedging-quantile float                                        quantile of historical datastore request time over which a request will be considered slow (default 0.95)
      --datastore-revision-quantization-interval duration                               boundary interval to which to round the quantized revision (default 5s)
      --datastore-revision-quantization-max-staleness-percent float                     float percentage (where 1 = 100%) of the revision quantization interval where we may opt to select a stale revision for performance reasons. Defaults to 0.1 (representing 10%) (default 0.1)
      --datastore-schema-watch-heartbeat duration                                       heartbeat time on the schema watch in the datastore (if supported). 0 means to default to the datastore's minimum. (default 1s)
      --datastore-spanner-credentials string                                            path to service account key credentials file with access to the cloud spanner instance (omit to use application default credentials)
      --datastore-spanner-emulator-host string                                          URI of spanner emulator instance used for development and testing (e.g. localhost:9010)
      --datastore-spanner-max-sessions uint                                             maximum number of sessions across all Spanner gRPC connections the client can have at a given time (default 400)
      --datastore-spanner-metrics string                                                configure the metrics that are emitted by the Spanner datastore ("none", "native", "otel", "deprecated-prometheus") (default "otel")
      --datastore-spanner-min-sessions uint                                             minimum number of sessions across all Spanner gRPC connections the client can have at a given time (default 100)
      --datastore-tx-overlap-key string                                                 static key to touch when writing to ensure transactions overlap (only used if --datastore-tx-overlap-strategy=static is set; cockroach driver only) (default "key")
      --datastore-tx-overlap-strategy string                                            strategy to generate transaction overlap keys ("request", "prefix", "static", "insecure") (cockroach driver only - see https://spicedb.dev/d/crdb-overlap for details) (default "static")
      --datastore-watch-buffer-length uint16                                            how large the watch buffer should be before blocking (default 1024)
      --datastore-watch-buffer-write-timeout duration                                   how long the watch buffer should queue before forcefully disconnecting the reader (default 1s)
      --datastore-watch-connect-timeout duration                                        how long the watch connection should wait before timing out (cockroachdb driver only) (default 1s)
      --disable-version-response                                                        disables version response support in the API
      --dispatch-cache-enabled                                                          enable caching (default true)
      --dispatch-cache-max-cost string                                                  upper bound cache size in bytes or percent of available memory (default "30%")
      --dispatch-cache-metrics                                                          enable cache metrics (default true)
      --dispatch-cache-num-counters int                                                 number of TinyLFU samples to track. A higher number means more accurate eviction decisions but more memory usage (default 10000)
      --dispatch-check-permission-concurrency-limit uint16                              maximum number of parallel goroutines to create for each check request or subrequest. defaults to --dispatch-concurrency-limit
      --dispatch-chunk-size uint16                                                      maximum number of object IDs in a dispatched request (default 100)
      --dispatch-cluster-addr string                                                    address to listen on to serve dispatch (default ":50053")
      --dispatch-cluster-cache-enabled                                                  enable caching (default true)
      --dispatch-cluster-cache-max-cost string                                          upper bound cache size in bytes or percent of available memory (default "70%")
      --dispatch-cluster-cache-metrics                                                  enable cache metrics (default true)
      --dispatch-cluster-cache-num-counters int                                         number of TinyLFU samples to track. A higher number means more accurate eviction decisions but more memory usage (default 100000)
      --dispatch-cluster-enabled                                                        enable dispatch gRPC server
      --dispatch-cluster-max-conn-age duration                                          how long a connection serving dispatch should be able to live (default 30s)
      --dispatch-cluster-max-workers uint32                                             set the number of workers for this server (0 value means 1 worker per request)
      --dispatch-cluster-network string                                                 network type to serve dispatch ("tcp", "tcp4", "tcp6", "unix", "unixpacket") (default "tcp")
      --dispatch-cluster-tls-cert-path string                                           local path to the TLS certificate used to serve dispatch
      --dispatch-cluster-tls-key-path string                                            local path to the TLS key used to serve dispatch
      --dispatch-concurrency-limit uint16                                               maximum number of parallel goroutines to create for each request or subrequest (default 50)
      --dispatch-hashring-replication-factor uint16                                     set the replication factor of the consistent hasher used for the dispatcher (default 100)
      --dispatch-hashring-spread uint8                                                  set the spread of the consistent hasher used for the dispatcher (default 1)
      --dispatch-lookup-resources-concurrency-limit uint16                              maximum number of parallel goroutines to create for each lookup resources request or subrequest. defaults to --dispatch-concurrency-limit
      --dispatch-lookup-subjects-concurrency-limit uint16                               maximum number of parallel goroutines to create for each lookup subjects request or subrequest. defaults to --dispatch-concurrency-limit
      --dispatch-max-depth uint32                                                       maximum recursion depth for nested calls (default 50)
      --dispatch-reachable-resources-concurrency-limit uint16                           maximum number of parallel goroutines to create for each reachable resources request or subrequest. defaults to --dispatch-concurrency-limit
      --dispatch-upstream-addr string                                                   upstream grpc address to dispatch to
      --dispatch-upstream-ca-path string                                                local path to the TLS CA used when connecting to the dispatch cluster
      --dispatch-upstream-timeout duration                                              maximum duration of a dispatch call an upstream cluster before it times out (default 1m0s)
      --enable-experimental-watchable-schema-cache                                      enables the experimental schema cache, which uses the Watch API to keep the schema up to date
      --enable-performance-insight-metrics                                              enables performance insight metrics, which are used to track the latency of API calls by shape
      --enable-revision-heartbeat                                                       enables support for revision heartbeat, used to create a synthetic revision on an interval defined by the quantization window (postgres only) (default true)
      --experimental-dispatch-secondary-maximum-primary-hedging-delays stringToString   maximum number of hedging delays to use for each request type to delay the primary request. default is 5ms (default [])
      --experimental-dispatch-secondary-upstream-addrs stringToString                   secondary upstream addresses for dispatches, each with a name (default [])
      --experimental-dispatch-secondary-upstream-exprs stringToString                   map from request type to its associated CEL expression, which returns the secondary upstream(s) to be used for the request (default [])
      --experimental-lookup-resources-version lr3                                       if non-empty, the version of the experimental lookup resources API to use: lr3 or empty
      --grpc-addr string                                                                address to listen on to serve gRPC (default ":50051")
      --grpc-enabled                                                                    enable gRPC gRPC server (default true)
      --grpc-log-requests-enabled                                                       enable logging of API request payloads
      --grpc-log-responses-enabled                                                      enable logging of API response payloads
      --grpc-max-conn-age duration                                                      how long a connection serving gRPC should be able to live (default 30s)
      --grpc-max-workers uint32                                                         set the number of workers for this server (0 value means 1 worker per request)
      --grpc-network string                                                             network type to serve gRPC ("tcp", "tcp4", "tcp6", "unix", "unixpacket") (default "tcp")
      --grpc-preshared-key strings                                                      (required) preshared key(s) that must be provided by clients to authenticate requests
      --grpc-shutdown-grace-period duration                                             amount of time after receiving sigint to continue serving
      --grpc-tls-cert-path string                                                       local path to the TLS certificate used to serve gRPC
      --grpc-tls-key-path string                                                        local path to the TLS key used to serve gRPC
      --http-addr string                                                                address to listen on to serve proxy (default ":8443")
      --http-enabled                                                                    enable http proxy server
      --http-tls-cert-path string                                                       local path to the TLS certificate used to serve proxy
      --http-tls-key-path string                                                        local path to the TLS key used to serve proxy
      --lookup-resources-chunk-cache-enabled                                            enable caching (default true)
      --lookup-resources-chunk-cache-max-cost string                                    upper bound cache size in bytes or percent of available memory (default "50MiB")
      --lookup-resources-chunk-cache-metrics                                            enable cache metrics
      --lookup-resources-chunk-cache-num-counters int                                   number of TinyLFU samples to track. A higher number means more accurate eviction decisions but more memory usage (default 10000)
      --max-bulk-export-relationships-limit uint32                                      maximum number of relationships that can be exported in a single request (default 10000)
      --max-caveat-context-size int                                                     maximum allowed size of request caveat context in bytes. A value of zero or less means no limit (default 4096)
      --max-datastore-read-page-size uint                                               limit on the maximum page size that we will load into memory from the datastore at one time (default 1000)
      --max-delete-relationships-limit uint32                                           maximum number of relationships that can be deleted in a single request (default 1000)
      --max-lookup-resources-limit uint32                                               maximum number of resources that can be looked up in a single request (default 1000)
      --max-read-relationships-limit uint32                                             maximum number of relationships that can be read in a single request (default 1000)
      --max-relationship-context-size int                                               maximum allowed size of the context to be stored in a relationship (default 25000)
      --metrics-addr string                                                             address to listen on to serve metrics (default ":9090")
      --metrics-enabled                                                                 enable http metrics server (default true)
      --metrics-tls-cert-path string                                                    local path to the TLS certificate used to serve metrics
      --metrics-tls-key-path string                                                     local path to the TLS key used to serve metrics
      --mismatch-zed-token-behavior string                                              behavior to enforce when an API call receives a zedtoken that was originally intended for a different kind of datastore. One of: full-consistency (treat as a full-consistency call, ignoring the zedtoken), min-latency (treat as a min-latency call, ignoring the zedtoken), error (return an error). defaults to full-consistency for safety. (default "full-consistency")
      --ns-cache-enabled                                                                enable caching (default true)
      --ns-cache-max-cost string                                                        upper bound cache size in bytes or percent of available memory (default "32MiB")
      --ns-cache-metrics                                                                enable cache metrics (default true)
      --ns-cache-num-counters int                                                       number of TinyLFU samples to track. A higher number means more accurate eviction decisions but more memory usage (default 1000)
      --otel-endpoint string                                                            OpenTelemetry collector endpoint - the endpoint can also be set by using enviroment variables
      --otel-insecure                                                                   connect to the OpenTelemetry collector in plaintext
      --otel-provider string                                                            OpenTelemetry provider for tracing ("none", "otlphttp", "otlpgrpc") (default "none")
      --otel-sample-ratio float                                                         ratio of traces that are sampled (default 0.01)
      --otel-service-name string                                                        service name for trace data (default "spicedb")
      --otel-trace-propagator string                                                    OpenTelemetry trace propagation format ("b3", "w3c", "ottrace"). Add multiple propagators separated by comma. (default "w3c")
      --pprof-block-profile-rate int                                                    sets the block profile sampling rate (between 0 and 1)
      --pprof-mutex-profile-rate int                                                    sets the mutex profile sampling rate (between 0 and 1)
      --schema-prefixes-required                                                        require prefixes on all object definitions in schemas
      --streaming-api-response-delay-timeout duration                                   maximum time that streaming APIs (LookupSubjects, LookupResources, ReadRelationships and ExportBulkRelationships) can be allowed to run but no response be sent to the client before the stream times out (default 30s)
      --telemetry-ca-override-path string                                               path to a custom CA to use with the telemetry endpoint
      --telemetry-endpoint string                                                       endpoint to which telemetry is reported, empty string to disable (default "https://telemetry.authzed.com")
      --telemetry-interval duration                                                     approximate period between telemetry reports, minimum 1 minute (default 1h0m0s)
      --termination-log-path string                                                     local path to the termination log file, which contains a JSON payload to surface as reason for termination
      --update-relationships-max-preconditions-per-call uint16                          maximum number of preconditions allowed for WriteRelationships and DeleteRelationships calls (default 1000)
      --watch-api-heartbeat duration                                                    heartbeat time on the watch in the API. 0 means to default to the datastore's minimum. (default 1s)
      --write-conn-acquisition-timeout duration                                         amount of time to wait for a connection to become available, otherwise causes resource exhausted errors (0 means wait indefinitely) (default 30ms)
      --write-relationships-max-updates-per-call uint16                                 maximum number of updates allowed for WriteRelationships calls (default 1000)
```

### Options Inherited From Parent Flags

```
      --log-format string    format of logs ("auto", "console", "json") (default "auto")
      --log-level string     verbosity of logging ("trace", "debug", "info", "warn", "error") (default "info")
      --skip-release-check   if true, skips checking for new SpiceDB releases
```



## Reference: `spicedb serve-testing`

An in-memory spicedb server which serves completely isolated datastores per client-supplied auth token used.

```
spicedb serve-testing [flags]
```

### Options

```
      --grpc-addr string                                         address to listen on to serve gRPC (default ":50051")
      --grpc-enabled                                             enable gRPC gRPC server (default true)
      --grpc-max-conn-age duration                               how long a connection serving gRPC should be able to live (default 30s)
      --grpc-max-workers uint32                                  set the number of workers for this server (0 value means 1 worker per request)
      --grpc-network string                                      network type to serve gRPC ("tcp", "tcp4", "tcp6", "unix", "unixpacket") (default "tcp")
      --grpc-tls-cert-path string                                local path to the TLS certificate used to serve gRPC
      --grpc-tls-key-path string                                 local path to the TLS key used to serve gRPC
      --http-addr string                                         address to listen on to serve http (default ":8443")
      --http-enabled                                             enable http http server
      --http-tls-cert-path string                                local path to the TLS certificate used to serve http
      --http-tls-key-path string                                 local path to the TLS key used to serve http
      --load-configs strings                                     configuration yaml files to load
      --max-bulk-export-relationships-limit uint32               maximum number of relationships that can be exported in a single request (default 10000)
      --max-caveat-context-size int                              maximum allowed size of request caveat context in bytes. A value of zero or less means no limit (default 4096)
      --max-delete-relationships-limit uint32                    maximum number of relationships that can be deleted in a single request (default 1000)
      --max-lookup-resources-limit uint32                        maximum number of resources that can be looked up in a single request (default 1000)
      --max-read-relationships-limit uint32                      maximum number of relationships that can be read in a single request (default 1000)
      --max-relationship-context-size int                        maximum allowed size of the context to be stored in a relationship (default 25000)
      --otel-endpoint string                                     OpenTelemetry collector endpoint - the endpoint can also be set by using enviroment variables
      --otel-insecure                                            connect to the OpenTelemetry collector in plaintext
      --otel-provider string                                     OpenTelemetry provider for tracing ("none", "otlphttp", "otlpgrpc") (default "none")
      --otel-sample-ratio float                                  ratio of traces that are sampled (default 0.01)
      --otel-service-name string                                 service name for trace data (default "spicedb")
      --otel-trace-propagator string                             OpenTelemetry trace propagation format ("b3", "w3c", "ottrace"). Add multiple propagators separated by comma. (default "w3c")
      --pprof-block-profile-rate int                             sets the block profile sampling rate (between 0 and 1)
      --pprof-mutex-profile-rate int                             sets the mutex profile sampling rate (between 0 and 1)
      --readonly-grpc-addr string                                address to listen on to serve read-only gRPC (default ":50052")
      --readonly-grpc-enabled                                    enable read-only gRPC gRPC server (default true)
      --readonly-grpc-max-conn-age duration                      how long a connection serving read-only gRPC should be able to live (default 30s)
      --readonly-grpc-max-workers uint32                         set the number of workers for this server (0 value means 1 worker per request)
      --readonly-grpc-network string                             network type to serve read-only gRPC ("tcp", "tcp4", "tcp6", "unix", "unixpacket") (default "tcp")
      --readonly-grpc-tls-cert-path string                       local path to the TLS certificate used to serve read-only gRPC
      --readonly-grpc-tls-key-path string                        local path to the TLS key used to serve read-only gRPC
      --readonly-http-addr string                                address to listen on to serve read-only HTTP (default ":8444")
      --readonly-http-enabled                                    enable http read-only HTTP server
      --readonly-http-tls-cert-path string                       local path to the TLS certificate used to serve read-only HTTP
      --readonly-http-tls-key-path string                        local path to the TLS key used to serve read-only HTTP
      --termination-log-path string                              local path to the termination log file, which contains a JSON payload to surface as reason for termination
      --update-relationships-max-preconditions-per-call uint16   maximum number of preconditions allowed for WriteRelationships and DeleteRelationships calls (default 1000)
      --write-relationships-max-updates-per-call uint16          maximum number of updates allowed for WriteRelationships calls (default 1000)
```

### Options Inherited From Parent Flags

```
      --log-format string    format of logs ("auto", "console", "json") (default "auto")
      --log-level string     verbosity of logging ("trace", "debug", "info", "warn", "error") (default "info")
      --skip-release-check   if true, skips checking for new SpiceDB releases
```



## Reference: `spicedb version`

displays the version of SpiceDB

```
spicedb version [flags]
```

### Options

```
      --include-deps   include dependencies' versions
```

### Options Inherited From Parent Flags

```
      --log-format string    format of logs ("auto", "console", "json") (default "auto")
      --log-level string     verbosity of logging ("trace", "debug", "info", "warn", "error") (default "info")
      --skip-release-check   if true, skips checking for new SpiceDB releases
```



