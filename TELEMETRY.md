# SpiceDB Telemetry

SpiceDB reports metrics that are used to understand how clusters are being configured and the performance they are experiencing.
The intent of collecting this information is to prioritize development that will have the most impact on the community.

Telemetry can always be disabled by providing the flag `--telemetry-endpoint=""`.

## Implementation

Telemetry is reported via the Prometheus Remote Write protocol.
Any metrics prefixed with `spicedb_telemetry` are reported hourly to `telemetry.authzed.com`.
You can find all of the code in [internal/telemetry][telemetry-package].

[telemetry-package]: https://github.com/authzed/spicedb/tree/main/internal/telemetry

## Collected metrics

### spicedb_telemetry_info (Gauge)

Information about the SpiceDB environment.

- CPU Architecture: architecture of the CPU
- vCPUs: number of CPUs reported by the Go runtime
- OS: operating system family as reported by the Go Runtime
- Datastore Engine: datastore engine configured
- Cluster ID: unique identifier for a cluster's datastore
- NodeID: unique identifier for the node, usually the hostname

### spicedb_telemetry_object_definitions_total (Gauge)

Count of the number of objects defined by the schema.

- Cluster ID: unique identifier for a cluster's datastore
- NodeID: unique identifier for the node, usually the hostname

### spicedb_telemetry_relationships_estimate_total (Gauge)

Rough estimate of the number of relationships stored in the datastore.

- Cluster ID: unique identifier for a cluster's datastore
- NodeID: unique identifier for the node, usually the hostname

### spicedb_telemetry_dispatches (Histogram)

Histogram of cluster dispatches performed by the instance.

- Method: gRPC method of the dispatch request
- Cached: boolean of whether or not the dispatch was a cache hit
- Cluster ID: unique identifier for a cluster's datastore
- NodeID: unique identifier for the node, usually the hostname
