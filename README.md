# SpiceDB

[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/6348/badge)](https://bestpractices.coreinfrastructure.org/projects/6348)
[![Container Image](https://img.shields.io/github/v/release/authzed/spicedb?color=%232496ED&label=container&logo=docker "Container Image")](https://hub.docker.com/r/authzed/spicedb/tags)
[![Docs](https://img.shields.io/badge/docs-authzed.com-%234B4B6C "Authzed Documentation")](https://docs.authzed.com)
[![Discord Server](https://img.shields.io/discord/844600078504951838?color=7289da&label=discord&logo=discord&logoColor=7289da "Discord Server")](https://authzed.com/discord)
[![Twitter](https://img.shields.io/badge/twitter-%40authzed-1D8EEE?logo=twitter "@authzed on Twitter")](https://twitter.com/authzed)

SpiceDB is an open source, [Google Zanzibar]-inspired, database system for creating and managing security-critical application permissions.

Developers create a schema that models their permissions requirements and use any of the official or community maintained [client libraries] to apply the schema to the database, insert data into the database, and query the data to efficiently check permissions in their applications.

Features that distinguish SpiceDB from other systems include:

- Expressive [gRPC] and [HTTP/JSON] APIs for checking permissions, listing access, and powering devtools
- A distributed, parallel graph-engine faithful to the architecture described in [Google's Zanzibar paper]
- A flexible consistency model configurable [per-request] that includes resistance to the [New Enemy Problem]
- An expressive [schema language] with tools for [rapid prototyping], [integration testing], and [validating designs] in CI/CD pipelines
- Pluggable storage system supporting [in-memory], [PostgreSQL], [Cloud Spanner], [CockroachDB], and [MySQL]
- Deep observability with [Prometheus] metrics, [pprof] profiles, structured logging, and [OpenTelemetry] tracing

[Google Zanzibar]: https://authzed.com/blog/what-is-zanzibar/
[client libraries]: https://github.com/authzed/awesome-spicedb#clients

[gRPC]: https://buf.build/authzed/api/docs/main:authzed.api.v1
[HTTP/JSON]: https://app.swaggerhub.com/apis-docs/authzed/authzed/1.0

[Google's Zanzibar paper]: https://authzed.com/zanzibar

[per-request]: https://docs.authzed.com/reference/api-consistency
[New Enemy Problem]: https://authzed.com/blog/new-enemies/

[schema language]: https://docs.authzed.com/guides/schema
[rapid prototyping]: https://play.authzed.com
[integration testing]: https://github.com/authzed/action-spicedb
[validating designs]: https://github.com/authzed/action-spicedb-validate

[in-memory]: https://github.com/hashicorp/go-memdb
[PostgreSQL]: https://www.postgresql.org
[Cloud Spanner]: https://cloud.google.com/spanner
[CockroachDB]: https://github.com/cockroachdb/cockroach
[MySQL]: https://www.mysql.com

[Prometheus]: https://prometheus.io
[pprof]: https://jvns.ca/blog/2017/09/24/profiling-go-with-pprof/
[OpenTelemetry]: https://opentelemetry.io

Have questions? Ask in our [Discord].

Want to learn more about the inspiration for SpiceDB? We've annotated [Google's Zanzibar Paper] with our own commentary.

Looking to contribute? See [CONTRIBUTING.md].

You can find issues by priority: [Urgent], [High], [Medium], [Low], [Maybe].
There are also [good first issues].

[Discord]: https://authzed.com/discord
[CONTRIBUTING.md]: https://github.com/authzed/spicedb/blob/main/CONTRIBUTING.md
[Urgent]: https://github.com/authzed/spicedb/labels/priority%2F0%20urgent
[High]: https://github.com/authzed/spicedb/labels/priority%2F1%20high
[Medium]: https://github.com/authzed/spicedb/labels/priority%2F2%20medium
[Low]: https://github.com/authzed/spicedb/labels/priority%2F3%20low
[Maybe]: https://github.com/authzed/spicedb/labels/priority%2F4%20maybe
[good first issues]: https://github.com/authzed/spicedb/labels/hint%2Fgood%20first%20issue

## Getting Started

### Installing the binary

Binary releases are available for Linux, macOS, and Windows on AMD64 and ARM64 architectures.

[Homebrew] users for both macOS and Linux can install the latest binary releases of SpiceDB and [zed] using the official tap:

```command
brew install authzed/tap/spicedb authzed/tap/zed
```

[Debian-based Linux] users can install SpiceDB packages by adding a new APT source:

```command
sudo echo "deb [trusted=yes] https://apt.fury.io/authzed/ /" > /etc/apt/sources.list.d/authzed-fury.list
sudo apt update && apt install spicedb
```

[RPM-based Linux] users can install SpiceDB packages by adding a new YUM repository:

```command
sudo cat << EOF >> /etc/yum.repos.d/Authzed-Fury.repo
[authzed-fury]
name=AuthZed Fury Repository
baseurl=https://yum.fury.io/authzed/
enabled=1
gpgcheck=0
EOF
sudo dnf install spicedb
```

[zed]: https://github.com/authzed/zed
[homebrew]: https://docs.authzed.com/spicedb/installing#brew
[Debian-based Linux]: https://en.wikipedia.org/wiki/List_of_Linux_distributions#Debian-based
[RPM-based Linux]: https://en.wikipedia.org/wiki/List_of_Linux_distributions#RPM-based
  
### Running a container

Container images are available for AMD64 and ARM64 architectures on the following registries:

- [authzed/spicedb](https://hub.docker.com/r/authzed/spicedb)
- [ghcr.io/authzed/spicedb](https://github.com/authzed/spicedb/pkgs/container/spicedb)
- [quay.io/authzed/spicedb](https://quay.io/authzed/spicedb)

[Docker] users can run the latest SpiceDB container with the following:

```command
docker run --rm -p 50051:50051 authzed/spicedb serve --grpc-preshared-key "somerandomkeyhere"
```

SpiceDB containers use [Chainguard Images] to ship the bare minimum userspace which is a huge boon to security, but can complicate debugging.
If you want to execute a user session into a running SpiceDB container and install packages, you can use one of our debug images.

Appending `-debug` to any tag will provide you an image that has a userspace with debug tooling:

```command
docker run --rm -ti --entrypoint sh authzed/spicedb:latest-debug
```

Containers are also available for each git commit to the `main` branch under `${REGISTRY}/authzed/spicedb-git:${COMMIT}`.

[Docker]: https://docs.docker.com/get-docker/
[Chainguard Images]: https://github.com/chainguard-images/images
  
### Deploying to Kubernetes

Production Kubernetes users should be relying on a stable release of the [SpiceDB Operator].
The Operator enforces not only best practices, but orchestrates SpiceDB updates without downtime.

If you're only experimenting, feel free to try out one of our community-maintained [examples] for [testing SpiceDB on Kubernetes]:

```command
kubectl apply -f https://raw.githubusercontent.com/authzed/examples/main/kubernetes/example.yaml
```

[examples]: https://github.com/authzed/examples
[Docker Compose]: https://github.com/authzed/examples/tree/main/datastores
[SpiceDB Operator]: https://github.com/authzed/spicedb-operator
[testing SpiceDB on Kubernetes]: https://github.com/authzed/examples/tree/main/kubernetes

### Developing your own schema

You can try both SpiceDB and zed entirely in your browser on the [Playground] thanks to the power of WebAssembly.

If you don't want to start with the examples loadable from the Playground, you can follow a guide for [developing a schema] or review the the schema language [design documentation].

To get a quick idea of schema development, you can watch the creators of SpiceDB writing a schema for GitHub:

[![Modeling GitHub YouTube Video Thumbnail](https://user-images.githubusercontent.com/343539/223837989-ead99ff9-ef35-4cf3-864d-d8d86ecdf9ce.png)](https://www.youtube.com/watch?v=x3-B9-ICj0w)

[Playground]: https://play.authzed.com
[developing a schema]: https://docs.authzed.com/guides/schema
[design documentation]: https://docs.authzed.com/reference/schema-lang

### Trying out the API

For debugging or getting started, we recommend [installing zed], the official command-line client.
The [Playground] also has a tab for experimenting with zed all from within your browser.

When it's time to write code, we recommend using one of the [existing client libraries] whether it's official or community-maintained.

Because every millisecond counts, we recommend using libraries that leverage the gRPC API for production workloads.

To get an understanding of integrating an application with SpiceDB, you can follow the [Protecting Your First App] guide or review API documentation on the [Buf Registry] or [Postman].

[installing zed]: https://github.com/authzed/zed
[existing client libraries]: https://github.com/authzed/awesome-spicedb#clients
[Protecting Your First App]: https://docs.authzed.com/guides/first-app
[Buf Registry]: https://buf.build/authzed/api/docs
[Postman]: https://www.postman.com/authzed/workspace/spicedb/overview

## Acknowledgements

SpiceDB is a community project fueled by contributions from both organizations and individuals.
We appreciate all contributions, large and small, and would like to thank all those involved.

In addition, we'd like to highlight a few notable contributions:

- The GitHub Authorization Team for implementing and contributing the MySQL datastore
