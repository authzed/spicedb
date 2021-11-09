# SpiceDB

[![Container Image](https://img.shields.io/github/v/release/authzed/spicedb?color=%232496ED&label=container&logo=docker "Container Image")](https://quay.io/repository/authzed/spicedb?tab=tags)
[![Docs](https://img.shields.io/badge/docs-authzed.com-%234B4B6C "Authzed Documentation")](https://docs.authzed.com)
[![GoDoc](https://godoc.org/github.com/authzed/spicedb?status.svg "Go documentation")](https://godoc.org/github.com/authzed/spicedb)
[![Build Status](https://github.com/authzed/spicedb/workflows/Build%20&%20Test/badge.svg "GitHub Actions")](https://github.com/authzed/spicedb/actions)
[![Discord Server](https://img.shields.io/discord/844600078504951838?color=7289da&logo=discord "Discord Server")](https://discord.gg/jTysUaxXzM)
[![Twitter](https://img.shields.io/twitter/follow/authzed?color=%23179CF0&logo=twitter&style=flat-square "@authzed on Twitter")](https://twitter.com/authzed)

SpiceDB is a database system for managing security-critical application permissions.

Developers create a schema that models their permissions requirements and use a [client library] to apply the schema to the database, insert data into the database, and query the data to efficiently check permissions in their applications.

Features that distinguish SpiceDB from other systems include:

- Expressive [gRPC] and [HTTP] APIs for checking permissions, listing access, and powering devtools
- An architecture faithful to [Google's Zanzibar paper], including resistance to the [New Enemy Problem]
- An intuitive and expressive [schema language] complete with a [playground] dev environment
- A powerful graph engine that supports distributed, parallel evaluation
- Pluggable storage that supports [in-memory], [PostgreSQL], and [CockroachDB]
- Deep observability with [Prometheus metrics], structured logging, and [distributed tracing]

See [CONTRIBUTING.md] for instructions on how to contribute and perform common tasks like building the project and running tests.

[client library]: https://docs.authzed.com/reference/api#client-libraries
[gRPC]: https://buf.build/authzed/api
[HTTP]: https://petstore.swagger.io/?url=https://raw.githubusercontent.com/authzed/authzed-go/main/proto/apidocs.swagger.json
[Google's Zanzibar paper]: https://authzed.com/blog/what-is-zanzibar/
[New Enemy Problem]: https://authzed.com/blog/new-enemies/
[schema language]: https://docs.authzed.com/guides/schema
[playground]: https://play.authzed.com
[in-memory]: https://github.com/hashicorp/go-memdb
[PostgreSQL]: https://www.postgresql.org
[CockroachDB]: https://github.com/cockroachdb/cockroach
[Prometheus metrics]: https://prometheus.io
[distributed tracing]: https://opentelemetry.io
[CONTRIBUTING.md]: CONTRIBUTING.md

## Why SpiceDB?

### Verifiable Correctness

The data used to calculate permissions have the most critical correctness requirements in the entirety a software system.
Despite that, developers continue to build their own ad-hoc solutions coupled to the internal code of each new project.
By developing a SpiceDB schema, you can iterate far more quickly and exhaustively test designs before altering any application code.
This becomes especially important as you introduce backwards-compatible changes to the schema and want to ensure that the system remains secure.

### Optimal Flexibility

The SpiceDB schema language is built on top of the concept of a graph of relationships between objects.
This ReBAC design is capable of efficiently supporting all popular access control models (such as [RBAC] and [ABAC]) and custom models that contain hybrid behavior.

Modern solutions to developing permission systems all have a similar goal: to decouple _policy_ from the application.
Using a dedicated database like SpiceDB not only accomplishes this, but takes this idea a step further by also decoupling the data that policies operate on.
SpiceDB is designed to share a single unified view of permissions across as many applications as your organization has.
This has strategy has become an industry best-practice and is being used to great success at companies large ([Google], GitHub, [Airbnb]) and small ([Carta], [Authzed]).

[RBAC]: https://docs.authzed.com/concepts/authz#what-is-rbac
[ABAC]: https://docs.authzed.com/concepts/authz#what-is-abac
[Google]: https://research.google/pubs/pub48190/
[Airbnb]: https://medium.com/airbnb-engineering/himeji-a-scalable-centralized-system-for-authorization-at-airbnb-341664924574
[Carta]: https://medium.com/building-carta/authz-cartas-highly-scalable-permissions-system-782a7f2c840f
[Authzed]: https://authzed.com

## Getting Started

### Installing SpiceDB

SpiceDB is currently packaged by [Homebrew] for both macOS and Linux.
Individual releases and other formats are also available on the [releases page].

[Homebrew]: https://brew.sh
[releases page]: https://github.com/authzed/spicedb/releases

```sh
brew install authzed/tap/spicedb
```

SpiceDB is also available as a container image:

```sh
docker pull quay.io/authzed/spicedb:latest
docker run quay.io/authzed/spicedb serve --grpc-preshared-key "somerandomkeyhere"
```

SpiceDB supports environment variables. You can replace any command's argument with an environment variable by adding the `SPICEDB` prefix.  
For example `--log-level` becomes `SPICEDB_LOG_LEVEL`.

```sh
docker run -e SPICEDB_GRPC_PRESHARED_KEY=somerandomkeyhere quay.io/authzed/spicedb serve
```

For production usage, we **highly** recommend using a tag that corresponds to the [latest release], rather than `latest`.

[latest release]: https://github.com/authzed/spicedb/releases

### Running SpiceDB locally

```sh
spicedb serve --grpc-preshared-key "somerandomkeyhere"
```

Visit [http://localhost:8080](http://localhost:8080) to see next steps, including loading the schema

### Running SpiceDB for testing

```sh
spicedb serve-testing
```

This command runs SpiceDB such that each [Bearer Token] provided by the client is allocated its own isolated, ephemeral datastore.
By using unique tokens in each of your application's integration tests, they can be executed in parallel safely against a single instance of SpiceDB.

A [SpiceDB GitHub action] is also available to run SpiceDB as part of your integration test workflows.

[Bearer Token]: https://docs.authzed.com/reference/api#authentication
[SpiceDB GitHub action]: https://github.com/authzed/action-spicedb

### Developing your own schema

- Follow the guide for [developing a schema]
- [Watch a video] of us modeling GitHub
- Read the schema language [design documentation]
- [Jump into the playground], load up some examples, and mess around

[developing a schema]: https://docs.authzed.com/guides/schema
[Watch a video]: https://www.youtube.com/watch?v=x3-B9-ICj0w
[design documentation]: https://docs.authzed.com/reference/schema-lang
[Jump into the playground]: https://play.authzed.com

### Integrating with your application

- Learn the latest best practice by following the [Protecting Your First App] guide
- Explore the gRPC API documentation on the [Buf Registry]
- [Install zed] and interact with a live database

[Protecting Your First App]: https://docs.authzed.com/guides/first-app
[Buf Registry]: https://buf.build/authzed/api/docs
[Install zed]: https://github.com/authzed/zed
