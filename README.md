# SpiceDB

[![Container Image](https://img.shields.io/github/v/release/authzed/spicedb?color=%232496ED&label=container&logo=docker "Container Image")](https://quay.io/repository/authzed/spicedb?tab=tags)
[![GoDoc](https://godoc.org/github.com/authzed/spicedb?status.svg "Go documentation")](https://godoc.org/github.com/authzed/spicedb)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg "Apache 2.0 License")](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Build Status](https://github.com/authzed/spicedb/workflows/Build%20&%20Test/badge.svg "GitHub Actions")](https://github.com/authzed/spicedb/actions)
[![Mailing List](https://img.shields.io/badge/email-google%20groups-4285F4 "authzed-oss@googlegroups.com")](https://groups.google.com/g/authzed-oss)
[![Discord Server](https://img.shields.io/discord/844600078504951838?color=7289da&logo=discord "Discord Server")](https://discord.gg/jTysUaxXzM)
[![Twitter](https://img.shields.io/twitter/follow/authzed?color=%23179CF0&logo=twitter&style=flat-square "@authzed on Twitter")](https://twitter.com/authzed)

SpiceDB is a [Zanzibar]-inspired database that stores, computes, and validates application permissions.

Developers create a schema that models their permissions requirements and use a [client library] to apply the schema to the database, insert data into the database, and query the data to efficiently check permissions in their applications.

Features that distinguish SpiceDB from other systems include:
- [Expressive APIs] for checking permissions, listing access, and powering devtools
- An architecture faithful to the [Google Zanzibar] paper, including resistance to the [New Enemy Problem]
- An intuitive and expressive [schema language] complete with a [playground] dev environment
- A powerful graph engine that supports distributed, parallel evaluation
- Pluggable storage that supports [in-memory], [PostgreSQL], and [CockroachDB]
- Deep observability with [Prometheus metrics], structured logging, and [distributed tracing]

See [CONTRIBUTING.md] for instructions on how to contribute and perform common tasks like building the project and running tests.

[client library]: https://docs.authzed.com/reference/api#client-libraries
[Expressive APIs]: https://buf.build/authzed/api
[Zanzibar]: https://authzed.com/blog/what-is-zanzibar/
[Google Zanzibar]: https://authzed.com/blog/what-is-zanzibar/
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

The SpiceDB schema langauge is built on top of the concept of a graph of relationships between objects.
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

#### Pulling down a container image

The latest stable release can be obtained by running the following command:

```sh
docker pull quay.io/authzed/spicedb:latest
```

For production usage, we **highly** recommend using a tag that corresponds to the [latest release], rather than `latest`.

[latest release]: https://github.com/authzed/spicedb/releases

#### Building and installing a binary

In order to build and install SpiceDB, the [latest stable version of Go] must be installed.

Running the following command will install the latest stable release: 

```sh
go install github.com/authzed/spicedb/cmd/spicedb@latest
```

[latest stable version of Go]: https://golang.org/dl
[working Go environment]: https://golang.org/doc/code.html

### Running SpiceDB locally

```sh
spicedb serve --grpc-preshared-key "somerandomkeyhere" --grpc-no-tls
```

Visit [http://localhost:8080](http://localhost:8080) to see next steps, including loading the schema

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
