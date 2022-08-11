# SpiceDB

[![Container Image](https://img.shields.io/github/v/release/authzed/spicedb?color=%232496ED&label=container&logo=docker "Container Image")](https://hub.docker.com/r/authzed/spicedb/tags)
[![Docs](https://img.shields.io/badge/docs-authzed.com-%234B4B6C "Authzed Documentation")](https://docs.authzed.com)
[![Build Status](https://github.com/authzed/spicedb/workflows/Build%20&%20Test/badge.svg "GitHub Actions")](https://github.com/authzed/spicedb/actions)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/6348/badge)](https://bestpractices.coreinfrastructure.org/projects/6348)
[![Discord Server](https://img.shields.io/discord/844600078504951838?color=7289da&logo=discord "Discord Server")](https://discord.gg/jTysUaxXzM)
[![Twitter](https://img.shields.io/twitter/follow/authzed?color=%23179CF0&logo=twitter&style=flat-square&label=@authzed "@authzed on Twitter")](https://twitter.com/authzed)

SpiceDB is an open source database system for managing security-critical application permissions inspired by Google's [Zanzibar] paper.

Developers create a schema that models their permissions requirements and use a [client library] to apply the schema to the database, insert data into the database, and query the data to efficiently check permissions in their applications.

[client library]: https://github.com/orgs/authzed/repositories?q=client+library

Features that distinguish SpiceDB from other systems include:

- Expressive [gRPC] and [HTTP] APIs for checking permissions, listing access, and powering devtools
- A distributed, parallel graph-engine faithful to the architecture described in [Google's Zanzibar paper]
- A flexible consistency model configurable [per-request] that includes resistance to the [New Enemy Problem]
- An expressive [schema language] with tools for [rapid prototyping], [integration testing], and [validating designs] in CI/CD pipelines
- Pluggable storage system supporting [memdb], [MySQL], [PostgreSQL], [CockroachDB], and [Cloud Spanner]
- Deep observability with [Prometheus metrics], structured logging, and [OpenTelemetry tracing]

[gRPC]: https://buf.build/authzed/api/docs/main:authzed.api.v1
[Zanzibar]: https://authzed.com/blog/what-is-zanzibar/
[HTTP]: https://app.swaggerhub.com/apis-docs/authzed/authzed/1.0
[Google's Zanzibar paper]: https://authzed.com/blog/what-is-zanzibar/
[per-request]: https://docs.authzed.com/reference/api-consistency
[New Enemy Problem]: https://authzed.com/blog/new-enemies/
[schema language]: https://docs.authzed.com/guides/schema
[rapid prototyping]: https://play.authzed.com
[integration testing]: https://github.com/authzed/action-spicedb
[validating designs]: https://github.com/authzed/action-spicedb-validate
[memdb]: https://github.com/hashicorp/go-memdb
[MySQL]: https://www.mysql.com
[PostgreSQL]: https://www.postgresql.org
[CockroachDB]: https://github.com/cockroachdb/cockroach
[Cloud Spanner]: https://cloud.google.com/spanner
[Prometheus metrics]: https://prometheus.io
[OpenTelemetry tracing]: https://opentelemetry.io

Have questions? Join our [Discord].

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

- Install SpiceDB with [homebrew] on macOS and Linux
- Run a SpiceDB container using a container engine such as [docker]
- Deploy non-production-ready [examples] using [Kubernetes] and [Docker Compose]

[homebrew]: https://docs.authzed.com/spicedb/installing#brew
[docker]: https://docs.authzed.com/spicedb/installing#docker
[examples]: https://github.com/authzed/examples
[Docker Compose]: https://github.com/authzed/examples/tree/main/docker-compose
[Kubernetes]: https://github.com/authzed/examples/tree/main/kubernetes

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
