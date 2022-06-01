# Quickstart examples

This directory contains a variety of quickstart examples to start SpiceDB with various storage engines.

> **Warning**
> These examples are **not** production-ready and are provided purely for local testing only.

## Usage

To use a quickstart, run:

```shell
docker compose -f <file> up
```

For example, running the Postgres quickstart:

```shell
docker compose -f postgres.yml up
```

Use `Ctrl+C` to stop the quickstart and use

```shell
docker compose -f postgres.yml down
```

to shut down the containers.

> **Note**
> Most quickstarts have configuration services in them to properly set up the storage engine. This means it may take a minute or so before the quickstart is ready to use.

## Available quickstarts

- [CockroachDB](crdb.yml)
- [Memory](memory.yml)
- [MySQL](mysql.yml)
- [Postgres](postgres.yml)
- [Spanner](spanner.yml)
