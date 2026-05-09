# PSQL wire protocol 🔌

[![CI](https://github.com/jeroenrinzema/psql-wire/actions/workflows/ci.yaml/badge.svg)](https://github.com/jeroenrinzema/psql-wire/actions/workflows/ci.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/jeroenrinzema/psql-wire.svg)](https://pkg.go.dev/github.com/jeroenrinzema/psql-wire) [![Latest release](https://img.shields.io/github/release/jeroenrinzema/psql-wire.svg)](https://github.com/jeroenrinzema/psql-wire/releases) [![Go Report Card](https://goreportcard.com/badge/github.com/jeroenrinzema/psql-wire)](https://goreportcard.com/report/github.com/jeroenrinzema/psql-wire)

A pure Go [PostgreSQL](https://www.postgresql.org/) server wire protocol implementation.
Build your own PostgreSQL server within a few lines of code.
This project attempts to make it as straight forward as possible to set-up and configure your own PSQL server.
Feel free to check out the [examples](https://github.com/jeroenrinzema/psql-wire/tree/main/examples) directory for various ways on how to configure/set-up your own server.

You can use this package to build your own fully fledged, PostgreSQL compatible database, or simply play with the wire protocol by creating a PSQL honeypot, testing drivers, or experimenting with third-party integrations.

It’s designed to give you a high-level implementation out of the box, while staying hackable so you can bend it to whatever you want to build.

> 🚧 This project does not include a PSQL parser. Please check out other projects such as [auxten/postgresql-parser](https://github.com/auxten/postgresql-parser) to parse PSQL SQL queries.

```go
package main

import (
	"context"
	"fmt"

	wire "github.com/jeroenrinzema/psql-wire"
)

func main() {
	wire.ListenAndServe("127.0.0.1:5432", handler)
}

func handler(ctx context.Context, query string) (wire.PreparedStatements, error) {
	return wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		fmt.Println(query)
		return writer.Complete("OK")
	})), nil
}
```

---

## Session Attributes

You can store custom session attributes for each client connection, allowing you to track session state:

```go
// Set a session attribute
wire.SetAttribute(ctx, "tenant_id", "tenant-123")

// Get a session attribute
tenantID, ok := wire.GetAttribute(ctx, "tenant_id")
```

---

> 🚧 When wanting to debug issues and or inspect the PostgreSQL wire protocol please check out the [psql-proxy](https://github.com/cloudproud/psql-proxy) cli

## Support

Feel free to start a new discussion to discuss feature requests or issues.

## Used by

<p align="center">
  <a href="https://cloudproud.nl" target="_blank" rel="noopener noreferrer">
    <img src=".github/assets/cloudproud_logo.svg" alt="Cloudproud" height="60">
  </a>
  &nbsp;&nbsp;&nbsp;&nbsp;
  <a href="https://www.shopify.com" target="_blank" rel="noopener noreferrer">
    <img src=".github/assets/shopify_logo.svg" alt="Shopify" height="60">
  </a>
</p>

## Contributing

Thank you for your interest in contributing to psql-wire!
Check out the open projects and/or issues and feel free to join any ongoing discussion.
Feel free to checkout the [open TODO's](https://github.com/jeroenrinzema/psql-wire/issues?q=is%3Aissue+is%3Aopen+label%3Atodo) within the project.

Everyone is welcome to contribute, whether it's in the form of code, documentation, bug reports, feature requests, or anything else. We encourage you to experiment with the project and make contributions to help evolve it to meet your needs!

See the [contributing guide](https://github.com/jeroenrinzema/psql-wire/blob/main/CONTRIBUTING.md) for more details.


