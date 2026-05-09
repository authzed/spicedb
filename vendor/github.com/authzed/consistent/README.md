# consistent

[![GoDoc](https://godoc.org/github.com/authzed/consistent?status.svg)](https://godoc.org/github.com/authzed/consistent)
[![Discord Server](https://img.shields.io/discord/844600078504951838?color=7289da&logo=discord "Discord Server")](https://authzed.com/discord)
[![Twitter](https://img.shields.io/badge/twitter-%40authzed-1D8EEE?logo=twitter "@authzed on Twitter")](https://twitter.com/authzed)

This package implements a gRPC Balancer that routes requests based upon a consistent hashring.
The hashing algorithm is customizable, but xxhash is recommended.
It was originally built to serve [SpiceDB](https://github.com/authzed/spicedb), but has been extracted from that repository to be made available for other projects.

In order to use this balancer, you must:

1. Register the balancer (typically in `main`):

```go
balancer.Register(consistent.NewBuilder(xxhash.Sum64))
```

2. Configure the connections:

```go
// This is using the defaults, but you can create your own config.
grpc.Dial(addr, grpc.WithDefaultServiceConfig(consistent.DefaultServiceConfigJSON))
```

## Acknowledgements

This project is a community effort fueled by contributions from both organizations and individuals.
We appreciate all contributions, large and small, and would like to thank all those involved.

A large portion of the structure of this library is based off of the example implementation in [grpc-go](https://github.com/grpc/grpc-go).
That original work is copyrighted by the gRPC authors and licensed under the Apache License, Version 2.0.
