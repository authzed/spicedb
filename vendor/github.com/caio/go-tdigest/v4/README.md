# T-Digest

A fast map-reduce and parallel streaming friendly data-structure for accurate
quantile approximation.

This package provides an implementation of Ted Dunning's t-digest data
structure in Go.

[![GoDoc](https://godoc.org/github.com/caio/go-tdigest?status.svg)](http://godoc.org/github.com/caio/go-tdigest)
[![Go Report Card](https://goreportcard.com/badge/github.com/caio/go-tdigest)](https://goreportcard.com/report/github.com/caio/go-tdigest)

## Project Status

This project is actively maintained. We are happy to collaborate on features
and issues if/when they arrive.

## Installation

This package uses go modules. Our releases are tagged and signed following
the [Semantic Versioning][semver] scheme.

    go get github.com/caio/go-tdigest/v4


[semver]: http://semver.org/

## Example Usage

```go
package main

import (
	"fmt"
	"math/rand"

	"github.com/caio/go-tdigest/v4"
)

func main() {
	// Analogue to tdigest.New(tdigest.Compression(100))
	t, _ := tdigest.New()

	for i := 0; i < 10000; i++ {
		// Analogue to t.AddWeighted(rand.Float64(), 1)
		t.Add(rand.Float64())
	}

	fmt.Printf("p(.5) = %.6f\n", t.Quantile(0.5))
	fmt.Printf("CDF(Quantile(.5)) = %.6f\n", t.CDF(t.Quantile(0.5)))
}
```

## Configuration

You can configure your digest upon creation with options documented
at [options.go](options.go). Example:

```go
// Construct a digest with compression=200 and its own
// (thread-unsafe) RNG seeded with 0xCA10:
digest, _ := tdigest.New(
        tdigest.Compression(200),
        tdigest.LocalRandomNumberGenerator(0xCA10),
)
```

## References

This is a port of the [reference][1] implementation with some ideas borrowed
from the [python version][2]. If you wanna get a quick grasp of how it works
and why it's useful, [this video and companion article is pretty helpful][3].

[1]: https://github.com/tdunning/t-digest
[2]: https://github.com/CamDavidsonPilon/tdigest
[3]: https://www.mapr.com/blog/better-anomaly-detection-t-digest-whiteboard-walkthrough

