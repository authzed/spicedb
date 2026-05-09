# Zerologr

[![Go Reference](https://pkg.go.dev/badge/github.com/go-logr/zerologr.svg)](https://pkg.go.dev/github.com/go-logr/zerologr)
![test](https://github.com/go-logr/zerologr/workflows/test/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-logr/zerologr)](https://goreportcard.com/report/github.com/go-logr/zerologr)

A [logr](https://github.com/go-logr/logr) LogSink implementation using [Zerolog](https://github.com/rs/zerolog).

## Usage

```go
import (
    "os"

    "github.com/go-logr/logr"
    "github.com/go-logr/zerologr"
    "github.com/rs/zerolog"
)

func main() {
    zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs

    zerologr.NameFieldName = "logger"
    zerologr.NameSeparator = "/"
    zerologr.SetMaxV(1)

    zl := zerolog.New(os.Stderr)
    zl = zl.With().Caller().Timestamp().Logger()
    var log logr.Logger = zerologr.New(&zl)

    log.Info("Logr in action!", "the answer", 42)
}
```

## Implementation Details

For the most part, concepts in Zerolog correspond directly with those in logr.

V-levels in logr correspond to levels in Zerolog as `zerologLevel = 1 - logrV`. `logr.V(0)` is equivalent to `zerolog.InfoLevel` or 1; `logr.V(1)` is equivalent to `zerolog.DebugLevel` or 0 (default global level in Zerolog); `logr.V(2)` is equivalent to `zerolog.TraceLevel` or -1. Higher than 2 V-level is possible but misses some features in Zerolog, e.g. Hooks and Sampling. V-level value is a number and is only logged on Info(), not Error().
