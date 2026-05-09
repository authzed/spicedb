
# slog: Zerolog handler

[![tag](https://img.shields.io/github/tag/samber/slog-zerolog.svg)](https://github.com/samber/slog-zerolog/releases)
![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.21-%23007d9c)
[![GoDoc](https://godoc.org/github.com/samber/slog-zerolog?status.svg)](https://pkg.go.dev/github.com/samber/slog-zerolog)
![Build Status](https://github.com/samber/slog-zerolog/actions/workflows/test.yml/badge.svg)
[![Go report](https://goreportcard.com/badge/github.com/samber/slog-zerolog)](https://goreportcard.com/report/github.com/samber/slog-zerolog)
[![Coverage](https://img.shields.io/codecov/c/github/samber/slog-zerolog)](https://codecov.io/gh/samber/slog-zerolog)
[![Contributors](https://img.shields.io/github/contributors/samber/slog-zerolog)](https://github.com/samber/slog-zerolog/graphs/contributors)
[![License](https://img.shields.io/github/license/samber/slog-zerolog)](./LICENSE)

A [Zerolog](https://github.com/rs/zerolog) Handler for [slog](https://pkg.go.dev/log/slog) Go library.

<div align="center">
  <hr>
  <sup><b>Sponsored by:</b></sup>
  <br>
  <a href="https://www.dash0.com?utm_campaign=148395251-samber%20github%20sponsorship&utm_source=github&utm_medium=sponsorship&utm_content=samber">
    <div>
      <img src="https://github.com/user-attachments/assets/b1f2e876-0954-4dc3-824d-935d29ba8f3f" width="200" alt="Dash0">
    </div>
    <div>
      100% OpenTelemetry-native observability platform<br>Simple to use, built on open standards, and designed for full cost control
    </div>
  </a>
  <hr>
</div>

**See also:**

- [slog-multi](https://github.com/samber/slog-multi): `slog.Handler` chaining, fanout, routing, failover, load balancing...
- [slog-formatter](https://github.com/samber/slog-formatter): `slog` attribute formatting
- [slog-sampling](https://github.com/samber/slog-sampling): `slog` sampling policy
- [slog-mock](https://github.com/samber/slog-mock): `slog.Handler` for test purposes

**HTTP middlewares:**

- [slog-gin](https://github.com/samber/slog-gin): Gin middleware for `slog` logger
- [slog-echo](https://github.com/samber/slog-echo): Echo middleware for `slog` logger
- [slog-fiber](https://github.com/samber/slog-fiber): Fiber middleware for `slog` logger
- [slog-chi](https://github.com/samber/slog-chi): Chi middleware for `slog` logger
- [slog-http](https://github.com/samber/slog-http): `net/http` middleware for `slog` logger

**Loggers:**

- [slog-zap](https://github.com/samber/slog-zap): A `slog` handler for `Zap`
- [slog-zerolog](https://github.com/samber/slog-zerolog): A `slog` handler for `Zerolog`
- [slog-logrus](https://github.com/samber/slog-logrus): A `slog` handler for `Logrus`

**Log sinks:**

- [slog-datadog](https://github.com/samber/slog-datadog): A `slog` handler for `Datadog`
- [slog-betterstack](https://github.com/samber/slog-betterstack): A `slog` handler for `Betterstack`
- [slog-rollbar](https://github.com/samber/slog-rollbar): A `slog` handler for `Rollbar`
- [slog-loki](https://github.com/samber/slog-loki): A `slog` handler for `Loki`
- [slog-sentry](https://github.com/samber/slog-sentry): A `slog` handler for `Sentry`
- [slog-syslog](https://github.com/samber/slog-syslog): A `slog` handler for `Syslog`
- [slog-logstash](https://github.com/samber/slog-logstash): A `slog` handler for `Logstash`
- [slog-fluentd](https://github.com/samber/slog-fluentd): A `slog` handler for `Fluentd`
- [slog-graylog](https://github.com/samber/slog-graylog): A `slog` handler for `Graylog`
- [slog-quickwit](https://github.com/samber/slog-quickwit): A `slog` handler for `Quickwit`
- [slog-slack](https://github.com/samber/slog-slack): A `slog` handler for `Slack`
- [slog-telegram](https://github.com/samber/slog-telegram): A `slog` handler for `Telegram`
- [slog-mattermost](https://github.com/samber/slog-mattermost): A `slog` handler for `Mattermost`
- [slog-microsoft-teams](https://github.com/samber/slog-microsoft-teams): A `slog` handler for `Microsoft Teams`
- [slog-webhook](https://github.com/samber/slog-webhook): A `slog` handler for `Webhook`
- [slog-kafka](https://github.com/samber/slog-kafka): A `slog` handler for `Kafka`
- [slog-nats](https://github.com/samber/slog-nats): A `slog` handler for `NATS`
- [slog-parquet](https://github.com/samber/slog-parquet): A `slog` handler for `Parquet` + `Object Storage`
- [slog-channel](https://github.com/samber/slog-channel): A `slog` handler for Go channels

## 🚀 Install

```sh
go get github.com/samber/slog-zerolog/v2
```

**Compatibility**: go >= 1.21

No breaking changes will be made to exported APIs before v3.0.0.

## 💡 Usage

GoDoc: [https://pkg.go.dev/github.com/samber/slog-zerolog/v2](https://pkg.go.dev/github.com/samber/slog-zerolog/v2)

### Handler options

```go
type Option struct {
    // log level (default: debug)
	// you can use ZeroLogLeveler to retrieve the level from the global zerolog instance or a custom one
    Level slog.Leveler

    // optional: zerolog logger (default: zerolog.Logger)
    Logger *zerolog.Logger
    // optional: don't add timestamp to record
    NoTimestamp bool

    // optional: customize json payload builder
    Converter Converter
    // optional: fetch attributes from context
    AttrFromContext []func(ctx context.Context) []slog.Attr

    // optional: see slog.HandlerOptions
    AddSource   bool
    ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
}
```

Other global parameters:

```go
slogzerolog.SourceKey = "source"
slogzerolog.ErrorKeys = []string{"error", "err"}
slogzerolog.LogLevels = map[slog.Level]zerolog.Level{...}
```

### Example

```go
import (
    "github.com/rs/zerolog"
    slogzerolog "github.com/samber/slog-zerolog/v2"
    "os"
    "log/slog"
)

func main() {
    zerologLogger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr})

    logger := slog.New(slogzerolog.Option{Level: slog.LevelDebug, Logger: &zerologLogger}.NewZerologHandler())
    logger = logger.
        With("environment", "dev").
        With("release", "v1.0.0")

    // log error
    logger.
        With("category", "sql").
        With("query.statement", "SELECT COUNT(*) FROM users;").
        With("query.duration", 1*time.Second).
        With("error", fmt.Errorf("could not count users")).
        Error("caramba!")

    // log user signup
    logger.
        With(
            slog.Group("user",
                slog.String("id", "user-123"),
                slog.Time("created_at", time.Now()),
            ),
        ).
        Info("user registration")
}
```

### Tracing

Import the samber/slog-otel library.

```go
import (
	slogzerolog "github.com/samber/slog-zerolog"
	slogotel "github.com/samber/slog-otel"
	"go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	tp := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
	)
	tracer := tp.Tracer("hello/world")

	ctx, span := tracer.Start(context.Background(), "foo")
	defer span.End()

	span.AddEvent("bar")

	logger := slog.New(
		slogzerolog.Option{
			// ...
			AttrFromContext: []func(ctx context.Context) []slog.Attr{
				slogotel.ExtractOtelAttrFromContext([]string{"tracing"}, "trace_id", "span_id"),
			},
		}.NewZerologHandler(),
	)

	logger.ErrorContext(ctx, "a message")
}
```

### Zerolog level mapping

Use the `slogzerolog.ZeroLogLeveler` as `slogzerolog.Option.Level` (`slog.Leveler`) to set the `slog.Level` from 
`zerolog.Level`.

Currently following levels are mapped:

| zerolog  | slog      | 
|----------|-----------| 
| Trace-N  | Debug-1-N | 
| Trace    | Debug-1   | 
| Debug    | Debug     | 
| Info     | Info      | 
| Warn     | Warn      | 
| Error    | Error     | 
| Panic    | Error     | 
| Fatal    | Error     | 
| NoLevel  | Info      | 
| Disabled | Debug-1   |
| *        | Info      |

```go
import (
    "github.com/rs/zerolog"
    slogzerolog "github.com/samber/slog-zerolog/v2"
    "os"
    "log/slog"
)

func main() {
    zerologLogger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr})

    logger := slog.New(slogzerolog.Option{Level: ZeroLogLeveler{&zerologLogger}, Logger: &zerologLogger}.NewZerologHandler())

    logger.Trace("caramba!")
}
```

## 🤝 Contributing

- Ping me on twitter [@samuelberthe](https://twitter.com/samuelberthe) (DMs, mentions, whatever :))
- Fork the [project](https://github.com/samber/slog-zerolog)
- Fix [open issues](https://github.com/samber/slog-zerolog/issues) or request new features

Don't hesitate ;)

```bash
# Install some dev dependencies
make tools

# Run tests
make test
# or
make watch-test
```

## 👤 Contributors

![Contributors](https://contrib.rocks/image?repo=samber/slog-zerolog)

## 💫 Show your support

Give a ⭐️ if this project helped you!

[![GitHub Sponsors](https://img.shields.io/github/sponsors/samber?style=for-the-badge)](https://github.com/sponsors/samber)

## 📝 License

Copyright © 2023 [Samuel Berthe](https://github.com/samber).

This project is [MIT](./LICENSE) licensed.
