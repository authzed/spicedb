package options

import "time"

//go:generate go run github.com/ecordell/optgen -output zz_generated.query_options.go . ExperimentalServerOptions

type ExperimentalServerOptions struct {
	StreamReadTimeout       time.Duration `debugmap:"visible" default:"600s"`
	DefaultExportBatchSize  uint32        `debugmap:"visible" default:"1_000"`
	MaxExportBatchSize      uint32        `debugmap:"visible" default:"100_000"`
	BulkCheckMaxConcurrency uint16        `debugmap:"visible" default:"50"`
}
