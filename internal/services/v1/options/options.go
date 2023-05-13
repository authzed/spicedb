package options

import "time"

//go:generate go run github.com/ecordell/optgen -output zz_generated.query_options.go . ExperimentalServerOptions

type ExperimentalServerOptions struct {
	StreamReadTimeount time.Duration `debugmap:"visible" default:"600s"`
}
