package testutil

import (
	"go.uber.org/goleak"
)

func GoLeakIgnores() []goleak.Option {
	return []goleak.Option{
		goleak.IgnoreAnyFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreAnyFunction("github.com/lthibault/jitterbug.(*Ticker).loop"), // https://github.com/lthibault/jitterbug/pull/10
	}
}
