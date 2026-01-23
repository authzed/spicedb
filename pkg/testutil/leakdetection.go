package testutil

import (
	"go.uber.org/goleak"
)

func GoLeakIgnores() []goleak.Option {
	return []goleak.Option{
		goleak.IgnoreAnyFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
}
