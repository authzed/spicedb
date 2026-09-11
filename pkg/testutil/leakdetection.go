package testutil

import (
	"go.uber.org/goleak"
)

func GoLeakIgnores() []goleak.Option {
	return []goleak.Option{
		// TODO: https://github.com/googleapis/google-cloud-go/issues/14228
		// when that is complete, remove this line
		goleak.IgnoreAnyFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
}
