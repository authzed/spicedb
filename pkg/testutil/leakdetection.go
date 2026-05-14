package testutil

import (
	"go.uber.org/goleak"
)

func GoLeakIgnores() []goleak.Option {
	return []goleak.Option{
		// TODO: https://github.com/googleapis/google-cloud-go/issues/14228
		// when that is complete, remove this line
		goleak.IgnoreAnyFunction("go.opencensus.io/stats/view.(*worker).start"),
		// Otter doesn't expose a `close` function, so we can't close down its periodicCleanup
		// function. See https://github.com/maypok86/otter/issues/181
		goleak.IgnoreAnyFunction("github.com/maypok86/otter/v2.(*cache[...]).periodicCleanUp"),
	}
}
