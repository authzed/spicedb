package testutil

import (
	"go.uber.org/goleak"
)

func GoLeakIgnores() []goleak.Option {
	return []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreAnyFunction("github.com/Yiling-J/theine-go/internal.(*Store[...]).maintance"),
		goleak.IgnoreAnyFunction("github.com/Yiling-J/theine-go/internal.(*Store[...]).maintance.func1"),
		goleak.IgnoreTopFunction("github.com/outcaste-io/ristretto.(*lfuPolicy).processItems"),
		goleak.IgnoreTopFunction("github.com/outcaste-io/ristretto.(*Cache).processItems"),
		goleak.IgnoreAnyFunction("github.com/maypok86/otter/internal/core.(*Cache[...]).cleanup"),
		goleak.IgnoreAnyFunction("github.com/maypok86/otter/internal/core.(*Cache[...]).process"),
		goleak.IgnoreAnyFunction("github.com/maypok86/otter/internal/unixtime.startTimer.func1"),
	}
}
