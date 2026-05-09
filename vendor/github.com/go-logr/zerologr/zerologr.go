// Package zerologr defines an implementation of the github.com/go-logr/logr
// interfaces built on top of Zerolog (https://github.com/rs/zerolog).
//
// # Usage
//
// A new logr.Logger can be constructed from an existing zerolog.Logger using
// the New function:
//
//	log := zerologr.New(someZeroLogger)
//
// # Implementation Details
//
// For the most part, concepts in Zerolog correspond directly with those in
// logr.
//
// V-levels in logr correspond to levels in Zerolog as `zerologLevel = 1 - logrV`.
// `logr.V(0)` is equivalent to `zerolog.InfoLevel` or 1; `logr.V(1)` is equivalent to
// `zerolog.DebugLevel` or 0 (default global level in Zerolog); `logr.V(2)` is equivalent
// to `zerolog.TraceLevel` or -1. Higher than 2 V-level is possible but misses some
// features in Zerolog, e.g. Hooks and Sampling. V-level value is a number and is only
// logged on Info(), not Error().
package zerologr

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/rs/zerolog"
)

var (
	// NameFieldName is the field key for logr.WithName.
	NameFieldName = "logger"
	// NameSeparator separates names for logr.WithName.
	NameSeparator = "/"
	// VerbosityFieldName is the field key for logr.Info verbosity. If set to "",
	// its value is not emitted.
	VerbosityFieldName = "v"

	// RenderArgsHook mutates the list of key-value pairs passed directly to
	// logr.Info and logr.Error. If set to nil, it is disabled.
	RenderArgsHook = DefaultRender
	// RenderValuesHook mutates the list of key-value pairs saved via logr.WithValues.
	// If set to nil, it is disabled.
	RenderValuesHook = DefaultRender
)

const (
	minZerologLevel = -128 // zerolog.Level is int8
)

// Logger is type alias of logr.Logger.
type Logger = logr.Logger

// LogSink implements logr.LogSink and logr.CallDepthLogSink.
type LogSink struct {
	l     *zerolog.Logger
	name  string
	depth int
}

// Underlier exposes access to the underlying logging implementation.  Since
// callers only have a logr.Logger, they have to know which implementation is
// in use, so this interface is less of an abstraction and more of way to test
// type conversion.
type Underlier interface {
	GetUnderlying() *zerolog.Logger
}

var (
	_ logr.LogSink          = &LogSink{}
	_ logr.CallDepthLogSink = &LogSink{}
)

// New returns a logr.Logger with logr.LogSink implemented by Zerolog. Local level
// is mutated to allow max V-level if not set explicitly, so SetMaxV alone can control
// Zerolog's level. Use NewLogSink directly if local level mutation is undesirable.
func New(l *zerolog.Logger) Logger {
	if l.GetLevel() == zerolog.TraceLevel {
		ll := l.Level(minZerologLevel)
		l = &ll
	}
	ls := NewLogSink(l)
	return logr.New(ls)
}

// NewLogSink returns a logr.LogSink implemented by Zerolog.
func NewLogSink(l *zerolog.Logger) *LogSink {
	return &LogSink{l: l}
}

// SetMaxV updates Zerolog's global level. Default max V-level is 1 (DebugLevel).
// The range of max V-level is 0 through 129 inclusive, but higher than 2 V-level
// misses some features in Zerolog, e.g. Hooks and Sampling.
func SetMaxV(level int) {
	if level < 0 {
		level = 0
	}
	zlvl := 1 - level
	if zlvl < minZerologLevel {
		zlvl = minZerologLevel
	}
	zerolog.SetGlobalLevel(zerolog.Level(zlvl))
}

// Init receives runtime info about the logr library.
func (ls *LogSink) Init(ri logr.RuntimeInfo) {
	ls.depth = ri.CallDepth + 2
}

// Enabled tests whether this LogSink is enabled at the specified V-level.
func (ls *LogSink) Enabled(level int) bool {
	zlvl := zerolog.Level(1 - level)
	return zlvl >= ls.l.GetLevel() && zlvl >= zerolog.GlobalLevel()
}

// Info logs a non-error message at specified V-level with the given key/value pairs as context.
func (ls *LogSink) Info(level int, msg string, keysAndValues ...interface{}) {
	e := ls.l.WithLevel(zerolog.Level(1 - level))
	if VerbosityFieldName != "" {
		e.Int(VerbosityFieldName, level)
	}
	ls.msg(e, msg, keysAndValues)
}

// Error logs an error, with the given message and key/value pairs as context.
func (ls *LogSink) Error(err error, msg string, keysAndValues ...interface{}) {
	e := ls.l.Error().Err(err)
	ls.msg(e, msg, keysAndValues)
}

func (ls *LogSink) msg(e *zerolog.Event, msg string, keysAndValues []interface{}) {
	if e == nil {
		return
	}
	if ls.name != "" {
		e.Str(NameFieldName, ls.name)
	}
	if RenderArgsHook != nil {
		keysAndValues = RenderArgsHook(keysAndValues)
	}
	e = e.Fields(keysAndValues)
	e.CallerSkipFrame(ls.depth)
	e.Msg(msg)
}

// WithValues returns a new LogSink with additional key/value pairs.
func (ls LogSink) WithValues(keysAndValues ...interface{}) logr.LogSink {
	if RenderValuesHook != nil {
		keysAndValues = RenderValuesHook(keysAndValues)
	}
	l := ls.l.With().Fields(keysAndValues).Logger()
	ls.l = &l
	return &ls
}

// WithName returns a new LogSink with the specified name appended in NameFieldName.
// Name elements are separated by NameSeparator.
func (ls LogSink) WithName(name string) logr.LogSink {
	if ls.name != "" {
		ls.name += NameSeparator + name
	} else {
		ls.name = name
	}
	return &ls
}

// WithCallDepth returns a new LogSink that offsets the call stack by adding specified depths.
func (ls LogSink) WithCallDepth(depth int) logr.LogSink {
	ls.depth += depth
	return &ls
}

// GetUnderlying returns the zerolog.Logger underneath this logSink.
func (ls *LogSink) GetUnderlying() *zerolog.Logger {
	return ls.l
}

// DefaultRender supports logr.Marshaler and fmt.Stringer.
func DefaultRender(keysAndValues []interface{}) []interface{} {
	for i, n := 1, len(keysAndValues); i < n; i += 2 {
		value := keysAndValues[i]
		switch v := value.(type) {
		case logr.Marshaler:
			keysAndValues[i] = v.MarshalLog()
		case fmt.Stringer:
			keysAndValues[i] = v.String()
		}
	}
	return keysAndValues
}
