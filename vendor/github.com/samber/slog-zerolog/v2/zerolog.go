package slogzerolog

import (
	"log/slog"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var LogLevels = map[slog.Level]zerolog.Level{
	slog.LevelDebug: zerolog.DebugLevel,
	slog.LevelInfo:  zerolog.InfoLevel,
	slog.LevelWarn:  zerolog.WarnLevel,
	slog.LevelError: zerolog.ErrorLevel,
}

var reverseLogLevels map[zerolog.Level]slog.Level

func init() {
	reverseLogLevels = map[zerolog.Level]slog.Level{}
	for level, z := range LogLevels {
		reverseLogLevels[z] = level
	}
}

// ZeroLogLeveler can be used for Option.Level (implements slog.Leveler).
// If no Logger is provided, the global zerolog.Logger is used.
type ZeroLogLeveler struct {
	// optional: zerolog logger (default: log.Logger)
	Logger *zerolog.Logger
}

var _ slog.Leveler = ZeroLogLeveler{}

func (z ZeroLogLeveler) Level() slog.Level {
	var logger = log.Logger
	if z.Logger != nil {
		logger = *z.Logger
	}
	zeroLogLevel := logger.GetLevel()
	level, ok := reverseLogLevels[zeroLogLevel]
	if !ok {
		switch zeroLogLevel {
		case zerolog.PanicLevel, zerolog.FatalLevel:
			return slog.LevelError
		case zerolog.NoLevel:
			return slog.LevelInfo
		case zerolog.Disabled:
			return slog.LevelDebug - 1
		default:
			if zeroLogLevel < zerolog.DebugLevel {
				return slog.Level(int(slog.LevelDebug) + int(zeroLogLevel))
			}
			return slog.LevelInfo
		}
	}
	return level
}
