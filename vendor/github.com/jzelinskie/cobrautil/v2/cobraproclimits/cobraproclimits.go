package cobraproclimits

import (
	"log/slog"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/rs/zerolog"
	slogzerolog "github.com/samber/slog-zerolog/v2"
	"github.com/spf13/cobra"
	"go.uber.org/automaxprocs/maxprocs"
)

// NOTE: Both of these assume that there is already a zerolog instance configured for the process
// by the time this RunE is invoked.

// SetLimitsRunE wraps the RunFunc with setup logic for memory limits
// for the go process. It requests 90% of the memory available and respects
// kubernetes cgroup limits.
func SetMemLimitRunE(options ...memlimit.Option) cobrautil.CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		// Need to invert the slog => zerolog map so that we can get the correct
		// slog loglevel for memlimit logging
		logLevelMap := make(map[zerolog.Level]slog.Level, len(slogzerolog.LogLevels))
		for sLevel, zLevel := range slogzerolog.LogLevels {
			logLevelMap[zLevel] = sLevel
		}

		logger := zerolog.DefaultContextLogger

		logLevel := logLevelMap[logger.GetLevel()]

		slogger := slog.New(slogzerolog.Option{Level: logLevel, Logger: logger}.NewZerologHandler())

		defaults := []memlimit.Option{
			memlimit.WithProvider(
				memlimit.ApplyFallback(
					memlimit.FromCgroup,
					memlimit.FromSystem,
				),
			),
			memlimit.WithLogger(slogger),
		}
		_, _ = memlimit.SetGoMemLimitWithOpts(
			append(defaults, options...)...,
		)

		return nil
	}
}

// SetProcLimitRunE wraps the RunFunc with setup logic for maxproc
// limits for the go process. It requests all of the available CPU quota.
func SetProcLimitRunE() cobrautil.CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		logger := zerolog.DefaultContextLogger

		_, err := maxprocs.Set(maxprocs.Logger(zerolog.DefaultContextLogger.Printf))
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to set maxprocs")
		}
		return nil
	}
}
