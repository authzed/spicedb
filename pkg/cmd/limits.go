package cmd

import (
	"log/slog"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/rs/zerolog"
	slogzerolog "github.com/samber/slog-zerolog/v2"
	"github.com/spf13/cobra"
	"go.uber.org/automaxprocs/maxprocs"

	log "github.com/authzed/spicedb/internal/logging"
)

// SetLimitsRunE wraps the RunFunc with setup logic for memory limits and maxprocs
// limits for the go process. Implementing it here means that we can use already-
// established loggers and wrap only those commands that require it.
func SetLimitsRunE() cobrautil.CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		// Need to invert the slog => zerolog map so that we can get the correct
		// slog loglevel for memlimit logging
		logLevelMap := make(map[zerolog.Level]slog.Level, len(slogzerolog.LogLevels))
		for sLevel, zLevel := range slogzerolog.LogLevels {
			logLevelMap[zLevel] = sLevel
		}

		logLevel := logLevelMap[zerolog.DefaultContextLogger.GetLevel()]

		slogger := slog.New(slogzerolog.Option{Level: logLevel, Logger: zerolog.DefaultContextLogger}.NewZerologHandler())

		undo, err := maxprocs.Set(maxprocs.Logger(zerolog.DefaultContextLogger.Printf))
		if err != nil {
			log.Fatal().Err(err).Msg("failed to set maxprocs")
		}
		defer undo()

		_, _ = memlimit.SetGoMemLimitWithOpts(
			memlimit.WithRatio(0.9),
			memlimit.WithProvider(
				memlimit.ApplyFallback(
					memlimit.FromCgroup,
					memlimit.FromSystem,
				),
			),
			memlimit.WithLogger(slogger),
		)

		return nil
	}
}
