package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/authzed/spicedb/internal/logging"
)

// SignalContextWithGracePeriod creates a new context that will be cancelled
// when an interrupt/SIGTERM signal is received and the provided grace period
// subsequently finishes.
func SignalContextWithGracePeriod(ctx context.Context, gracePeriod time.Duration) context.Context {
	newCtx, cancelfn := context.WithCancel(ctx)
	go func() {
		signalctx, _ := signal.NotifyContext(newCtx, os.Interrupt, syscall.SIGTERM)
		<-signalctx.Done()
		log.Info().Msg("received interrupt")

		if gracePeriod > 0 {
			interruptGrace, _ := signal.NotifyContext(context.Background(), os.Interrupt)
			graceTimer := time.NewTimer(gracePeriod)

			log.Info().Stringer("timeout", gracePeriod).Msg("starting shutdown grace period")

			select {
			case <-graceTimer.C:
			case <-interruptGrace.Done():
				log.Warn().Msg("interrupted shutdown grace period")
			}
		}
		log.Info().Msg("shutting down")
		cancelfn()
	}()

	return newCtx
}
