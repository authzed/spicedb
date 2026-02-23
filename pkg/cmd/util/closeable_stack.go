package util

import (
	"errors"
	"io"
	"time"

	log "github.com/authzed/spicedb/internal/logging"
)

type CloseableStack struct {
	Closers []func() error
}

func (c *CloseableStack) AddWithError(closer func() error) {
	c.Closers = append(c.Closers, closer)
}

func (c *CloseableStack) AddCloser(closer io.Closer) {
	if closer != nil {
		c.Closers = append(c.Closers, closer.Close)
	}
}

// AddCloserWithGracePeriod adds a closer function that invokes gracefulCloser immediately
// and waits gracePeriod for it to finish. If gracePeriod elapses, it invokes forceCloser.
// If gracePeriod is zero, it does NOT invoke forceCloser and waits indefinitely.
func (c *CloseableStack) AddCloserWithGracePeriod(name string, gracePeriod time.Duration, gracefulCloser, forceCloser func()) {
	if gracefulCloser != nil {
		c.Closers = append(c.Closers, func() error {
			log.Info().Str("service", name).Dur("grace-period", gracePeriod).Msg("gracefully shutting down")

			shutdownGraceful := make(chan struct{})
			go func() {
				gracefulCloser()
				close(shutdownGraceful)
			}()

			if gracePeriod == 0 {
				<-shutdownGraceful
				log.Info().Str("service", name).Msg("graceful shutdown complete")
			} else {
				select {
				case <-shutdownGraceful:
					log.Info().Str("service", name).Msg("graceful shutdown complete")
				case <-time.After(gracePeriod):
					log.Info().Str("service", name).Msg("grace period exceeded, forcefully shutting down")
					forceCloser()
				}
			}
			return nil
		})
	}
}

func (c *CloseableStack) AddWithoutError(closer func()) {
	c.Closers = append(c.Closers, func() error {
		closer()
		return nil
	})
}

// Close calls the functions in reverse order how it's expected in deferred funcs
func (c *CloseableStack) Close() error {
	var err error
	for i := len(c.Closers) - 1; i >= 0; i-- {
		if closerErr := c.Closers[i](); closerErr != nil {
			err = errors.Join(err, closerErr)
		}
	}
	return err
}

func (c *CloseableStack) CloseIfError(err error) error {
	if err != nil {
		return c.Close()
	}
	return nil
}
