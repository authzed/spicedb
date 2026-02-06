package grpchelpers

import (
	"errors"
	"io"
	"time"

	log "github.com/authzed/spicedb/internal/logging"
)

type CloseableStack struct {
	Closers     []func() error
	graceperiod time.Duration
}

func NewCloseableStack(graceperiod time.Duration) *CloseableStack {
	return &CloseableStack{
		graceperiod: graceperiod,
	}
}

func (c *CloseableStack) AddWithError(closer func() error) {
	c.Closers = append(c.Closers, closer)
}

func (c *CloseableStack) AddCloser(closer io.Closer) {
	if closer != nil {
		c.Closers = append(c.Closers, closer.Close)
	}
}

func (c *CloseableStack) AddCloserWithGracePeriod(closer, forceCloser func()) {
	if closer != nil {
		c.Closers = append(c.Closers, func() error {
			shutdownGraceful := make(chan struct{})
			go func() {
				log.Info().Msg("gracefully shutting down")
				closer()
				close(shutdownGraceful)
			}()

			select {
			case <-shutdownGraceful:
				log.Info().Msg("graceful shutdown complete")
			case <-time.After(c.graceperiod):
				log.Info().Msg("grace period exceeded, forcefully shutting down")
				forceCloser()
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
