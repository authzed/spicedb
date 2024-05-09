package closer

import (
	"io"

	"github.com/hashicorp/go-multierror"
)

type Stack struct {
	closers []func() error
}

func (c *Stack) AddWithError(closer func() error) {
	c.closers = append(c.closers, closer)
}

func (c *Stack) AddCloser(closer io.Closer) {
	if closer != nil {
		c.closers = append(c.closers, closer.Close)
	}
}

func (c *Stack) AddWithoutError(closer func()) {
	c.closers = append(c.closers, func() error {
		closer()
		return nil
	})
}

func (c *Stack) Close() error {
	var err error
	// closer in reverse order how it's expected in deferred funcs
	for i := len(c.closers) - 1; i >= 0; i-- {
		if closerErr := c.closers[i](); closerErr != nil {
			err = multierror.Append(err, closerErr)
		}
	}
	return err
}

func (c *Stack) CloseIfError(err error) error {
	if err != nil {
		return c.Close()
	}
	return nil
}
