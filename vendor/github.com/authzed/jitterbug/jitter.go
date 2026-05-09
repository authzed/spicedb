package jitterbug

import (
	"context"
	"time"
)

// Jitter can compute a jitter
type Jitter interface {
	// Jitter consumes an interval from a ticker and returns the final, jittered
	// duration.
	Jitter(time.Duration) time.Duration
}

// Ticker behaves like time.Ticker
type Ticker struct {
	C      <-chan time.Time
	ctx    context.Context
	cancel context.CancelFunc
	Jitter
	Interval time.Duration
}

// New Ticker with the base interval d and the jitter source j.
func New(d time.Duration, j Jitter) (t *Ticker) {
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan time.Time)
	t = &Ticker{
		C:        c,
		ctx:      ctx,
		cancel:   cancel,
		Interval: d,
		Jitter:   j,
	}
	go t.loop(c)
	return
}

// Stop the Ticker. It's safe to call many times.
func (t *Ticker) Stop() {
	t.cancel()
}

// loop runs forever until Stop is called.
// it sends the current time to channel "c".
func (t *Ticker) loop(c chan<- time.Time) {
	defer close(c)

	for {
		timer := time.NewTimer(t.calcDelay())

		select {
		case <-t.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:

		}

		// Timer expired, proceed to send tick

		select {
		case <-t.ctx.Done():
			return
		case c <- time.Now():
		default: // there may be nobody ready to recv
		}
	}
}

func (t *Ticker) calcDelay() time.Duration { return t.Jitter.Jitter(t.Interval) }
