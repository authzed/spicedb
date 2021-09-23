package e2e

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	shutdownSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}
	signalCtx       context.Context
	cancel          context.CancelFunc
	once            sync.Once
)

// Context returns a Context registered to close on SIGTERM and SIGINT.
// If a second signal is caught, the program is terminated with exit code 1.
func Context() context.Context {
	once.Do(func() {
		c := make(chan os.Signal, 2)
		signal.Notify(c, shutdownSignals...)
		signalCtx, cancel = context.WithCancel(context.Background())
		go func() {
			<-c
			cancel()

			select {
			case <-signalCtx.Done():
			case <-c:
				fmt.Println("shutting down")
				os.Exit(1)
			}
		}()
	})

	return signalCtx
}
