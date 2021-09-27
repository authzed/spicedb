package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// File returns a writer to a new file that will be closed when the context is
// cancelled.
func File(ctx context.Context, name string) (*os.File, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}

	// close file on context done
	go func() {
		<-ctx.Done()
		if err := f.Close(); err != nil {
			fmt.Println(err)
		}
	}()
	return f, nil
}

// MustFile fails the test if it can't create a file called name.
func MustFile(ctx context.Context, t *testing.T, name string) *os.File {
	f, err := File(ctx, name)
	require.NoError(t, err)
	return f
}
