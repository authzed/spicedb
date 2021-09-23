package e2e

import (
	"context"
	"fmt"
	"os"
)

// TempDir returns a temporary directory that will be cleaned up on context done
func TempDir(ctx context.Context) string {
	dir := os.TempDir()

	// clean up tempdir on context done
	go func() {
		defer func() {
			if err := os.Remove(dir); err != nil {
				fmt.Println(err)
			}
		}()
		for {
			select {
			case <-ctx.Done():
			}
		}
	}()
	return dir
}
