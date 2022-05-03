//go:build githubapi
// +build githubapi

package releases

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReleases(t *testing.T) {
	release, err := GetLatestRelease(context.Background())
	require.NoError(t, err)
	require.NotNil(t, release)
}
