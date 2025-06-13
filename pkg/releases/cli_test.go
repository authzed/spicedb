package releases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestRegisterFlags(t *testing.T) {
	flagset := pflag.NewFlagSet("test", pflag.ContinueOnError)
	RegisterFlags(flagset)

	flag := flagset.Lookup("skip-release-check")
	require.NotNil(t, flag)
	require.Equal(t, "false", flag.DefValue)
	require.Equal(t, "if true, skips checking for new SpiceDB releases", flag.Usage)
}

func TestCheckAndLogVersionUpdate(t *testing.T) {
	ctx := t.Context()

	t.Run("version check error", func(t *testing.T) {
		err := mustCheckAndLogVersionUpdate(ctx, func() (string, error) {
			return "", errors.New("version error")
		}, func(ctx context.Context) (*Release, error) {
			return &Release{Version: "v1.0.0"}, nil
		})
		require.NoError(t, err)
	})

	t.Run("unreleased version", func(t *testing.T) {
		err := mustCheckAndLogVersionUpdate(ctx, func() (string, error) {
			return "dev-123", nil
		}, func(ctx context.Context) (*Release, error) {
			return &Release{Version: "v1.0.0"}, nil
		})
		require.NoError(t, err)
	})

	t.Run("update available", func(t *testing.T) {
		err := mustCheckAndLogVersionUpdate(ctx, func() (string, error) {
			return "v1.0.0", nil
		}, func(ctx context.Context) (*Release, error) {
			return &Release{
				Version:     "v1.1.0",
				ViewURL:     "https://github.com/authzed/spicedb/releases/tag/v1.1.0",
				PublishedAt: time.Now(),
			}, nil
		})
		require.NoError(t, err)
	})

	t.Run("update available but release fetch error", func(t *testing.T) {
		err := mustCheckAndLogVersionUpdate(ctx, func() (string, error) {
			return "v1.0.0", nil
		}, func(ctx context.Context) (*Release, error) {
			return nil, errors.New("failed to fetch release")
		})
		require.NoError(t, err)
	})

	t.Run("up to date", func(t *testing.T) {
		err := mustCheckAndLogVersionUpdate(ctx, func() (string, error) {
			return "v1.0.0", nil
		}, func(ctx context.Context) (*Release, error) {
			return &Release{
				Version:     "v1.0.0",
				ViewURL:     "https://github.com/authzed/spicedb/releases/tag/v1.0.0",
				PublishedAt: time.Now(),
			}, nil
		})
		require.NoError(t, err)
	})

	t.Run("up to date but release fetch error", func(t *testing.T) {
		err := mustCheckAndLogVersionUpdate(ctx, func() (string, error) {
			return "v1.0.0", nil
		}, func(ctx context.Context) (*Release, error) {
			return nil, errors.New("failed to fetch release")
		})
		require.NoError(t, err)
	})

	t.Run("unknown state", func(t *testing.T) {
		err := mustCheckAndLogVersionUpdate(ctx, func() (string, error) {
			return "v1.0.0", nil
		}, func(ctx context.Context) (*Release, error) {
			return &Release{
				Version:     "invalid-version",
				ViewURL:     "https://github.com/authzed/spicedb/releases/tag/invalid-version",
				PublishedAt: time.Now(),
			}, nil
		})
		require.NoError(t, err)
	})

	t.Run("unknown state but release fetch error", func(t *testing.T) {
		err := mustCheckAndLogVersionUpdate(ctx, func() (string, error) {
			return "v1.0.0", nil
		}, func(ctx context.Context) (*Release, error) {
			return nil, errors.New("failed to fetch release")
		})
		require.NoError(t, err)
	})

	t.Run("release fetch error", func(t *testing.T) {
		err := mustCheckAndLogVersionUpdate(ctx, func() (string, error) {
			return "v1.0.0", nil
		}, func(ctx context.Context) (*Release, error) {
			return nil, errors.New("release error")
		})
		require.NoError(t, err)
	})
}
