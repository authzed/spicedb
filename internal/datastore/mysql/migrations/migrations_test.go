//go:build ci
// +build ci

package migrations

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMySQLMigrationsWithUnsupportedPrefix(t *testing.T) {
	req := require.New(t)
	err := registerMigration("888", "", func(ctx context.Context, d Wrapper) error {
		return nil
	}, noTxMigration)
	req.Error(err)
}
