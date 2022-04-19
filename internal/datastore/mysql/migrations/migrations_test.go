//go:build ci
// +build ci

package migrations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMySQLMigrationsWithUnsupportedPrefix(t *testing.T) {
	req := require.New(t)
	err := registerMigration("888", "", struct{}{})
	req.Error(err)
}
