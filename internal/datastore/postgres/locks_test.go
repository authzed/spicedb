package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReleaseLockNotHeldSentinel locks in the contract both the GC and the
// revision heartbeat release paths rely on: releaseLock wraps ErrLockNotHeld,
// so callers can tell the benign "session lost the lock" case apart from a
// genuine release failure via errors.Is. See issue #2847.
func TestReleaseLockNotHeldSentinel(t *testing.T) {
	wrapped := fmt.Errorf("lock %d: %w", gcRunLock, ErrLockNotHeld)
	require.ErrorIs(t, wrapped, ErrLockNotHeld)

	require.NotErrorIs(t, errors.New("some other failure"), ErrLockNotHeld)
}
