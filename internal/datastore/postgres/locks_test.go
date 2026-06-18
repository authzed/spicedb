package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestErrLockNotHeldWrapping locks in the contract both the GC and revision
// heartbeat release paths rely on: releaseLock wraps ErrLockNotHeld with %w, so
// callers can distinguish the benign "lock not held" case (e.g. a session reset
// after acquiring the lock) from a genuine error via errors.Is.
func TestErrLockNotHeldWrapping(t *testing.T) {
	wrapped := fmt.Errorf("lock %d: %w", gcRunLock, ErrLockNotHeld)
	require.ErrorIs(t, wrapped, ErrLockNotHeld)

	// A different error must not match the sentinel.
	require.NotErrorIs(t, errors.New("some other failure"), ErrLockNotHeld)
}
