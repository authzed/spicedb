package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeleteExpiredRelsFailsWhenClosed(t *testing.T) {
	// A closed collector must fail the isClosed guard (which panics via
	// MustBugf under test) before touching the datastore or acquiring the
	// cluster-wide GC lock; mds is nil here, so any datastore access would
	// surface as a nil-pointer panic instead.
	mcc := &mysqlGarbageCollector{mds: nil, isClosed: true}

	require.PanicsWithValue(t, "mysqlGarbageCollector is closed", func() {
		_, _ = mcc.DeleteExpiredRels(t.Context())
	})
}
