package revisions

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestZeroTransactionIDRevision(t *testing.T) {
	require.False(t, TransactionIDRevision(0).LessThan(zeroTransactionIDRevision))
	require.True(t, TransactionIDRevision(0).Equal(zeroTransactionIDRevision))
	require.False(t, TransactionIDRevision(0).GreaterThan(zeroTransactionIDRevision))

	require.False(t, TransactionIDRevision(1).LessThan(zeroTransactionIDRevision))
	require.False(t, TransactionIDRevision(1).Equal(zeroTransactionIDRevision))
	require.True(t, TransactionIDRevision(1).GreaterThan(zeroTransactionIDRevision))
}
