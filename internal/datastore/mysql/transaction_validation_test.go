package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

func TestTransactionBinderRejectsNilInputs(t *testing.T) {
	pending, err := (&transactionBinder{}).WithTransaction(context.Background(), nil, false, func(context.Context, datastore.ReadWriteTransaction) error {
		return nil
	})
	require.Nil(t, pending)
	require.ErrorContains(t, err, "transaction and callback are required")
}
