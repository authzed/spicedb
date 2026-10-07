package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

func TestTransactionBinderRejectsNilInputs(t *testing.T) {
	var tx pgx.Tx
	pending, err := (&transactionBinder{}).WithTransaction(context.Background(), tx, ExternalTransactionOptions{}, func(context.Context, datastore.ReadWriteTransaction) error {
		return nil
	})
	require.Nil(t, pending)
	require.ErrorContains(t, err, "transaction and callback are required")
}
