package mysql

import (
	"math"
	"math/big"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

func Test_revisionFromTransaction(t *testing.T) {
	tests := []struct {
		name string
		txID uint64
		want datastore.Revision
	}{
		{"0", 0, decimal.NewFromInt(0)},
		{"uint64 max", math.MaxUint64, decimal.NewFromBigInt(new(big.Int).SetUint64(math.MaxUint64), 0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			got := revisionFromTransaction(tt.txID)
			require.True(tt.want.Equal(got))
		})
	}
}

func Test_transactionFromRevision(t *testing.T) {
	tests := []struct {
		name     string
		revision datastore.Revision
		want     uint64
	}{
		{"0", decimal.NewFromInt(0), 0},
		{"uint64 max", decimal.NewFromBigInt(new(big.Int).SetUint64(math.MaxUint64), 0), math.MaxUint64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			got := transactionFromRevision(tt.revision)
			require.Equal(tt.want, got)
		})
	}
}
