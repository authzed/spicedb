package mysql

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestRevisionFromTransaction(t *testing.T) {
	tests := []struct {
		name string
		txID uint64
		want datastore.Revision
	}{
		{"0", 0, revisions.NewForTransactionID(0)},
		{"uint64 max", math.MaxUint64, revisions.NewForTransactionID(math.MaxUint64)},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			got := revisions.NewForTransactionID(tt.txID)
			require.True(tt.want.Equal(got))
		})
	}
}
