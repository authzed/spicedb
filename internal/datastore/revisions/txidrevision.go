package revisions

import (
	"fmt"
	"strconv"

	"github.com/authzed/spicedb/pkg/datastore"
)

// TransactionIDRevision is a revision that is a transaction ID.
type TransactionIDRevision uint64

// NewForTransactionID creates a new revision for the given transaction ID.
func NewForTransactionID(transactionID uint64) TransactionIDRevision {
	return TransactionIDRevision(transactionID)
}

// parseTransactionIDRevisionString parses a string into a transaction ID revision.
func parseTransactionIDRevisionString(revisionStr string) (rev datastore.Revision, err error) {
	parsed, err := strconv.ParseUint(revisionStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid integer revision: %w", err)
	}

	return TransactionIDRevision(parsed), nil
}

func (ir TransactionIDRevision) Equal(other datastore.Revision) bool {
	return uint64(ir) == uint64(other.(TransactionIDRevision))
}

func (ir TransactionIDRevision) GreaterThan(other datastore.Revision) bool {
	return uint64(ir) > uint64(other.(TransactionIDRevision))
}

func (ir TransactionIDRevision) LessThan(other datastore.Revision) bool {
	return uint64(ir) < uint64(other.(TransactionIDRevision))
}

func (TransactionIDRevision) MarshalBinary() (data []byte, err error) {
	panic("unimplemented")
}

func (ir TransactionIDRevision) TransactionID() uint64 {
	return uint64(ir)
}

func (ir TransactionIDRevision) String() string {
	return strconv.FormatInt(int64(ir), 10)
}

func (ir TransactionIDRevision) WithInexactFloat64() float64 {
	return float64(ir)
}

var _ datastore.Revision = TransactionIDRevision(0)

// TransactionIDKeyFunc is used to create keys for transaction IDs.
func TransactionIDKeyFunc(r TransactionIDRevision) uint64 {
	return uint64(r)
}

// TransactionIDKeyLessThanFunc is used to create keys for transaction IDs.
func TransactionIDKeyLessThanFunc(l, r uint64) bool {
	return l < r
}
