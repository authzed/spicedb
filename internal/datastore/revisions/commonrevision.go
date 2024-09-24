package revisions

import (
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// RevisionKind is an enum of the different kinds of revisions that can be used.
type RevisionKind string

const (
	// Timestamp is a revision that is a timestamp.
	Timestamp RevisionKind = "timestamp"

	// TransactionID is a revision that is a transaction ID.
	TransactionID = "txid"

	// HybridLogicalClock is a revision that is a hybrid logical clock.
	HybridLogicalClock = "hlc"
)

// ParsingFunc is a function that can parse a string into a revision.
type ParsingFunc func(revisionStr string) (rev datastore.Revision, err error)

// RevisionParser returns a ParsingFunc for the given RevisionKind.
func RevisionParser(kind RevisionKind) ParsingFunc {
	switch kind {
	case TransactionID:
		return parseTransactionIDRevisionString

	case Timestamp:
		return parseTimestampRevisionString

	case HybridLogicalClock:
		return parseHLCRevisionString

	default:
		return func(revisionStr string) (rev datastore.Revision, err error) {
			return nil, spiceerrors.MustBugf("unknown revision kind: %v", kind)
		}
	}
}

// CommonDecoder is a revision decoder that can decode revisions of a given kind.
type CommonDecoder struct {
	Kind RevisionKind
}

func (cd CommonDecoder) RevisionFromString(s string) (datastore.Revision, error) {
	switch cd.Kind {
	case TransactionID:
		return parseTransactionIDRevisionString(s)

	case Timestamp:
		return parseTimestampRevisionString(s)

	case HybridLogicalClock:
		return parseHLCRevisionString(s)

	default:
		return nil, spiceerrors.MustBugf("unknown revision kind in decoder: %v", cd.Kind)
	}
}

// WithInexactFloat64 is an interface that can be implemented by a revision to
// provide an inexact float64 representation of the revision.
type WithInexactFloat64 interface {
	// InexactFloat64 returns a float64 that is an inexact representation of the
	// revision.
	InexactFloat64() float64
}

// WithTimestampRevision is an interface that can be implemented by a revision to
// provide a timestamp.
type WithTimestampRevision interface {
	datastore.Revision
	TimestampNanoSec() int64
	ConstructForTimestamp(timestampNanoSec int64) WithTimestampRevision
}
