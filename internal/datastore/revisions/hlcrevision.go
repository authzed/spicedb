package revisions

import (
	"time"

	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
)

// HLCRevision is a revision that is a hybrid logical clock, stored as a decimal.
type HLCRevision struct {
	decimal decimal.Decimal
}

// parseHLCRevisionString parses a string into a hybrid logical clock revision.
func parseHLCRevisionString(revisionStr string) (datastore.Revision, error) {
	parsed, err := decimal.NewFromString(revisionStr)
	if err != nil {
		return datastore.NoRevision, err
	}
	return HLCRevision{parsed}, nil
}

// HLCRevisionFromString parses a string into a hybrid logical clock revision.
func HLCRevisionFromString(revisionStr string) (HLCRevision, error) {
	parsed, err := decimal.NewFromString(revisionStr)
	if err != nil {
		return HLCRevision{decimal.Zero}, err
	}
	return HLCRevision{parsed}, nil
}

// NewForHLC creates a new revision for the given hybrid logical clock.
func NewForHLC(decimal decimal.Decimal) HLCRevision {
	return HLCRevision{decimal}
}

// NewHLCForTime creates a new revision for the given time.
func NewHLCForTime(time time.Time) HLCRevision {
	return HLCRevision{decimal.NewFromInt(time.UnixNano())}
}

func (hlc HLCRevision) Equal(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		return false
	}

	rhsD := rhs.(HLCRevision)
	return hlc.decimal.Equal(rhsD.decimal)
}

func (hlc HLCRevision) GreaterThan(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		rhs = HLCRevision{decimal.Zero}
	}

	rhsD := rhs.(HLCRevision)
	return hlc.decimal.GreaterThan(rhsD.decimal)
}

func (hlc HLCRevision) LessThan(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		rhs = HLCRevision{decimal.Zero}
	}

	rhsD := rhs.(HLCRevision)
	return hlc.decimal.LessThan(rhsD.decimal)
}

func (hlc HLCRevision) String() string {
	return hlc.decimal.String()
}

func (hlc HLCRevision) TimestampNanoSec() int64 {
	return hlc.decimal.IntPart()
}

func (hlc HLCRevision) InexactFloat64() float64 {
	return float64(hlc.decimal.IntPart())
}

func (hlc HLCRevision) ConstructForTimestamp(timestamp int64) WithTimestampRevision {
	return HLCRevision{decimal.NewFromInt(timestamp)}
}

var _ datastore.Revision = HLCRevision{}
var _ WithTimestampRevision = HLCRevision{}

// HLCKeyFunc is used to convert a simple HLC to an int64 for use in maps.
func HLCKeyFunc(r HLCRevision) int64 {
	return r.TimestampNanoSec()
}

// HLCKeyLessThanFunc is used to compare keys created by the HLCKeyFunc.
func HLCKeyLessThanFunc(lhs, rhs int64) bool {
	return lhs < rhs
}
