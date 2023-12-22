package revisions

import (
	"time"

	"github.com/cockroachdb/apd"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var zeroHLC = HLCRevision(*apd.New(0, 0))

// HLCRevision is a revision that is a hybrid logical clock, stored as a decimal.
type HLCRevision apd.Decimal

// parseHLCRevisionString parses a string into a hybrid logical clock revision.
func parseHLCRevisionString(revisionStr string) (datastore.Revision, error) {
	parsed, _, err := apd.NewFromString(revisionStr)
	if err != nil {
		return datastore.NoRevision, err
	}
	if parsed == nil {
		return datastore.NoRevision, spiceerrors.MustBugf("got nil parsed HLC")
	}
	return HLCRevision(*parsed), nil
}

// HLCRevisionFromString parses a string into a hybrid logical clock revision.
func HLCRevisionFromString(revisionStr string) (HLCRevision, error) {
	parsed, _, err := apd.NewFromString(revisionStr)
	if err != nil {
		return zeroHLC, err
	}
	if parsed == nil {
		return zeroHLC, spiceerrors.MustBugf("got nil parsed HLC")
	}
	return HLCRevision(*parsed), nil
}

// NewForHLC creates a new revision for the given hybrid logical clock.
func NewForHLC(decimal decimal.Decimal) HLCRevision {
	return HLCRevision(*apd.NewWithBigInt(decimal.Coefficient(), decimal.Exponent()))
}

// NewHLCForTime creates a new revision for the given time.
func NewHLCForTime(time time.Time) HLCRevision {
	return HLCRevision(*apd.New(0, 0).SetInt64(time.UnixNano()))
}

func (hlc HLCRevision) Equal(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		rhs = HLCRevision(*apd.New(0, 0))
	}

	lhsD := apd.Decimal(hlc)
	lhsDP := &lhsD
	rhsD := apd.Decimal(rhs.(HLCRevision))
	return lhsDP.Cmp(&rhsD) == 0
}

func (hlc HLCRevision) GreaterThan(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		rhs = HLCRevision(*apd.New(0, 0))
	}

	lhsD := apd.Decimal(hlc)
	lhsDP := &lhsD
	rhsD := apd.Decimal(rhs.(HLCRevision))
	return lhsDP.Cmp(&rhsD) == 1
}

func (hlc HLCRevision) LessThan(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		return false
	}

	lhsD := apd.Decimal(hlc)
	lhsDP := &lhsD
	rhsD := apd.Decimal(rhs.(HLCRevision))
	return lhsDP.Cmp(&rhsD) == -1
}

func (hlc HLCRevision) String() string {
	d := apd.Decimal(hlc)
	dp := &d
	return dp.String()
}

func (hlc HLCRevision) TimestampNanoSec() int64 {
	d := apd.Decimal(hlc)
	dp := &d
	c := apd.BaseContext
	output := new(apd.Decimal)
	_, _ = c.Floor(output, dp)
	i, _ := output.Int64()
	return i
}

func (hlc HLCRevision) InexactFloat64() float64 {
	d := apd.Decimal(hlc)
	dp := &d
	f, _ := dp.Float64()
	return f
}

func (hlc HLCRevision) ConstructForTimestamp(timestamp int64) WithTimestampRevision {
	return HLCRevision(*(apd.New(0, 0).SetInt64(timestamp)))
}

var (
	_ datastore.Revision    = HLCRevision{}
	_ WithTimestampRevision = HLCRevision{}
)

// HLCKeyFunc is used to convert a simple HLC for use in maps.
func HLCKeyFunc(r HLCRevision) string {
	return r.String()
}

// HLCKeyLessThanFunc is used to compare keys created by the HLCKeyFunc.
func HLCKeyLessThanFunc(lhs, rhs string) bool {
	// Return the HLCs as strings to ensure precise is maintained.
	lp := mustParseDecimal(lhs)
	rp := mustParseDecimal(rhs)
	return lp.Cmp(rp) == -1
}

func mustParseDecimal(value string) *apd.Decimal {
	parsed, _, err := apd.NewFromString(value)
	if err != nil {
		panic("could not parse decimal")
	}
	return parsed
}
