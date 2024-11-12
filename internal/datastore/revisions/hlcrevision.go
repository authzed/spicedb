package revisions

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ccoveille/go-safecast"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var zeroHLC = HLCRevision{}

// NOTE: This *must* match the length defined in CRDB or the implementation below will break.
const logicalClockLength = 10

var logicalClockOffset = uint32(math.Pow10(logicalClockLength + 1))

// HLCRevision is a revision that is a hybrid logical clock, stored as two integers.
// The first integer is the timestamp in nanoseconds, and the second integer is the
// logical clock defined as 11 digits, with the first digit being ignored to ensure
// precision of the given logical clock.
type HLCRevision struct {
	time         int64
	logicalclock uint32
}

// parseHLCRevisionString parses a string into a hybrid logical clock revision.
func parseHLCRevisionString(revisionStr string) (datastore.Revision, error) {
	pieces := strings.Split(revisionStr, ".")
	if len(pieces) == 1 {
		// If there is no decimal point, assume the revision is a timestamp.
		timestamp, err := strconv.ParseInt(pieces[0], 10, 64)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf("invalid revision string: %q", revisionStr)
		}
		return HLCRevision{timestamp, logicalClockOffset}, nil
	}

	if len(pieces) != 2 {
		return datastore.NoRevision, fmt.Errorf("invalid revision string: %q", revisionStr)
	}

	timestamp, err := strconv.ParseInt(pieces[0], 10, 64)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf("invalid revision string: %q", revisionStr)
	}

	if len(pieces[1]) > logicalClockLength {
		return datastore.NoRevision, spiceerrors.MustBugf("invalid revision string due to unexpected logical clock size (%d): %q", len(pieces[1]), revisionStr)
	}

	paddedLogicalClockStr := pieces[1] + strings.Repeat("0", logicalClockLength-len(pieces[1]))
	logicalclock, err := strconv.ParseUint(paddedLogicalClockStr, 10, 64)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf("invalid revision string: %q", revisionStr)
	}

	if logicalclock > math.MaxUint32 {
		return datastore.NoRevision, spiceerrors.MustBugf("received logical lock that exceeds MaxUint32 (%d > %d): revision %q", logicalclock, math.MaxUint32, revisionStr)
	}

	uintLogicalClock, err := safecast.ToUint32(logicalclock)
	if err != nil {
		return datastore.NoRevision, spiceerrors.MustBugf("could not cast logicalclock to uint32: %v", err)
	}

	return HLCRevision{timestamp, uintLogicalClock + logicalClockOffset}, nil
}

// HLCRevisionFromString parses a string into a hybrid logical clock revision.
func HLCRevisionFromString(revisionStr string) (HLCRevision, error) {
	rev, err := parseHLCRevisionString(revisionStr)
	if err != nil {
		return zeroHLC, err
	}

	return rev.(HLCRevision), nil
}

// NewForHLC creates a new revision for the given hybrid logical clock.
func NewForHLC(decimal decimal.Decimal) (HLCRevision, error) {
	rev, err := HLCRevisionFromString(decimal.String())
	if err != nil {
		return zeroHLC, fmt.Errorf("invalid HLC decimal: %v (%s) => %w", decimal, decimal.String(), err)
	}

	return rev, nil
}

// NewHLCForTime creates a new revision for the given time.
func NewHLCForTime(time time.Time) HLCRevision {
	return HLCRevision{time.UnixNano(), logicalClockOffset}
}

func (hlc HLCRevision) ByteSortable() bool {
	return true
}

func (hlc HLCRevision) Equal(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		rhs = zeroHLC
	}

	rhsHLC := rhs.(HLCRevision)
	return hlc.time == rhsHLC.time && hlc.logicalclock == rhsHLC.logicalclock
}

func (hlc HLCRevision) GreaterThan(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		rhs = zeroHLC
	}

	rhsHLC := rhs.(HLCRevision)
	return hlc.time > rhsHLC.time || (hlc.time == rhsHLC.time && hlc.logicalclock > rhsHLC.logicalclock)
}

func (hlc HLCRevision) LessThan(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		rhs = zeroHLC
	}

	rhsHLC := rhs.(HLCRevision)
	return hlc.time < rhsHLC.time || (hlc.time == rhsHLC.time && hlc.logicalclock < rhsHLC.logicalclock)
}

func (hlc HLCRevision) String() string {
	logicalClockString := strconv.FormatInt(int64(hlc.logicalclock)-int64(logicalClockOffset), 10)
	return strconv.FormatInt(hlc.time, 10) + "." + strings.Repeat("0", logicalClockLength-len(logicalClockString)) + logicalClockString
}

func (hlc HLCRevision) TimestampNanoSec() int64 {
	return hlc.time
}

func (hlc HLCRevision) InexactFloat64() float64 {
	return float64(hlc.time) + float64(hlc.logicalclock-logicalClockOffset)/math.Pow10(logicalClockLength)
}

func (hlc HLCRevision) ConstructForTimestamp(timestamp int64) WithTimestampRevision {
	return HLCRevision{timestamp, logicalClockOffset}
}

func (hlc HLCRevision) AsDecimal() (decimal.Decimal, error) {
	return decimal.NewFromString(hlc.String())
}

var (
	_ datastore.Revision    = HLCRevision{}
	_ WithTimestampRevision = HLCRevision{}
)

// HLCKeyFunc is used to convert a simple HLC for use in maps.
func HLCKeyFunc(r HLCRevision) HLCRevision {
	return r
}

// HLCKeyLessThanFunc is used to compare keys created by the HLCKeyFunc.
func HLCKeyLessThanFunc(lhs, rhs HLCRevision) bool {
	return lhs.time < rhs.time || (lhs.time == rhs.time && lhs.logicalclock < rhs.logicalclock)
}
