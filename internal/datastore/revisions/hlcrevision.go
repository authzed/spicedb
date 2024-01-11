package revisions

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var zeroHLC = HLCRevision([2]int64{0, 0})

// NOTE: This *must* match the length defined in CRDB or the implementation below will break.
const logicalClockLength = 10

var logicalClockOffset = int64(math.Pow10(logicalClockLength + 1))

// HLCRevision is a revision that is a hybrid logical clock, stored as two integers.
// The first integer is the timestamp in nanoseconds, and the second integer is the
// logical clock defined as 11 digits, with the first digit being ignored to ensure
// precision of the given logical clock.
type HLCRevision [2]int64

// parseHLCRevisionString parses a string into a hybrid logical clock revision.
func parseHLCRevisionString(revisionStr string) (datastore.Revision, error) {
	pieces := strings.Split(revisionStr, ".")
	if len(pieces) == 1 {
		// If there is no decimal point, assume the revision is a timestamp.
		timestamp, err := strconv.ParseInt(pieces[0], 10, 64)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf("invalid revision string: %q", revisionStr)
		}
		return HLCRevision([2]int64{timestamp, 0}), nil
	}

	if len(pieces) != 2 {
		return datastore.NoRevision, fmt.Errorf("invalid revision string: %q", revisionStr)
	}

	timestamp, err := strconv.ParseInt(pieces[0], 10, 64)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf("invalid revision string: %q", revisionStr)
	}

	logicalclock, err := strconv.ParseInt(pieces[1], 10, 64)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf("invalid revision string: %q", revisionStr)
	}

	if len(pieces[1]) != logicalClockLength {
		return datastore.NoRevision, spiceerrors.MustBugf("invalid revision string due to unexpected logical clock size (%d): %q", len(pieces[1]), revisionStr)
	}

	return HLCRevision([2]int64{timestamp, logicalclock + logicalClockOffset}), nil
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
func NewForHLC(decimal decimal.Decimal) HLCRevision {
	rev, _ := HLCRevisionFromString(decimal.String())
	return rev
}

// NewHLCForTime creates a new revision for the given time.
func NewHLCForTime(time time.Time) HLCRevision {
	return HLCRevision([2]int64{time.UnixNano(), 0})
}

func (hlc HLCRevision) Equal(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		rhs = zeroHLC
	}

	rhsHLC := rhs.(HLCRevision)
	return hlc[0] == rhsHLC[0] && hlc[1] == rhsHLC[1]
}

func (hlc HLCRevision) GreaterThan(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		rhs = zeroHLC
	}

	rhsHLC := rhs.(HLCRevision)
	return hlc[0] > rhsHLC[0] || (hlc[0] == rhsHLC[0] && hlc[1] > rhsHLC[1])
}

func (hlc HLCRevision) LessThan(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		rhs = zeroHLC
	}

	rhsHLC := rhs.(HLCRevision)
	return hlc[0] < rhsHLC[0] || (hlc[0] == rhsHLC[0] && hlc[1] < rhsHLC[1])
}

func (hlc HLCRevision) String() string {
	if hlc[1] == 0 {
		return strconv.FormatInt(hlc[0], 10)
	}

	logicalClockString := strconv.FormatInt(hlc[1]-logicalClockOffset, 10)
	return strconv.FormatInt(hlc[0], 10) + "." + strings.Repeat("0", logicalClockLength-len(logicalClockString)) + logicalClockString
}

func (hlc HLCRevision) TimestampNanoSec() int64 {
	return hlc[0]
}

func (hlc HLCRevision) InexactFloat64() float64 {
	if hlc[1] == 0 {
		return float64(hlc[0])
	}

	return float64(hlc[0]) + float64(hlc[1]-logicalClockOffset)/math.Pow10(logicalClockLength)
}

func (hlc HLCRevision) ConstructForTimestamp(timestamp int64) WithTimestampRevision {
	return HLCRevision([2]int64{timestamp, 0})
}

var (
	_ datastore.Revision    = HLCRevision{}
	_ WithTimestampRevision = HLCRevision{}
)

// HLCKeyFunc is used to convert a simple HLC for use in maps.
func HLCKeyFunc(r HLCRevision) [2]int64 {
	return r
}

// HLCKeyLessThanFunc is used to compare keys created by the HLCKeyFunc.
func HLCKeyLessThanFunc(lhs, rhs [2]int64) bool {
	return lhs[0] < rhs[0] || (lhs[0] == rhs[0] && lhs[1] < rhs[1])
}
