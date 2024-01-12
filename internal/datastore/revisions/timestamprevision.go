package revisions

import (
	"fmt"
	"strconv"
	"time"

	"github.com/authzed/spicedb/pkg/datastore"
)

// TimestampRevision is a revision that is a timestamp.
type TimestampRevision int64

// NewForTime creates a new revision for the given time.
func NewForTime(time time.Time) TimestampRevision {
	return TimestampRevision(time.UnixNano())
}

// NewForTimestamp creates a new revision for the given timestamp.
func NewForTimestamp(timestampNanosec int64) TimestampRevision {
	return TimestampRevision(timestampNanosec)
}

// parseTimestampRevisionString parses a string into a timestamp revision.
func parseTimestampRevisionString(revisionStr string) (rev datastore.Revision, err error) {
	parsed, err := strconv.ParseInt(revisionStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid integer revision: %w", err)
	}

	return TimestampRevision(parsed), nil
}

func (ir TimestampRevision) Equal(other datastore.Revision) bool {
	return int64(ir) == int64(other.(TimestampRevision))
}

func (ir TimestampRevision) GreaterThan(other datastore.Revision) bool {
	return int64(ir) > int64(other.(TimestampRevision))
}

func (ir TimestampRevision) LessThan(other datastore.Revision) bool {
	return int64(ir) < int64(other.(TimestampRevision))
}

func (ir TimestampRevision) TimestampNanoSec() int64 {
	return int64(ir)
}

func (ir TimestampRevision) String() string {
	return strconv.FormatInt(int64(ir), 10)
}

func (ir TimestampRevision) Time() time.Time {
	return time.Unix(0, int64(ir))
}

func (ir TimestampRevision) WithInexactFloat64() float64 {
	return float64(ir)
}

func (ir TimestampRevision) ConstructForTimestamp(timestamp int64) WithTimestampRevision {
	return TimestampRevision(timestamp)
}

func (ir TimestampRevision) IntegerRepresentation() (int64, uint32) {
	return int64(ir), 0
}

var (
	_ datastore.Revision    = TimestampRevision(0)
	_ WithTimestampRevision = TimestampRevision(0)
)

// TimestampIDKeyFunc is used to create keys for timestamps.
func TimestampIDKeyFunc(r TimestampRevision) int64 {
	return int64(r)
}

// TimestampIDKeyLessThanFunc is used to create keys for timestamps.
func TimestampIDKeyLessThanFunc(l, r int64) bool {
	return l < r
}
