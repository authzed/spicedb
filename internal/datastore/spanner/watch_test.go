package spanner

import (
	"testing"
	"time"

	"github.com/cloudspannerecosystem/spanner-change-streams-tail/changestreams"
	"github.com/stretchr/testify/require"
)

func TestWatchPartitionFrontier(t *testing.T) {
	timestamp := func(n int64) time.Time { return time.Unix(0, n).UTC() }
	p := watchPartitions{start: timestamp(10), timestamps: map[string]time.Time{"": timestamp(10)}}
	result := func(token string, record *changestreams.ChangeRecord) time.Time {
		return p.advance(&changestreams.ReadResult{PartitionToken: token, ChangeRecords: []*changestreams.ChangeRecord{record}})
	}
	children := func(start int64, tokens ...string) *changestreams.ChangeRecord {
		entries := make([]*changestreams.ChildPartition, 0, len(tokens))
		for _, token := range tokens {
			entries = append(entries, &changestreams.ChildPartition{Token: token})
		}
		return &changestreams.ChangeRecord{ChildPartitionsRecords: []*changestreams.ChildPartitionsRecord{{StartTimestamp: timestamp(start), ChildPartitions: entries}}}
	}
	heartbeat := func(n int64) *changestreams.ChangeRecord {
		return &changestreams.ChangeRecord{HeartbeatRecords: []*changestreams.HeartbeatRecord{{Timestamp: timestamp(n)}}}
	}
	require.Equal(t, timestamp(11), result("", children(11, "a")))
	require.NotContains(t, p.timestamps, "")
	// Metadata can span callbacks, and retired streams can have a trailing heartbeat.
	require.Equal(t, timestamp(11), result("", children(11, "b")))
	require.Equal(t, timestamp(11), result("", heartbeat(11)))
	require.NotContains(t, p.timestamps, "")
	// A fast partition cannot publish past an unread partition, including its last-record flag.
	require.Equal(t, timestamp(11), result("a", &changestreams.ChangeRecord{DataChangeRecords: []*changestreams.DataChangeRecord{{CommitTimestamp: timestamp(20), IsLastRecordInTransactionInPartition: true, NumberOfPartitionsInTransaction: 2}}}))
	require.Equal(t, timestamp(15), result("b", heartbeat(15)))
	require.Equal(t, timestamp(20), result("b", heartbeat(30)))
	// Replayed timestamps cannot move a partition's frontier backwards.
	require.Equal(t, timestamp(20), result("b", heartbeat(12)))
	// Split replacement partitions start at the split boundary, not their first received record.
	require.Equal(t, timestamp(21), result("a", children(21, "a1", "a2")))
	require.Equal(t, timestamp(21), result("a1", heartbeat(40)))
	require.Equal(t, timestamp(30), result("a2", heartbeat(40)))
	// A merge announced by both parents must preserve progress already received from the child.
	require.Equal(t, timestamp(31), result("b", children(31, "merged")))
	require.Equal(t, timestamp(40), result("merged", heartbeat(50)))
	require.Equal(t, timestamp(40), result("a1", children(31, "merged")))
	require.Equal(t, timestamp(50), result("a2", children(31, "merged")))
	require.Equal(t, timestamp(50), result("a2", heartbeat(31)))
	require.NotContains(t, p.timestamps, "a2")
}

func TestWatchMutablePartitionFrontier(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	p := watchPartitions{start: start, timestamps: map[string]time.Time{"": start}}
	advance := func(token string, record *changestreams.ChangeRecord) time.Time {
		return p.advance(&changestreams.ReadResult{PartitionToken: token, ChangeRecords: []*changestreams.ChangeRecord{record}})
	}
	require.Equal(t, start, advance("", &changestreams.ChangeRecord{PartitionStartRecords: []*changestreams.PartitionStartRecord{{StartTimestamp: start, PartitionTokens: []string{"a", "b"}}}}))
	require.NotContains(t, p.timestamps, "")
	require.Equal(t, start, advance("a", &changestreams.ChangeRecord{PartitionEventRecords: []*changestreams.PartitionEventRecord{{CommitTimestamp: start.Add(time.Second), MoveOutEvents: []*changestreams.MoveOutEvent{{DestinationPartitionToken: "c"}}}}}))
	require.Equal(t, start.Add(time.Second), advance("b", &changestreams.ChangeRecord{PartitionEndRecords: []*changestreams.PartitionEndRecord{{PartitionToken: "b", EndTimestamp: start.Add(time.Second)}}}))
	require.Contains(t, p.timestamps, "c")
}
