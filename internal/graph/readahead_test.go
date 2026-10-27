package graph

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// fakeSource serves a fixed, ordered set of relationships to readAheadRelationships, resuming
// after a cursor as a datastore query would, and records how it was read.
type fakeSource struct {
	rels []tuple.Relationship
	err  error

	queries atomic.Int32
	read    atomic.Int64

	mu       sync.Mutex
	finished []chan struct{} // GUARDED_BY(mu)
}

func newFakeSource(count int, err error) *fakeSource {
	rels := make([]tuple.Relationship, 0, count)
	for i := range count {
		rels = append(rels, tuple.MustParse(fmt.Sprintf("document:doc%05d#viewer@user:tom", i)))
	}
	return &fakeSource{rels: rels, err: err}
}

// finishedChan returns a channel that is closed once the nth (0-indexed) query's iterator has
// returned, as a SQL datastore iterator releases its connection once it returns.
func (fs *fakeSource) finishedChan(n int) chan struct{} {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for len(fs.finished) <= n {
		fs.finished = append(fs.finished, make(chan struct{}))
	}
	return fs.finished[n]
}

func (fs *fakeSource) query(after options.Cursor) (datastore.RelationshipIterator, error) {
	queryIndex := int(fs.queries.Add(1)) - 1
	start := 0
	if afterRel := options.ToRelationship(after); afterRel != nil {
		start = slices.IndexFunc(fs.rels, func(rel tuple.Relationship) bool {
			return tuple.MustString(rel) == tuple.MustString(*afterRel)
		}) + 1
	}

	return func(yield func(tuple.Relationship, error) bool) {
		defer close(fs.finishedChan(queryIndex))
		for _, rel := range fs.rels[start:] {
			fs.read.Add(1)
			if !yield(rel, nil) {
				return
			}
		}
		if fs.err != nil {
			yield(tuple.Relationship{}, fs.err)
		}
	}, nil
}

func requireClosed(t *testing.T, ch chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		require.Fail(t, msg)
	}
}

func TestReadAheadRelationshipsYieldsAllInOrder(t *testing.T) {
	source := newFakeSource(1000, nil)

	var found []string
	for rel, err := range readAheadRelationships(nil, 10_000, source.query) {
		require.NoError(t, err)
		found = append(found, rel.Resource.ObjectID)
	}

	require.Len(t, found, 1000)
	for i, id := range found {
		require.Equal(t, fmt.Sprintf("doc%05d", i), id)
	}
	require.Equal(t, int32(1), source.queries.Load())
}

func TestReadAheadRelationshipsResumesAfterCursor(t *testing.T) {
	source := newFakeSource(10, nil)

	var found []string
	for rel, err := range readAheadRelationships(options.ToCursor(source.rels[6]), 10_000, source.query) {
		require.NoError(t, err)
		found = append(found, rel.Resource.ObjectID)
	}
	require.Equal(t, []string{"doc00007", "doc00008", "doc00009"}, found)
}

func TestReadAheadRelationshipsReadsWithoutWaitingForConsumer(t *testing.T) {
	source := newFakeSource(100, nil)

	count := 0
	for _, err := range readAheadRelationships(nil, 10_000, source.query) {
		require.NoError(t, err)
		if count == 0 {
			// The query must be fully read, and so its connection released, while the consumer
			// is still processing the first relationship.
			requireClosed(t, source.finishedChan(0), "query was not read ahead of the consumer")
		}
		count++
	}
	require.Equal(t, 100, count)
}

func TestReadAheadRelationshipsPausesWhenBufferIsFull(t *testing.T) {
	const maxBuffered = 10
	source := newFakeSource(35, nil)

	var found []string
	for rel, err := range readAheadRelationships(nil, maxBuffered, source.query) {
		require.NoError(t, err)

		if len(found) == 0 {
			// With the consumer blocked on the first relationship, the first query must stop
			// once the buffer is full, releasing its connection, and no further query is made
			// until the consumer catches up.
			requireClosed(t, source.finishedChan(0), "query was not closed when the buffer filled")
			require.Equal(t, int64(maxBuffered), source.read.Load())
			time.Sleep(50 * time.Millisecond)
			require.Equal(t, int32(1), source.queries.Load())
		}

		found = append(found, rel.Resource.ObjectID)
		require.LessOrEqual(t, source.read.Load()-int64(len(found)), int64(maxBuffered),
			"more relationships were buffered than allowed")
	}

	require.Len(t, found, 35)
	for i, id := range found {
		require.Equal(t, fmt.Sprintf("doc%05d", i), id)
	}
	require.Greater(t, source.queries.Load(), int32(1))
}

func TestReadAheadRelationshipsPropagatesError(t *testing.T) {
	sourceErr := errors.New("source failed")
	source := newFakeSource(10, sourceErr)

	count := 0
	var gotErr error
	for _, err := range readAheadRelationships(nil, 10_000, source.query) {
		if err != nil {
			gotErr = err
			break
		}
		count++
	}

	require.Equal(t, 10, count)
	require.ErrorIs(t, gotErr, sourceErr)
}

func TestReadAheadRelationshipsPropagatesQueryError(t *testing.T) {
	queryErr := errors.New("query failed")
	query := func(options.Cursor) (datastore.RelationshipIterator, error) {
		return nil, queryErr
	}

	var gotErr error
	for _, err := range readAheadRelationships(nil, 10_000, query) {
		gotErr = err
	}
	require.ErrorIs(t, gotErr, queryErr)
}

func TestReadAheadRelationshipsStopsReadingWhenConsumerStops(t *testing.T) {
	finished := make(chan struct{})
	release := make(chan struct{})

	// A query that blocks after its first relationship until released, so that the consumer
	// stops before the query has finished.
	query := func(options.Cursor) (datastore.RelationshipIterator, error) {
		return func(yield func(tuple.Relationship, error) bool) {
			defer close(finished)
			if !yield(tuple.MustParse("document:first#viewer@user:tom"), nil) {
				return
			}
			<-release
			for i := range 1000 {
				if !yield(tuple.MustParse(fmt.Sprintf("document:doc%d#viewer@user:tom", i)), nil) {
					return
				}
			}
		}, nil
	}

	for range readAheadRelationships(nil, 10_000, query) {
		break
	}
	close(release)

	requireClosed(t, finished, "query kept being read after the consumer stopped")
}
