package graph

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// sourceIterator yields count relationships, then err if non-nil, and closes finished once it
// returns, as a SQL datastore iterator releases its connection once it returns.
func sourceIterator(count int, err error, finished chan struct{}) datastore.RelationshipIterator {
	return func(yield func(tuple.Relationship, error) bool) {
		defer close(finished)
		for i := range count {
			if !yield(tuple.MustParse(fmt.Sprintf("document:doc%d#viewer@user:tom", i)), nil) {
				return
			}
		}
		if err != nil {
			yield(tuple.Relationship{}, err)
		}
	}
}

func TestReadAheadRelationshipsYieldsAllInOrder(t *testing.T) {
	finished := make(chan struct{})
	var found []string
	for rel, err := range readAheadRelationships(sourceIterator(1000, nil, finished)) {
		require.NoError(t, err)
		found = append(found, rel.Resource.ObjectID)
	}

	require.Len(t, found, 1000)
	for i, id := range found {
		require.Equal(t, fmt.Sprintf("doc%d", i), id)
	}
	<-finished
}

func TestReadAheadRelationshipsReadsWithoutWaitingForConsumer(t *testing.T) {
	finished := make(chan struct{})
	count := 0
	for _, err := range readAheadRelationships(sourceIterator(100, nil, finished)) {
		require.NoError(t, err)
		if count == 0 {
			// The source must be fully read, and so its connection released, while the consumer is
			// still processing the first relationship.
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				require.Fail(t, "source iterator was not read ahead of the consumer")
			}
		}
		count++
	}
	require.Equal(t, 100, count)
}

func TestReadAheadRelationshipsPropagatesError(t *testing.T) {
	finished := make(chan struct{})
	sourceErr := errors.New("source failed")

	count := 0
	var gotErr error
	for _, err := range readAheadRelationships(sourceIterator(10, sourceErr, finished)) {
		if err != nil {
			gotErr = err
			break
		}
		count++
	}

	require.Equal(t, 10, count)
	require.ErrorIs(t, gotErr, sourceErr)
}

func TestReadAheadRelationshipsStopsReadingWhenConsumerStops(t *testing.T) {
	finished := make(chan struct{})
	release := make(chan struct{})

	// A source that blocks after its first relationship until released, so that the consumer
	// stops before the source has finished.
	source := func(yield func(tuple.Relationship, error) bool) {
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
	}

	for range readAheadRelationships(source) {
		break
	}
	close(release)

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		require.Fail(t, "source iterator kept being read after the consumer stopped")
	}
}

func TestReadAheadRelationshipsRepanicsOnConsumer(t *testing.T) {
	source := func(yield func(tuple.Relationship, error) bool) {
		yield(tuple.MustParse("document:first#viewer@user:tom"), nil)
		panic("source panicked")
	}

	require.PanicsWithValue(t, "source panicked", func() {
		for range readAheadRelationships(source) {
		}
	})
}
