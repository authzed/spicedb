package test

import (
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/datastore"
)

type DatastoreTester interface {
	// Creates a new datastore instance for a single test
	New(revisionFuzzingTimedelta time.Duration) (datastore.Datastore, error)
}

func TestAll(t *testing.T, tester DatastoreTester) {
	t.Run("TestSimple", func(t *testing.T) { TestSimple(t, tester) })
	t.Run("TestWatch", func(t *testing.T) { TestWatch(t, tester) })
	t.Run("TestWatchCancel", func(t *testing.T) { TestWatchCancel(t, tester) })
	t.Run("TestDelete", func(t *testing.T) { TestDelete(t, tester) })
	t.Run("TestPreconditions", func(t *testing.T) { TestPreconditions(t, tester) })
	t.Run("TestRevisionFuzzing", func(t *testing.T) { TestRevisionFuzzing(t, tester) })
}
