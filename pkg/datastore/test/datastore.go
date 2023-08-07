package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/namespace"
)

// veryLargeGCWindow is a very large time duration, which when passed to a constructor should
// effectively disable garbage collection.
const (
	veryLargeGCWindow   = 90000 * time.Second
	veryLargeGCInterval = 90000 * time.Second
)

// DatastoreTester provides a generic datastore suite a means of initializing
// a particular datastore.
type DatastoreTester interface {
	// New creates a new datastore instance for a single test.
	New(revisionQuantization, gcInterval, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error)
}

type DatastoreTesterFunc func(revisionQuantization, gcInterval, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error)

func (f DatastoreTesterFunc) New(revisionQuantization, gcInterval, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	return f(revisionQuantization, gcInterval, gcWindow, watchBufferLength)
}

type TestableDatastore interface {
	datastore.Datastore

	ExampleRetryableError() error
}

type Categories map[string]struct{}

func (c Categories) GC() bool {
	_, ok := c[GCCategory]
	return ok
}

func (c Categories) Watch() bool {
	_, ok := c[WatchCategory]
	return ok
}

var noException = Categories{}

const (
	GCCategory    = "GC"
	WatchCategory = "Watch"
)

func WithCategories(cats ...string) Categories {
	c := Categories{}
	for _, cat := range cats {
		c[cat] = struct{}{}
	}
	return c
}

// AllWithExceptions runs all generic datastore tests on a DatastoreTester, except
// those specified test categories
func AllWithExceptions(t *testing.T, tester DatastoreTester, except Categories) {
	t.Run("TestNamespaceNotFound", func(t *testing.T) { NamespaceNotFoundTest(t, tester) })
	t.Run("TestNamespaceWrite", func(t *testing.T) { NamespaceWriteTest(t, tester) })
	t.Run("TestNamespaceDelete", func(t *testing.T) { NamespaceDeleteTest(t, tester) })
	t.Run("TestNamespaceMultiDelete", func(t *testing.T) { NamespaceMultiDeleteTest(t, tester) })
	t.Run("TestEmptyNamespaceDelete", func(t *testing.T) { EmptyNamespaceDeleteTest(t, tester) })
	t.Run("TestStableNamespaceReadWrite", func(t *testing.T) { StableNamespaceReadWriteTest(t, tester) })

	t.Run("TestSimple", func(t *testing.T) { SimpleTest(t, tester) })
	t.Run("TestObjectIDs", func(t *testing.T) { ObjectIDsTest(t, tester) })
	t.Run("TestDeleteRelationships", func(t *testing.T) { DeleteRelationshipsTest(t, tester) })
	t.Run("TestDeleteNonExistant", func(t *testing.T) { DeleteNotExistantTest(t, tester) })
	t.Run("TestDeleteAlreadyDeleted", func(t *testing.T) { DeleteAlreadyDeletedTest(t, tester) })
	t.Run("TestWriteDeleteWrite", func(t *testing.T) { WriteDeleteWriteTest(t, tester) })
	t.Run("TestCreateAlreadyExisting", func(t *testing.T) { CreateAlreadyExistingTest(t, tester) })
	t.Run("TestTouchAlreadyExisting", func(t *testing.T) { TouchAlreadyExistingTest(t, tester) })
	t.Run("TestCreateDeleteTouchTest", func(t *testing.T) { CreateDeleteTouchTest(t, tester) })
	t.Run("TestCreateTouchDeleteTouchTest", func(t *testing.T) { CreateTouchDeleteTouchTest(t, tester) })
	t.Run("TestTouchAlreadyExistingCaveated", func(t *testing.T) { TouchAlreadyExistingCaveatedTest(t, tester) })

	t.Run("TestMultipleReadsInRWT", func(t *testing.T) { MultipleReadsInRWTTest(t, tester) })
	t.Run("TestConcurrentWriteSerialization", func(t *testing.T) { ConcurrentWriteSerializationTest(t, tester) })

	t.Run("TestOrdering", func(t *testing.T) { OrderingTest(t, tester) })
	t.Run("TestLimit", func(t *testing.T) { LimitTest(t, tester) })
	t.Run("TestOrderedLimit", func(t *testing.T) { OrderedLimitTest(t, tester) })
	t.Run("TestResume", func(t *testing.T) { ResumeTest(t, tester) })
	t.Run("TestCursorErrors", func(t *testing.T) { CursorErrorsTest(t, tester) })

	t.Run("TestRevisionQuantization", func(t *testing.T) { RevisionQuantizationTest(t, tester) })
	t.Run("TestRevisionSerialization", func(t *testing.T) { RevisionSerializationTest(t, tester) })

	if !except.GC() {
		t.Run("TestRevisionGC", func(t *testing.T) { RevisionGCTest(t, tester) })
		t.Run("TestInvalidReads", func(t *testing.T) { InvalidReadsTest(t, tester) })
	}

	t.Run("TestBulkUpload", func(t *testing.T) { BulkUploadTest(t, tester) })
	t.Run("TestBulkUploadErrors", func(t *testing.T) { BulkUploadErrorsTest(t, tester) })

	t.Run("TestStats", func(t *testing.T) { StatsTest(t, tester) })

	t.Run("TestRetries", func(t *testing.T) { RetryTest(t, tester) })

	t.Run("TestCaveatNotFound", func(t *testing.T) { CaveatNotFoundTest(t, tester) })
	t.Run("TestWriteReadDeleteCaveat", func(t *testing.T) { WriteReadDeleteCaveatTest(t, tester) })
	t.Run("TestWriteCaveatedRelationship", func(t *testing.T) { WriteCaveatedRelationshipTest(t, tester) })
	t.Run("TestCaveatedRelationshipFilter", func(t *testing.T) { CaveatedRelationshipFilterTest(t, tester) })
	t.Run("TestCaveatSnapshotReads", func(t *testing.T) { CaveatSnapshotReadsTest(t, tester) })

	if !except.Watch() {
		t.Run("TestWatch", func(t *testing.T) { WatchTest(t, tester) })
		t.Run("TestWatchCancel", func(t *testing.T) { WatchCancelTest(t, tester) })
		t.Run("TestCaveatedRelationshipWatch", func(t *testing.T) { CaveatedRelationshipWatchTest(t, tester) })
		t.Run("TestWatchWithTouch", func(t *testing.T) { WatchWithTouchTest(t, tester) })
	}
}

// All runs all generic datastore tests on a DatastoreTester.
func All(t *testing.T, tester DatastoreTester) {
	AllWithExceptions(t, tester, noException)
}

var testResourceNS = namespace.Namespace(
	testResourceNamespace,
	namespace.MustRelation(testReaderRelation, nil),
)

var testGroupNS = namespace.Namespace(
	testGroupNamespace,
	namespace.MustRelation(testMemberRelation, nil),
)

var testUserNS = namespace.Namespace(testUserNamespace)

func makeTestTuple(resourceID, userID string) *core.RelationTuple {
	return &core.RelationTuple{
		ResourceAndRelation: &core.ObjectAndRelation{
			Namespace: testResourceNamespace,
			ObjectId:  resourceID,
			Relation:  testReaderRelation,
		},
		Subject: &core.ObjectAndRelation{
			Namespace: testUserNamespace,
			ObjectId:  userID,
			Relation:  ellipsis,
		},
	}
}

func setupDatastore(ds datastore.Datastore, require *require.Assertions) datastore.Revision {
	ctx := context.Background()

	revision, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, testGroupNS, testResourceNS, testUserNS)
	})
	require.NoError(err)

	return revision
}
