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

func (c Categories) Stats() bool {
	_, ok := c[StatsCategory]
	return ok
}

func (c Categories) Watch() bool {
	_, ok := c[WatchCategory]
	return ok
}

func (c Categories) WatchSchema() bool {
	_, ok := c[WatchSchemaCategory]
	return ok
}

func (c Categories) WatchCheckpoints() bool {
	_, ok := c[WatchCheckpointsCategory]
	return ok
}

var noException = Categories{}

const (
	GCCategory               = "GC"
	WatchCategory            = "Watch"
	WatchSchemaCategory      = "WatchSchema"
	WatchCheckpointsCategory = "WatchCheckpoints"
	StatsCategory            = "Stats"
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
	t.Run("TestUseAfterClose", func(t *testing.T) { UseAfterCloseTest(t, tester) })

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
	t.Run("TestRecreateRelationshipsAfterDeleteWithFilter", func(t *testing.T) { RecreateRelationshipsAfterDeleteWithFilter(t, tester) })
	t.Run("TestWriteDeleteWrite", func(t *testing.T) { WriteDeleteWriteTest(t, tester) })
	t.Run("TestCreateAlreadyExisting", func(t *testing.T) { CreateAlreadyExistingTest(t, tester) })
	t.Run("TestTouchAlreadyExistingWithoutCaveat", func(t *testing.T) { TouchAlreadyExistingTest(t, tester) })
	t.Run("TestCreateDeleteTouch", func(t *testing.T) { CreateDeleteTouchTest(t, tester) })
	t.Run("TestDeleteOneThousandIndividualInOneCall", func(t *testing.T) { DeleteOneThousandIndividualInOneCallTest(t, tester) })
	t.Run("TestCreateTouchDeleteTouch", func(t *testing.T) { CreateTouchDeleteTouchTest(t, tester) })
	t.Run("TestTouchAlreadyExistingCaveated", func(t *testing.T) { TouchAlreadyExistingCaveatedTest(t, tester) })
	t.Run("TestBulkDeleteRelationships", func(t *testing.T) { BulkDeleteRelationshipsTest(t, tester) })
	t.Run("TestDeleteCaveatedTuple", func(t *testing.T) { DeleteCaveatedTupleTest(t, tester) })
	t.Run("TestDeleteWithLimit", func(t *testing.T) { DeleteWithLimitTest(t, tester) })
	t.Run("TestQueryRelationshipsWithVariousFilters", func(t *testing.T) { QueryRelationshipsWithVariousFiltersTest(t, tester) })
	t.Run("TestDeleteRelationshipsWithVariousFilters", func(t *testing.T) { DeleteRelationshipsWithVariousFiltersTest(t, tester) })
	t.Run("TestTouchTypedAlreadyExistingWithoutCaveat", func(t *testing.T) { TypedTouchAlreadyExistingTest(t, tester) })
	t.Run("TestTouchTypedAlreadyExistingWithCaveat", func(t *testing.T) { TypedTouchAlreadyExistingWithCaveatTest(t, tester) })

	t.Run("TestMultipleReadsInRWT", func(t *testing.T) { MultipleReadsInRWTTest(t, tester) })
	t.Run("TestConcurrentWriteSerialization", func(t *testing.T) { ConcurrentWriteSerializationTest(t, tester) })

	t.Run("TestOrdering", func(t *testing.T) { OrderingTest(t, tester) })
	t.Run("TestLimit", func(t *testing.T) { LimitTest(t, tester) })
	t.Run("TestOrderedLimit", func(t *testing.T) { OrderedLimitTest(t, tester) })
	t.Run("TestResume", func(t *testing.T) { ResumeTest(t, tester) })
	t.Run("TestCursorErrors", func(t *testing.T) { CursorErrorsTest(t, tester) })
	t.Run("TestReverseQueryCursor", func(t *testing.T) { ReverseQueryCursorTest(t, tester) })

	t.Run("TestRevisionQuantization", func(t *testing.T) { RevisionQuantizationTest(t, tester) })
	t.Run("TestRevisionSerialization", func(t *testing.T) { RevisionSerializationTest(t, tester) })
	t.Run("TestSequentialRevisions", func(t *testing.T) { SequentialRevisionsTest(t, tester) })
	t.Run("TestConcurrentRevisions", func(t *testing.T) { ConcurrentRevisionsTest(t, tester) })
	t.Run("TestCheckRevisions", func(t *testing.T) { CheckRevisionsTest(t, tester) })

	if !except.GC() {
		t.Run("TestRevisionGC", func(t *testing.T) { RevisionGCTest(t, tester) })
		t.Run("TestInvalidReads", func(t *testing.T) { InvalidReadsTest(t, tester) })
	}

	t.Run("TestBulkUpload", func(t *testing.T) { BulkUploadTest(t, tester) })
	t.Run("TestBulkUploadErrors", func(t *testing.T) { BulkUploadErrorsTest(t, tester) })
	t.Run("TestBulkUploadAlreadyExistsError", func(t *testing.T) { BulkUploadAlreadyExistsErrorTest(t, tester) })
	t.Run("TestBulkUploadAlreadyExistsSameCallError", func(t *testing.T) { BulkUploadAlreadyExistsSameCallErrorTest(t, tester) })
	t.Run("BulkUploadEditCaveat", func(t *testing.T) { BulkUploadEditCaveat(t, tester) })

	if !except.Stats() {
		t.Run("TestStats", func(t *testing.T) { StatsTest(t, tester) })
	}

	t.Run("TestRetries", func(t *testing.T) { RetryTest(t, tester) })

	t.Run("TestCaveatNotFound", func(t *testing.T) { CaveatNotFoundTest(t, tester) })
	t.Run("TestWriteReadDeleteCaveat", func(t *testing.T) { WriteReadDeleteCaveatTest(t, tester) })
	t.Run("TestWriteCaveatedRelationship", func(t *testing.T) { WriteCaveatedRelationshipTest(t, tester) })
	t.Run("TestCaveatedRelationshipFilter", func(t *testing.T) { CaveatedRelationshipFilterTest(t, tester) })
	t.Run("TestCaveatSnapshotReads", func(t *testing.T) { CaveatSnapshotReadsTest(t, tester) })

	if !except.Watch() {
		t.Run("TestWatchBasic", func(t *testing.T) { WatchTest(t, tester) })
		t.Run("TestWatchCancel", func(t *testing.T) { WatchCancelTest(t, tester) })
		t.Run("TestCaveatedRelationshipWatch", func(t *testing.T) { CaveatedRelationshipWatchTest(t, tester) })
		t.Run("TestWatchWithTouch", func(t *testing.T) { WatchWithTouchTest(t, tester) })
		t.Run("TestWatchWithDelete", func(t *testing.T) { WatchWithDeleteTest(t, tester) })
	}

	if !except.Watch() && !except.WatchSchema() {
		t.Run("TestWatchSchema", func(t *testing.T) { WatchSchemaTest(t, tester) })
		t.Run("TestWatchAll", func(t *testing.T) { WatchAllTest(t, tester) })
	}

	if !except.Watch() && !except.WatchCheckpoints() {
		t.Run("TestWatchCheckpoints", func(t *testing.T) { WatchCheckpointsTest(t, tester) })
	}

	t.Run("TestRelationshipCounters", func(t *testing.T) { RelationshipCountersTest(t, tester) })
	t.Run("TestUpdateRelationshipCounter", func(t *testing.T) { UpdateRelationshipCounterTest(t, tester) })
	t.Run("TestDeleteAllData", func(t *testing.T) { DeleteAllDataTest(t, tester) })
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

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, testGroupNS, testResourceNS, testUserNS)
	})
	require.NoError(err)

	return revision
}
