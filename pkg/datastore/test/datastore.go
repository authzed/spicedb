package test

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/namespace"
)

// veryLargeGCWindow is a very large time duration, which when passed to a constructor should
// effectively disable garbage collection.
const veryLargeGCWindow = 90000 * time.Second

// DatastoreTester provides a generic datastore suite a means of initializing
// a particular datastore.
type DatastoreTester interface {
	// New creates a new datastore instance for a single test.
	New(revisionQuantization, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error)
}

type DatastoreTesterFunc func(revisionQuantization, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error)

func (f DatastoreTesterFunc) New(revisionQuantization, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	return f(revisionQuantization, gcWindow, watchBufferLength)
}

// All runs all generic datastore tests on a DatastoreTester.
func All(t *testing.T, tester DatastoreTester) {
	t.Run("TestNamespaceWrite", func(t *testing.T) { NamespaceWriteTest(t, tester) })
	t.Run("TestNamespaceDelete", func(t *testing.T) { NamespaceDeleteTest(t, tester) })
	t.Run("TestEmptyNamespaceDelete", func(t *testing.T) { EmptyNamespaceDeleteTest(t, tester) })
	t.Run("TestStableNamespaceReadWrite", func(t *testing.T) { StableNamespaceReadWriteTest(t, tester) })

	t.Run("TestSimple", func(t *testing.T) { SimpleTest(t, tester) })
	t.Run("TestDeleteRelationships", func(t *testing.T) { DeleteRelationshipsTest(t, tester) })
	t.Run("TestInvalidReads", func(t *testing.T) { InvalidReadsTest(t, tester) })
	t.Run("TestDeleteNonExistant", func(t *testing.T) { DeleteNotExistantTest(t, tester) })
	t.Run("TestDeleteAlreadyDeleted", func(t *testing.T) { DeleteAlreadyDeletedTest(t, tester) })
	t.Run("TestWriteDeleteWrite", func(t *testing.T) { WriteDeleteWriteTest(t, tester) })
	t.Run("TestCreateAlreadyExisting", func(t *testing.T) { CreateAlreadyExistingTest(t, tester) })
	t.Run("TestTouchAlreadyExisting", func(t *testing.T) { TouchAlreadyExistingTest(t, tester) })
	t.Run("TestUsersets", func(t *testing.T) { UsersetsTest(t, tester) })
	t.Run("TestMultipleReadsInRWT", func(t *testing.T) { MultipleReadsInRWTTest(t, tester) })
	t.Run("TestConcurrentWriteSerialization", func(t *testing.T) { ConcurrentWriteSerializationTest(t, tester) })

	t.Run("TestRevisionQuantization", func(t *testing.T) { RevisionQuantizationTest(t, tester) })

	t.Run("TestWatch", func(t *testing.T) { WatchTest(t, tester) })
	t.Run("TestWatchCancel", func(t *testing.T) { WatchCancelTest(t, tester) })

	t.Run("TestStats", func(t *testing.T) { StatsTest(t, tester) })
}

var testResourceNS = namespace.Namespace(
	testResourceNamespace,
	namespace.Relation(testReaderRelation, nil),
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

func setupDatastore(ds datastore.Datastore, require *require.Assertions) decimal.Decimal {
	ctx := context.Background()

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(testResourceNS, testUserNS)
	})
	require.NoError(err)

	return revision
}
