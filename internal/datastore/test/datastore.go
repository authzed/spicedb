package test

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/pkg/namespace"
	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
)

// veryLargeGCWindow is a very large time duration, which when passed to a constructor should
// effectively disable garbage collection.
const veryLargeGCWindow = 90000 * time.Second

type DatastoreTester interface {
	// Creates a new datastore instance for a single test
	New(revisionFuzzingTimedelta, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error)
}

func TestAll(t *testing.T, tester DatastoreTester) {
	t.Run("TestSimple", func(t *testing.T) { TestSimple(t, tester) })
	t.Run("TestRevisionFuzzing", func(t *testing.T) { TestRevisionFuzzing(t, tester) })
	t.Run("TestPreconditions", func(t *testing.T) { TestPreconditions(t, tester) })
	t.Run("TestInvalidReads", func(t *testing.T) { TestInvalidReads(t, tester) })
	t.Run("TestNamespaceWrite", func(t *testing.T) { TestNamespaceWrite(t, tester) })
	t.Run("TestNamespaceDelete", func(t *testing.T) { TestNamespaceDelete(t, tester) })
	t.Run("TestWatch", func(t *testing.T) { TestWatch(t, tester) })
	t.Run("TestWatchCancel", func(t *testing.T) { TestWatchCancel(t, tester) })
}

var testResourceNS = namespace.Namespace(
	testResourceNamespace,
	namespace.Relation(testReaderRelation, nil),
)

var testUserNS = namespace.Namespace(testUserNamespace)

func makeTestTuple(resourceID, userID string) *pb.RelationTuple {
	return &pb.RelationTuple{
		ObjectAndRelation: &pb.ObjectAndRelation{
			Namespace: testResourceNamespace,
			ObjectId:  resourceID,
			Relation:  testReaderRelation,
		},
		User: &pb.User{
			UserOneof: &pb.User_Userset{
				Userset: &pb.ObjectAndRelation{
					Namespace: testUserNamespace,
					ObjectId:  userID,
					Relation:  ellipsis,
				},
			},
		},
	}
}

func setupDatastore(ds datastore.Datastore, require *require.Assertions) decimal.Decimal {
	ctx := context.Background()

	_, err := ds.WriteNamespace(ctx, testResourceNS)
	require.NoError(err)

	revision, err := ds.WriteNamespace(ctx, testUserNS)
	require.NoError(err)

	return revision
}
