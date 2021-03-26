package test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/namespace"
)

type DatastoreTester interface {
	// Creates a new datastore instance for a single test
	New(revisionFuzzingTimedelta time.Duration) (datastore.Datastore, error)
}

func TestAll(t *testing.T, tester DatastoreTester) {
	t.Run("TestSimple", func(t *testing.T) { TestSimple(t, tester) })
	t.Run("TestRevisionFuzzing", func(t *testing.T) { TestRevisionFuzzing(t, tester) })
	t.Run("TestPreconditions", func(t *testing.T) { TestPreconditions(t, tester) })
	t.Run("TestWriteInvalidTuples", func(t *testing.T) { TestWriteInvalidTuples(t, tester) })
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

func setupDatastore(ds datastore.Datastore, require *require.Assertions) {
	_, err := ds.WriteNamespace(testResourceNS)
	require.NoError(err)

	_, err = ds.WriteNamespace(testUserNS)
	require.NoError(err)
}
