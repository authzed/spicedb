package test

import (
	"context"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/pkg/namespace"
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
	t.Run("TestWritePreconditions", func(t *testing.T) { TestWritePreconditions(t, tester) })
	t.Run("TestDeletePreconditions", func(t *testing.T) { TestDeletePreconditions(t, tester) })
	t.Run("TestDeleteRelationships", func(t *testing.T) { TestDeleteRelationships(t, tester) })
	t.Run("TestInvalidReads", func(t *testing.T) { TestInvalidReads(t, tester) })
	t.Run("TestNamespaceWrite", func(t *testing.T) { TestNamespaceWrite(t, tester) })
	t.Run("TestNamespaceDelete", func(t *testing.T) { TestNamespaceDelete(t, tester) })
	t.Run("TestWatch", func(t *testing.T) { TestWatch(t, tester) })
	t.Run("TestWatchCancel", func(t *testing.T) { TestWatchCancel(t, tester) })
	t.Run("TestUsersets", func(t *testing.T) { TestUsersets(t, tester) })
}

var testResourceNS = namespace.Namespace(
	testResourceNamespace,
	namespace.Relation(testReaderRelation, nil),
)

var testUserNS = namespace.Namespace(testUserNamespace)

func makeTestTuple(resourceID, userID string) *v0.RelationTuple {
	return &v0.RelationTuple{
		ObjectAndRelation: &v0.ObjectAndRelation{
			Namespace: testResourceNamespace,
			ObjectId:  resourceID,
			Relation:  testReaderRelation,
		},
		User: &v0.User{UserOneof: &v0.User_Userset{Userset: &v0.ObjectAndRelation{
			Namespace: testUserNamespace,
			ObjectId:  userID,
			Relation:  ellipsis,
		}}},
	}
}

func makeTestRelationship(resourceID, userID string) *v1.Relationship {
	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: testResourceNamespace,
			ObjectId:   resourceID,
		},
		Relation: testReaderRelation,
		Subject: &v1.SubjectReference{Object: &v1.ObjectReference{
			ObjectType: testUserNamespace,
			ObjectId:   userID,
		}},
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
