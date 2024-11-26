package test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

// CaveatNotFound tests to ensure that an unknown caveat returns the expected
// error.
func CaveatNotFoundTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := context.Background()

	startRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	_, _, err = ds.SnapshotReader(startRevision).ReadCaveatByName(ctx, "unknown")
	require.True(errors.As(err, &datastore.CaveatNameNotFoundError{}))
}

func WriteReadDeleteCaveatTest(t *testing.T, tester DatastoreTester) {
	req := require.New(t)
	ds, err := tester.New(0*time.Second, veryLargeGCInterval, veryLargeGCWindow, 1)
	req.NoError(err)

	skipIfNotCaveatStorer(t, ds)

	ctx := context.Background()
	// Don't fail on writing empty caveat list
	_, err = writeCaveats(ctx, ds)
	req.NoError(err)

	// Dupes in same transaction fail to be written
	coreCaveat := createCoreCaveat(t)
	coreCaveat.Name = "a"
	_, err = writeCaveats(ctx, ds, coreCaveat, coreCaveat)
	req.Error(err)

	// Succeeds writing various caveats
	anotherCoreCaveat := createCoreCaveat(t)
	anotherCoreCaveat.Name = "b"
	rev, err := writeCaveats(ctx, ds, coreCaveat, anotherCoreCaveat)
	req.NoError(err)

	// The caveat can be looked up by name
	cr := ds.SnapshotReader(rev)
	cv, _, err := cr.ReadCaveatByName(ctx, coreCaveat.Name)
	req.NoError(err)

	foundDiff := cmp.Diff(coreCaveat, cv, protocmp.Transform())
	req.Empty(foundDiff)

	// All caveats can be listed when no arg is provided
	// Manually check the caveat's contents.
	req.Equal(coreCaveat.Name, cv.Name)
	req.Equal(2, len(cv.ParameterTypes))
	req.Equal("int", cv.ParameterTypes["foo"].TypeName)
	req.Equal("map", cv.ParameterTypes["bar"].TypeName)
	req.Equal("bytes", cv.ParameterTypes["bar"].ChildTypes[0].TypeName)

	// All caveats can be listed
	cvs, err := cr.ListAllCaveats(ctx)
	req.NoError(err)
	req.Len(cvs, 2)

	foundDiff = cmp.Diff(coreCaveat, cvs[0].Definition, protocmp.Transform())
	req.Empty(foundDiff)
	foundDiff = cmp.Diff(anotherCoreCaveat, cvs[1].Definition, protocmp.Transform())
	req.Empty(foundDiff)

	// Caveats can be found by names
	cvs, err = cr.LookupCaveatsWithNames(ctx, []string{coreCaveat.Name})
	req.NoError(err)
	req.Len(cvs, 1)

	foundDiff = cmp.Diff(coreCaveat, cvs[0].Definition, protocmp.Transform())
	req.Empty(foundDiff)

	// Non-existing names returns no caveat
	cvs, err = cr.LookupCaveatsWithNames(ctx, []string{"doesnotexist"})
	req.NoError(err)
	req.Empty(cvs)

	// Empty lookup returns no values.
	cvs, err = cr.LookupCaveatsWithNames(ctx, []string{})
	req.NoError(err)
	req.Len(cvs, 0)

	// nil lookup returns no values.
	cvs, err = cr.LookupCaveatsWithNames(ctx, nil)
	req.NoError(err)
	req.Len(cvs, 0)

	// Delete Caveat
	rev, err = ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.DeleteCaveats(ctx, []string{coreCaveat.Name})
	})
	req.NoError(err)
	cr = ds.SnapshotReader(rev)
	_, _, err = cr.ReadCaveatByName(ctx, coreCaveat.Name)
	req.ErrorAs(err, &datastore.CaveatNameNotFoundError{})

	// Returns an error if caveat name or ID does not exist
	_, _, err = cr.ReadCaveatByName(ctx, "doesnotexist")
	req.ErrorAs(err, &datastore.CaveatNameNotFoundError{})
}

func WriteCaveatedRelationshipTest(t *testing.T, tester DatastoreTester) {
	req := require.New(t)
	ds, err := tester.New(0*time.Second, veryLargeGCInterval, veryLargeGCWindow, 1)
	req.NoError(err)

	skipIfNotCaveatStorer(t, ds)

	req.NoError(err)
	sds, _ := testfixtures.StandardDatastoreWithSchema(ds, req)

	// Store caveat, write caveated tuple and read back same value
	coreCaveat := createCoreCaveat(t)
	anotherCoreCaveat := createCoreCaveat(t)
	ctx := context.Background()
	_, err = writeCaveats(ctx, ds, coreCaveat, anotherCoreCaveat)
	req.NoError(err)

	rel := createTestCaveatedRel(t, "document:companyplan#somerelation@folder:company#...", coreCaveat.Name)
	rev, err := common.WriteRelationships(ctx, sds, tuple.UpdateOperationCreate, rel)
	req.NoError(err)
	assertRelCorrectlyStored(req, ds, rev, rel)

	// RelationTupleUpdate_CREATE of the same tuple and different caveat context will fail
	_, err = common.WriteRelationships(ctx, sds, tuple.UpdateOperationCreate, rel)
	req.ErrorAs(err, &common.CreateRelationshipExistsError{})

	// RelationTupleUpdate_TOUCH does update the caveat context for a caveated relationship that already exists
	currentMap := rel.OptionalCaveat.Context.AsMap()
	delete(currentMap, "b")
	st, err := structpb.NewStruct(currentMap)
	require.NoError(t, err)

	rel.OptionalCaveat.Context = st
	rev, err = common.WriteRelationships(ctx, sds, tuple.UpdateOperationTouch, rel)
	req.NoError(err)
	assertRelCorrectlyStored(req, ds, rev, rel)

	// RelationTupleUpdate_TOUCH does update the caveat name for a caveated relationship that already exists
	rel.OptionalCaveat.CaveatName = anotherCoreCaveat.Name
	rev, err = common.WriteRelationships(ctx, sds, tuple.UpdateOperationTouch, rel)
	req.NoError(err)
	assertRelCorrectlyStored(req, ds, rev, rel)

	// TOUCH can remove caveat from relationship
	caveatContext := rel.OptionalCaveat
	rel.OptionalCaveat = nil
	rev, err = common.WriteRelationships(ctx, sds, tuple.UpdateOperationTouch, rel)
	req.NoError(err)
	assertRelCorrectlyStored(req, ds, rev, rel)

	// TOUCH can store caveat in relationship with no caveat
	rel.OptionalCaveat = caveatContext
	rev, err = common.WriteRelationships(ctx, sds, tuple.UpdateOperationTouch, rel)
	req.NoError(err)
	assertRelCorrectlyStored(req, ds, rev, rel)

	// RelationTupleUpdate_DELETE ignores caveat part of the request
	rel.OptionalCaveat.CaveatName = "rando"
	rev, err = common.WriteRelationships(ctx, sds, tuple.UpdateOperationDelete, rel)
	req.NoError(err)
	iter, err := ds.SnapshotReader(rev).QueryRelationships(context.Background(), datastore.RelationshipsFilter{
		OptionalResourceType: rel.Resource.ObjectType,
	})
	req.NoError(err)

	for _, err := range iter {
		req.NoError(err)
		req.Fail("expected no relationships to be found")
	}

	// Caveated tuple can reference non-existing caveat - controller layer is responsible for validation
	rel = createTestCaveatedRel(t, "document:rando#somerelation@folder:company#...", "rando")
	_, err = common.WriteRelationships(ctx, sds, tuple.UpdateOperationCreate, rel)
	req.NoError(err)
}

func CaveatedRelationshipFilterTest(t *testing.T, tester DatastoreTester) {
	req := require.New(t)
	ds, err := tester.New(0*time.Second, veryLargeGCInterval, veryLargeGCWindow, 1)
	req.NoError(err)

	skipIfNotCaveatStorer(t, ds)

	req.NoError(err)
	sds, _ := testfixtures.StandardDatastoreWithSchema(ds, req)

	// Store caveat, write caveated tuple and read back same value
	coreCaveat := createCoreCaveat(t)
	anotherCoreCaveat := createCoreCaveat(t)
	ctx := context.Background()
	_, err = writeCaveats(ctx, ds, coreCaveat, anotherCoreCaveat)
	req.NoError(err)

	rel := createTestCaveatedRel(t, "document:companyplan#parent@folder:company#...", coreCaveat.Name)
	anotherTpl := createTestCaveatedRel(t, "document:anothercompanyplan#parent@folder:company#...", anotherCoreCaveat.Name)
	nonCaveatedTpl := tuple.MustParse("document:yetanothercompanyplan#parent@folder:company#...")
	rev, err := common.WriteRelationships(ctx, sds, tuple.UpdateOperationCreate, rel, anotherTpl, nonCaveatedTpl)
	req.NoError(err)

	// filter by first caveat
	iter, err := ds.SnapshotReader(rev).QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: rel.Resource.ObjectType,
		OptionalCaveatName:   coreCaveat.Name,
	})
	req.NoError(err)

	expectRel(req, iter, rel)

	// filter by second caveat
	iter, err = ds.SnapshotReader(rev).QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: anotherTpl.Resource.ObjectType,
		OptionalCaveatName:   anotherCoreCaveat.Name,
	})
	req.NoError(err)

	expectRel(req, iter, anotherTpl)
}

func CaveatSnapshotReadsTest(t *testing.T, tester DatastoreTester) {
	req := require.New(t)
	ds, err := tester.New(0*time.Second, veryLargeGCInterval, veryLargeGCWindow, 1)
	req.NoError(err)

	skipIfNotCaveatStorer(t, ds)

	// Write an initial caveat
	coreCaveat := createCoreCaveat(t)
	ctx := context.Background()
	oldRev, err := writeCaveat(ctx, ds, coreCaveat)
	req.NoError(err)

	// Modify caveat and update
	oldExpression := coreCaveat.SerializedExpression
	newExpression := []byte{0x0a}
	coreCaveat.SerializedExpression = newExpression
	newRev, err := writeCaveat(ctx, ds, coreCaveat)
	req.NoError(err)

	// check most recent revision
	cr := ds.SnapshotReader(newRev)
	cv, _, err := cr.ReadCaveatByName(ctx, coreCaveat.Name)
	req.NoError(err)
	req.Equal(newExpression, cv.SerializedExpression)

	// check previous revision
	cr = ds.SnapshotReader(oldRev)
	cv, _, err = cr.ReadCaveatByName(ctx, coreCaveat.Name)
	req.NoError(err)
	req.Equal(oldExpression, cv.SerializedExpression)
}

func CaveatedRelationshipWatchTest(t *testing.T, tester DatastoreTester) {
	req := require.New(t)
	ds, err := tester.New(0*time.Second, veryLargeGCInterval, veryLargeGCWindow, 16)
	req.NoError(err)

	skipIfNotCaveatStorer(t, ds)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Write caveat and caveated relationship
	// TODO bug(postgres): Watch API won't send updates if revision used is the first revision, so write something first
	coreCaveat := createCoreCaveat(t)
	_, err = writeCaveat(ctx, ds, coreCaveat)
	req.NoError(err)

	// test relationship with caveat and context
	relWithContext := createTestCaveatedRel(t, "document:a#parent@folder:company#...", coreCaveat.Name)

	revBeforeWrite, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	writeRev, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, relWithContext)
	require.NoError(t, err)
	require.NotEqual(t, revBeforeWrite, writeRev, "found same transaction IDs: %v and %v", revBeforeWrite, writeRev)

	expectRelChange(t, ds, revBeforeWrite, relWithContext)

	// test relationship with caveat and empty context
	tupleWithEmptyContext := createTestCaveatedRel(t, "document:b#parent@folder:company#...", coreCaveat.Name)
	strct, err := structpb.NewStruct(nil)

	req.NoError(err)
	tupleWithEmptyContext.OptionalCaveat.Context = strct

	secondRevBeforeWrite, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	secondWriteRev, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, tupleWithEmptyContext)
	require.NoError(t, err)
	require.NotEqual(t, secondRevBeforeWrite, secondWriteRev)

	expectRelChange(t, ds, secondRevBeforeWrite, tupleWithEmptyContext)

	// test relationship with caveat and empty context
	tupleWithNilContext := createTestCaveatedRel(t, "document:c#parent@folder:company#...", coreCaveat.Name)
	tupleWithNilContext.OptionalCaveat.Context = nil

	thirdRevBeforeWrite, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	thirdWriteRev, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, tupleWithNilContext)
	req.NoError(err)
	require.NotEqual(t, thirdRevBeforeWrite, thirdWriteRev)

	tupleWithNilContext.OptionalCaveat.Context = &structpb.Struct{} // nil struct comes back as zero-value struct
	expectRelChange(t, ds, thirdRevBeforeWrite, tupleWithNilContext)
}

func expectRelChange(t *testing.T, ds datastore.Datastore, revBeforeWrite datastore.Revision, expectedRel tuple.Relationship) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chanRevisionChanges, chanErr := ds.Watch(ctx, revBeforeWrite, datastore.WatchJustRelationships())
	require.Zero(t, len(chanErr))

	changeWait := time.NewTimer(waitForChangesTimeout)
	select {
	case change, ok := <-chanRevisionChanges:
		require.True(t, ok)
		require.True(t, tuple.Equal(change.RelationshipChanges[0].Relationship, expectedRel))
	case <-changeWait.C:
		require.Fail(t, "timed out waiting for relationship update via Watch API")
	}
}

func expectRel(req *require.Assertions, iter datastore.RelationshipIterator, rel tuple.Relationship) {
	for found, err := range iter {
		req.NoError(err)
		req.True(tuple.Equal(found, rel))
	}
}

func assertRelCorrectlyStored(req *require.Assertions, ds datastore.Datastore, rev datastore.Revision, expected tuple.Relationship) {
	iter, err := ds.SnapshotReader(rev).QueryRelationships(context.Background(), datastore.RelationshipsFilter{
		OptionalResourceType: expected.Resource.ObjectType,
	})
	req.NoError(err)

	for found, err := range iter {
		req.NoError(err)
		req.True(tuple.Equal(found, expected))
	}
}

func skipIfNotCaveatStorer(t *testing.T, ds datastore.Datastore) {
	ctx := context.Background()
	_, _ = ds.ReadWriteTx(ctx, func(ctx context.Context, transaction datastore.ReadWriteTransaction) error { // nolint: errcheck
		_, _, err := transaction.ReadCaveatByName(ctx, uuid.NewString())
		if !errors.As(err, &datastore.CaveatNameNotFoundError{}) {
			t.Skip("datastore does not implement CaveatStorer interface")
		}
		return fmt.Errorf("force rollback of unnecesary tx")
	})
}

func createTestCaveatedRel(t *testing.T, relString string, caveatName string) tuple.Relationship {
	rel := tuple.MustParse(relString)
	st, err := structpb.NewStruct(map[string]interface{}{"a": 1, "b": "test"})
	require.NoError(t, err)
	return rel.WithCaveat(&core.ContextualizedCaveat{
		CaveatName: caveatName,
		Context:    st,
	})
}

func writeCaveats(ctx context.Context, ds datastore.Datastore, coreCaveat ...*core.CaveatDefinition) (datastore.Revision, error) {
	rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteCaveats(ctx, coreCaveat)
	})
	if err != nil {
		return datastore.NoRevision, err
	}
	return rev, err
}

func writeCaveat(ctx context.Context, ds datastore.Datastore, coreCaveat *core.CaveatDefinition) (datastore.Revision, error) {
	rev, err := writeCaveats(ctx, ds, coreCaveat)
	if err != nil {
		return datastore.NoRevision, err
	}
	return rev, nil
}

func createCoreCaveat(t *testing.T) *core.CaveatDefinition {
	t.Helper()
	c := createCompiledCaveat(t)
	cBytes, err := c.Serialize()
	require.NoError(t, err)

	env := caveats.NewEnvironment()

	err = env.AddVariable("foo", caveattypes.IntType)
	require.NoError(t, err)

	err = env.AddVariable("bar", caveattypes.MustMapType(caveattypes.BytesType))
	require.NoError(t, err)

	coreCaveat := &core.CaveatDefinition{
		Name:                 c.Name(),
		SerializedExpression: cBytes,
		ParameterTypes:       env.EncodedParametersTypes(),
	}
	require.NoError(t, err)

	return coreCaveat
}

func createCompiledCaveat(t *testing.T) *caveats.CompiledCaveat {
	t.Helper()
	env, err := caveats.EnvForVariables(map[string]caveattypes.VariableType{
		"a": caveattypes.IntType,
		"b": caveattypes.IntType,
	})
	require.NoError(t, err)

	c, err := caveats.CompileCaveatWithName(env, "a == b", uuid.New().String())
	require.NoError(t, err)

	return c
}
