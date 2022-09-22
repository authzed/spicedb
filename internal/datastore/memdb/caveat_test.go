package memdb

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestWriteReadCaveat(t *testing.T) {
	req := require.New(t)

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	req.NoError(err)

	// Dupes in same transaction are treated as upserts
	coreCaveat := createCoreCaveat(t)
	ctx := context.Background()
	_, err = writeCaveats(ctx, ds, coreCaveat, coreCaveat)
	req.NoError(err)

	// Succeeds upserting an existing caveat
	rev, err := writeCaveat(ctx, ds, coreCaveat)
	req.NoError(err)

	// The caveat can be looked up by name
	cr, ok := ds.SnapshotReader(rev).(datastore.CaveatReader)
	req.True(ok, "expected a CaveatStorer value")

	cv, err := cr.ReadCaveatByName(coreCaveat.Name)
	req.NoError(err)
	foundDiff := cmp.Diff(coreCaveat, cv, protocmp.Transform())
	req.Empty(foundDiff)

	// Returns an error if caveat name or ID does not exist
	_, err = cr.ReadCaveatByName("doesnotexist")
	req.ErrorAs(err, &datastore.ErrCaveatNameNotFound{})
}

func TestWriteCaveatedTuple(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	req.NoError(err)
	sds, _ := testfixtures.StandardDatastoreWithSchema(ds, req)

	// Store caveat, write caveated tuple and read back same value
	coreCaveat := createCoreCaveat(t)
	_, err = writeCaveat(ctx, ds, coreCaveat)
	req.NoError(err)

	tpl := createTestCaveatedTuple(t, "document:companyplan#parent@folder:company#...", coreCaveat.Name)
	rev, err := common.WriteTuples(ctx, sds, core.RelationTupleUpdate_CREATE, tpl)
	req.NoError(err)
	iter, err := ds.SnapshotReader(rev).QueryRelationships(ctx, datastore.RelationshipsFilter{
		ResourceType: tpl.ResourceAndRelation.Namespace,
	})
	req.NoError(err)

	defer iter.Close()
	readTpl := iter.Next()
	foundDiff := cmp.Diff(tpl, readTpl, protocmp.Transform())
	req.Empty(foundDiff)

	// Caveated tuple can reference non-existing caveat - controller layer is responsible for validation
	tpl = createTestCaveatedTuple(t, "document:rando#parent@folder:company#...", "rando")
	_, err = common.WriteTuples(ctx, sds, core.RelationTupleUpdate_CREATE, tpl)
	req.NoError(err)
}

func TestCaveatSnapshotReads(t *testing.T) {
	req := require.New(t)

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	req.NoError(err)

	// Write an initial caveat
	coreCaveat := createCoreCaveat(t)
	ctx := context.Background()
	oldRev, err := writeCaveat(ctx, ds, coreCaveat)
	req.NoError(err)

	// Modify caveat and update
	oldExpression := coreCaveat.Expression
	newExpression := []byte{0x0a}
	coreCaveat.Expression = newExpression
	newRev, err := writeCaveat(ctx, ds, coreCaveat)
	req.NoError(err)

	// check most recent revision
	cr, ok := ds.SnapshotReader(newRev).(datastore.CaveatReader)
	req.True(ok, "expected a CaveatStorer value")
	cv, err := cr.ReadCaveatByName(coreCaveat.Name)
	req.NoError(err)
	req.Equal(newExpression, cv.Expression)

	// check previous revision
	cr, ok = ds.SnapshotReader(oldRev).(datastore.CaveatReader)
	req.True(ok, "expected a CaveatStorer value")
	cv, err = cr.ReadCaveatByName(coreCaveat.Name)
	req.NoError(err)
	req.Equal(oldExpression, cv.Expression)
}

func createTestCaveatedTuple(t *testing.T, tplString string, caveatName string) *core.RelationTuple {
	tpl := tuple.MustParse(tplString)
	st, err := structpb.NewStruct(map[string]interface{}{"a": 1, "b": "test"})
	require.NoError(t, err)

	tpl.Caveat = &core.ContextualizedCaveat{
		CaveatName: caveatName,
		Context:    st,
	}
	return tpl
}

func writeCaveats(ctx context.Context, ds datastore.Datastore, coreCaveat ...*core.Caveat) (datastore.Revision, error) {
	rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		cs, ok := tx.(datastore.CaveatStorer)
		if !ok {
			panic("expected a CaveatStorer value")
		}
		return cs.WriteCaveats(coreCaveat)
	})
	if err != nil {
		return datastore.NoRevision, err
	}
	return rev, err
}

func writeCaveat(ctx context.Context, ds datastore.Datastore, coreCaveat *core.Caveat) (datastore.Revision, error) {
	rev, err := writeCaveats(ctx, ds, coreCaveat)
	if err != nil {
		return datastore.NoRevision, err
	}
	return rev, nil
}

func createCoreCaveat(t *testing.T) *core.Caveat {
	t.Helper()
	c := createCompiledCaveat(t)
	cBytes, err := c.Serialize()
	require.NoError(t, err)

	coreCaveat := &core.Caveat{
		Name:       c.Name(),
		Expression: cBytes,
	}
	require.NoError(t, err)

	return coreCaveat
}

func createCompiledCaveat(t *testing.T) *caveats.CompiledCaveat {
	t.Helper()
	env, err := caveats.EnvForVariables(map[string]caveats.VariableType{
		"a": caveats.IntType,
		"b": caveats.IntType,
	})
	require.NoError(t, err)

	c, err := caveats.CompileCaveatWithName(env, "a == b", uuid.New().String())
	require.NoError(t, err)

	return c
}
