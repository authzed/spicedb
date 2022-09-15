package memdb

import (
	"context"
	"math"
	"testing"
	"time"

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
	coreCaveat := createCoreCaveat(t)
	ctx := context.Background()

	// Fails to write dupes in the same transaction
	_, _, err = writeCaveat(ctx, ds, coreCaveat, coreCaveat)
	req.Error(err)

	// Succeeds writing a caveat
	rev, ID, err := writeCaveat(ctx, ds, coreCaveat)
	req.NoError(err)

	// fails to write caveat with the same name in different tx
	_, _, err = writeCaveat(ctx, ds, coreCaveat)
	req.Error(err)

	// the caveat can be looked up by name
	cr, ok := ds.SnapshotReader(rev).(datastore.CaveatReader)
	req.True(ok, "expected a CaveatStorer value")
	cv, err := cr.ReadCaveatByName(coreCaveat.Name)
	req.NoError(err)
	req.Equal(coreCaveat, cv)
	req.NoError(err)

	// the caveat can be looked up by ID
	cv, err = cr.ReadCaveatByID(ID)
	req.NoError(err)
	req.Equal(coreCaveat, cv)
	req.NoError(err)

	// returns an error if caveat name or ID does not exist
	_, err = cr.ReadCaveatByName("doesnotexist")
	req.ErrorIs(err, datastore.ErrCaveatNotFound)
	_, err = cr.ReadCaveatByID(math.MaxUint64)
	req.ErrorIs(err, datastore.ErrCaveatNotFound)
}

func TestWriteTupleWithCaveat(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	req.NoError(err)
	sds, _ := testfixtures.StandardDatastoreWithSchema(ds, req)
	tpl := createTestCaveatedTuple(t, "document:companyplan#parent@folder:company#...", math.MaxUint64)
	// should fail because the caveat is not present in the datastore
	_, err = common.WriteTuples(ctx, sds, core.RelationTupleUpdate_CREATE, tpl)
	req.ErrorIs(err, datastore.ErrCaveatNotFound)

	// let's write the caveat and try again
	coreCaveat := createCoreCaveat(t)
	_, cavID, err := writeCaveat(ctx, ds, coreCaveat)
	req.NoError(err)
	tpl = createTestCaveatedTuple(t, "document:companyplan#parent@folder:company#...", cavID)
	rev, err := common.WriteTuples(ctx, sds, core.RelationTupleUpdate_CREATE, tpl)
	req.NoError(err)
	iter, err := ds.SnapshotReader(rev).QueryRelationships(ctx, datastore.RelationshipsFilter{
		ResourceType: tpl.ResourceAndRelation.Namespace,
	})
	req.NoError(err)
	defer iter.Close()
	readTpl := iter.Next()
	req.Equal(tpl, readTpl)
}

func createTestCaveatedTuple(t *testing.T, tplString string, id datastore.CaveatID) *core.RelationTuple {
	tpl := tuple.MustParse(tplString)
	st, err := structpb.NewStruct(map[string]interface{}{"a": 1, "b": "test"})
	require.NoError(t, err)
	tpl.Caveat = &core.ContextualizedCaveat{
		CaveatId: uint64(id),
		Context:  st,
	}
	return tpl
}

func writeCaveat(ctx context.Context, ds datastore.Datastore, coreCaveat ...*core.Caveat) (datastore.Revision, datastore.CaveatID, error) {
	var IDs []datastore.CaveatID
	rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		cs, ok := tx.(datastore.CaveatStorer)
		if !ok {
			panic("expected a CaveatStorer value")
		}
		var err error
		IDs, err = cs.WriteCaveats(coreCaveat)
		return err
	})
	if err != nil {
		return datastore.NoRevision, 0, err
	}
	return rev, IDs[0], err
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
