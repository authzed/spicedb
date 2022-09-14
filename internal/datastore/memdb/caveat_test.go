package memdb

import (
	"context"
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestWriteReadCaveat(t *testing.T) {
	req := require.New(t)

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	req.NoError(err)
	c := createCompiledCaveat(t)
	coreCaveat := createCoreCaveat(t, c, false)
	ctx := context.Background()
	rev, err := writeCaveat(ctx, ds, coreCaveat)
	req.NoError(err)
	cr, ok := ds.SnapshotReader(rev).(datastore.CaveatReader)
	req.True(ok, "expected a CaveatStorer value")
	it, err := cr.ReadCaveat(c.Name())
	req.NoError(err)
	cv := it.Next()
	req.Equal(coreCaveat, cv)
	req.Equal(core.Caveat_NAMED, cv.Type)
	req.NoError(err)

	it, err = cr.ReadCaveat("doesnotexist")
	req.NoError(err)
	cv = it.Next()
	req.Nil(cv)
}

func TestWriteTupleWithNamedCaveat(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	req.NoError(err)
	sds, _ := testfixtures.StandardDatastoreWithSchema(ds, req)
	tpl, coreCaveat := createTestCaveatedTuple(t, "document:companyplan#parent@folder:company#...", false)
	_, err = common.WriteTuples(ctx, sds, core.RelationTupleUpdate_CREATE, tpl)
	// should fail because the name caveat is not present in the datastore
	req.Error(err)
	// let's write the named caveat and try again
	_, err = writeCaveat(ctx, ds, coreCaveat)
	req.NoError(err)
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

func TestWriteTupleWithAnonymousCaveat(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	req.NoError(err)
	sds, _ := testfixtures.StandardDatastoreWithSchema(ds, req)
	tpl, _ := createTestCaveatedTuple(t, "document:companyplan#parent@folder:company#...", true)
	rev, err := common.WriteTuples(ctx, sds, core.RelationTupleUpdate_CREATE, tpl)
	// the caveat is anonymous and is created alongside the tuple
	req.NoError(err)
	iter, err := ds.SnapshotReader(rev).QueryRelationships(ctx, datastore.RelationshipsFilter{
		ResourceType: tpl.ResourceAndRelation.Namespace,
	})
	req.NoError(err)
	defer iter.Close()
	readTpl := iter.Next()
	req.Equal(tpl, readTpl)
}

func createTestCaveatedTuple(t *testing.T, tplString string, anonymous bool) (*core.RelationTuple, *core.Caveat) {
	tpl := tuple.MustParse(tplString)
	c := createCompiledCaveat(t)
	coreCaveat := createCoreCaveat(t, c, anonymous)

	st, err := structpb.NewStruct(map[string]interface{}{"a": 1, "b": "test"})
	require.NoError(t, err)
	tpl.Caveat = &core.CaveatReference{
		Caveat:  coreCaveat,
		Context: st,
	}
	return tpl, coreCaveat
}

func writeCaveat(ctx context.Context, ds datastore.Datastore, coreCaveat *core.Caveat) (datastore.Revision, error) {
	return ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		cs, ok := tx.(datastore.CaveatStorer)
		if !ok {
			panic("expected a CaveatStorer value")
		}
		return cs.WriteCaveats([]*core.Caveat{coreCaveat})
	})
}

func createCoreCaveat(t *testing.T, c *caveats.CompiledCaveat, anonymous bool) *core.Caveat {
	t.Helper()
	cBytes, err := c.Serialize()
	require.NoError(t, err)
	ty := core.Caveat_NAMED
	if anonymous {
		ty = core.Caveat_ANONYMOUS
	}
	coreCaveat := &core.Caveat{
		Name:       c.Name(),
		Expression: cBytes,
		Type:       ty,
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
	c, err := caveats.CompileCaveatWithName(env, "a == b", "test")
	require.NoError(t, err)
	return c
}
