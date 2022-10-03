package caveat

import (
	"context"
	"errors"
	"testing"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

const testSchema string = `
definition user {}

definition organization {
	relation admin: user
}
		
definition document {
	relation org: organization
	relation viewer: user
	relation editor: user

	permission edit = editor + org->admin
	permission view = viewer + edit
}`

type caveatDefinition struct {
	name       string
	expression string
}

type caveatedUpdate struct {
	Operation  core.RelationTupleUpdate_Operation
	tuple      string
	caveatName string
	contxt     map[string]any
}

func TestComputeCheckWithCaveats(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	dispatch := NewCaveatEvaluatingDispatcher(graph.NewLocalOnlyDispatcher(10))
	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
	require.NoError(t, datastoremw.SetInContext(ctx, ds))

	type result struct {
		check  string
		member v1.ResourceCheckResult_Membership
	}

	testCases := []struct {
		name    string
		caveats []caveatDefinition
		env     map[string]caveats.VariableType
		updates []caveatedUpdate
		results []result
	}{
		{
			"simple test",
			[]caveatDefinition{
				{"testcaveat", "somecondition == 42 && somebool"},
				{"anothercaveat", "int(anothercondition) == 15"},
			},
			map[string]caveats.VariableType{
				"somecondition":    caveats.IntType,
				"somebool":         caveats.BooleanType,
				"anothercondition": caveats.UIntType,
			},
			[]caveatedUpdate{
				{core.RelationTupleUpdate_CREATE, "organization:someorg#admin@user:sarah", "testcaveat", map[string]any{"anothercondition": 15}},
				{core.RelationTupleUpdate_CREATE, "organization:someorg#admin@user:john", "testcaveat", map[string]any{"somecondition": 42, "somebool": true}},
				{core.RelationTupleUpdate_CREATE, "organization:someorg#admin@user:jane", "", nil},
				{core.RelationTupleUpdate_CREATE, "document:foo#org@organization:someorg", "anothercaveat", map[string]any{"anothercondition": 15}},
				{core.RelationTupleUpdate_CREATE, "document:bar#org@organization:someorg", "", nil},
			},
			[]result{
				{"document:foo#view@user:sarah", v1.ResourceCheckResult_CAVEATED_MEMBER},
				{"document:foo#view@user:john", v1.ResourceCheckResult_CAVEATED_MEMBER},
				{"document:bar#view@user:jane", v1.ResourceCheckResult_MEMBER},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			e, err := caveats.EnvForVariables(tt.env)
			require.NoError(t, err)
			revision, err := writeCaveatedTuples(ctx, ds, testSchema, e, tt.caveats, tt.updates)
			require.NoError(t, err)

			for _, r := range tt.results {
				rt := tuple.Parse(r.check)

				checkResult, err := dispatch.DispatchCheck(ctx, &v1.DispatchCheckRequest{
					ResourceRelation: &core.RelationReference{
						Namespace: rt.ResourceAndRelation.Namespace,
						Relation:  rt.ResourceAndRelation.Relation,
					},
					ResourceIds:    []string{rt.ResourceAndRelation.ObjectId},
					ResultsSetting: v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
					Subject:        rt.Subject,
					Metadata: &v1.ResolverMeta{
						AtRevision:     revision.String(),
						DepthRemaining: 50,
					},
				})

				require.NoError(t, err)
				resultMap := checkResult.GetResultsByResourceId()
				require.Contains(t, resultMap, rt.ResourceAndRelation.ObjectId)
				result := resultMap[rt.ResourceAndRelation.ObjectId]
				require.Equal(t, r.member, result.Membership)
			}
		})
	}
}

func writeCaveatedTuples(ctx context.Context, ds datastore.Datastore, schema string, env *caveats.Environment, rawCaveats []caveatDefinition, updates []caveatedUpdate) (datastore.Revision, error) {
	empty := ""
	defs, err := compiler.Compile([]compiler.InputSchema{
		{Source: "schema", SchemaString: schema},
	}, &empty)
	if err != nil {
		return datastore.NoRevision, err
	}

	compiledCaveats := make([]*core.Caveat, 0, len(rawCaveats))
	for _, c := range rawCaveats {
		compiled, err := caveats.CompileCaveatWithName(env, c.expression, c.name)
		if err != nil {
			return datastore.NoRevision, err
		}
		serialized, err := compiled.Serialize()
		if err != nil {
			return datastore.NoRevision, err
		}
		compiledCaveats = append(compiledCaveats, &core.Caveat{Name: c.name, Expression: serialized})
	}

	return ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		if err := rwt.WriteNamespaces(defs...); err != nil {
			return err
		}

		cs, ok := rwt.(datastore.CaveatStorer)
		if !ok {
			return errors.New("datastore does not implement CaveatStorer")
		}
		if err := cs.WriteCaveats(compiledCaveats); err != nil {
			return err
		}

		var rtu []*core.RelationTupleUpdate
		for _, updt := range updates {
			rtu = append(rtu, &core.RelationTupleUpdate{
				Operation: updt.Operation,
				Tuple:     caveatedRelationTuple(updt.tuple, updt.caveatName, updt.contxt),
			})
		}

		return rwt.WriteRelationships(rtu)
	})
}

func caveatedRelationTuple(relationTuple string, caveatName string, context map[string]any) *core.RelationTuple {
	c := tuple.Parse(relationTuple)
	strct, err := structpb.NewStruct(context)
	if err != nil {
		panic(err)
	}
	if caveatName != "" {
		c.Caveat = &core.ContextualizedCaveat{
			CaveatName: caveatName,
			Context:    strct,
		}
	}
	return c
}
