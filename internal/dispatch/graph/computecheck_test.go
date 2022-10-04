package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/authzed/spicedb/pkg/caveats/customtypes"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

type caveatDefinition struct {
	expression string
	env        map[string]caveats.VariableType
}

type caveatedUpdate struct {
	Operation  core.RelationTupleUpdate_Operation
	tuple      string
	caveatName string
	context    map[string]any
}

func TestComputeCheckWithCaveats(t *testing.T) {
	type check struct {
		check                 string
		context               map[string]any
		member                v1.ResourceCheckResult_Membership
		expectedMissingFields []string
		error                 string
	}

	testCases := []struct {
		name    string
		schema  string
		caveats map[string]caveatDefinition
		updates []caveatedUpdate
		checks  []check
	}{
		{
			"simple test",
			`definition user {}

			definition organization {
				relation admin: user
			}
					
			definition document {
				relation org: organization
				relation viewer: user
				relation editor: user

				permission edit = editor + org->admin
				permission view = viewer + edit
			}`,
			map[string]caveatDefinition{
				"testcaveat": {
					"somecondition == 42 && somebool",
					map[string]caveats.VariableType{
						"somecondition": caveats.IntType,
						"somebool":      caveats.BooleanType,
					},
				},
				"anothercaveat": {
					"int(anothercondition) == 15",
					map[string]caveats.VariableType{
						"anothercondition": caveats.UIntType,
					},
				},
			},
			[]caveatedUpdate{
				{core.RelationTupleUpdate_CREATE, "organization:someorg#admin@user:sarah", "testcaveat", nil},
				{core.RelationTupleUpdate_CREATE, "organization:someorg#admin@user:john", "testcaveat", map[string]any{"somecondition": 42, "somebool": true}},
				{core.RelationTupleUpdate_CREATE, "organization:someorg#admin@user:jane", "", nil},
				{core.RelationTupleUpdate_CREATE, "document:foo#org@organization:someorg", "anothercaveat", nil},
				{core.RelationTupleUpdate_CREATE, "document:bar#org@organization:someorg", "", nil},
				{core.RelationTupleUpdate_CREATE, "document:foo#editor@user:vic", "testcaveat", map[string]any{"somecondition": 42, "somebool": true}},
				{core.RelationTupleUpdate_CREATE, "document:foo#viewer@user:vic", "testcaveat", map[string]any{"somecondition": 42, "somebool": true}},
				{core.RelationTupleUpdate_CREATE, "document:foo#viewer@user:blippy", "testcaveat", map[string]any{"somecondition": 42, "somebool": "hi"}},
				{core.RelationTupleUpdate_CREATE, "document:foo#viewer@user:noa", "testcaveat", map[string]any{"somecondition": 42, "somebool": "hi"}},
				{core.RelationTupleUpdate_CREATE, "document:foo#editor@user:noa", "testcaveat", map[string]any{"somecondition": 42, "somebool": "hi"}},
				{core.RelationTupleUpdate_CREATE, "document:foo#editor@user:wayne", "invalid", nil},
			},
			[]check{
				{
					"document:foo#view@user:sarah",
					nil,
					v1.ResourceCheckResult_CAVEATED_MEMBER,
					[]string{"anothercondition"},
					"",
				},
				{
					"document:foo#view@user:sarah",
					map[string]any{
						"somecondition": 42,
					},
					v1.ResourceCheckResult_CAVEATED_MEMBER,
					[]string{"anothercondition"},
					"",
				},
				{
					"document:foo#view@user:sarah",
					map[string]any{
						"anothercondition": 15,
					},
					v1.ResourceCheckResult_CAVEATED_MEMBER,
					[]string{"somecondition"},
					"",
				},
				{
					"document:foo#view@user:sarah",
					map[string]any{
						"somecondition":    42,
						"anothercondition": 15,
						"somebool":         true,
					},
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view@user:john",
					map[string]any{
						"anothercondition": 14,
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view@user:john",
					map[string]any{
						"anothercondition": 15,
					},
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
				{
					"document:bar#view@user:jane", nil, v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view@user:peter",
					nil,
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view@user:vic",
					nil,
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view@user:blippy",
					nil,
					v1.ResourceCheckResult_MEMBER,
					nil,
					"no such overload",
				},
				{
					"document:foo#view@user:noa",
					nil,
					v1.ResourceCheckResult_MEMBER,
					nil,
					"no such overload",
				},
				{
					"document:foo#view@user:wayne",
					nil,
					v1.ResourceCheckResult_MEMBER,
					nil,
					"caveat with name `invalid` not found",
				},
			},
		},
		{
			"overridden context test",
			`definition user {}

			definition document {
				relation viewer: user
				permission view = viewer
			}`,
			map[string]caveatDefinition{
				"testcaveat": {
					"somecondition == 42",
					map[string]caveats.VariableType{
						"somecondition": caveats.IntType,
					},
				},
			},
			[]caveatedUpdate{
				{core.RelationTupleUpdate_CREATE, "document:foo#viewer@user:tom", "testcaveat", map[string]any{
					"somecondition": 41, // not allowed
				}},
			},
			[]check{
				{
					"document:foo#view@user:tom",
					nil,
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view@user:tom",
					map[string]any{
						"somecondition": 42, // still not a member, because the written value takes precedence
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
			},
		},
		{
			"intersection test",
			`definition user {}

			definition document {
				relation viewer: user
				relation editor: user
				permission view_and_edit = viewer & editor
			}`,
			map[string]caveatDefinition{
				"viewcaveat": {
					"somecondition == 42",
					map[string]caveats.VariableType{
						"somecondition": caveats.IntType,
					},
				},
				"editcaveat": {
					"today == 'tuesday'",
					map[string]caveats.VariableType{
						"today": caveats.StringType,
					},
				},
			},
			[]caveatedUpdate{
				{
					core.RelationTupleUpdate_CREATE,
					"document:foo#viewer@user:tom",
					"viewcaveat",
					nil,
				},
				{
					core.RelationTupleUpdate_CREATE,
					"document:foo#editor@user:tom",
					"editcaveat",
					nil,
				},
			},
			[]check{
				{
					"document:foo#view_and_edit@user:tom",
					map[string]any{
						"somecondition": 42,
						"today":         "wednesday",
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view_and_edit@user:tom",
					map[string]any{
						"somecondition": 41,
						"today":         "tuesday",
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view_and_edit@user:tom",
					map[string]any{
						"somecondition": 42,
						"today":         "tuesday",
					},
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
			},
		},
		{
			"exclusion test",
			`definition user {}

			definition document {
				relation viewer: user
				relation banned: user
				permission view_not_banned = viewer - banned
			}`,
			map[string]caveatDefinition{
				"viewcaveat": {
					"somecondition == 42",
					map[string]caveats.VariableType{
						"somecondition": caveats.IntType,
					},
				},
				"bannedcaveat": {
					"region == 'bad'",
					map[string]caveats.VariableType{
						"region": caveats.StringType,
					},
				},
			},
			[]caveatedUpdate{
				{
					core.RelationTupleUpdate_CREATE,
					"document:foo#viewer@user:tom",
					"viewcaveat",
					nil,
				},
				{
					core.RelationTupleUpdate_CREATE,
					"document:foo#banned@user:tom",
					"bannedcaveat",
					nil,
				},
			},
			[]check{
				{
					"document:foo#view_not_banned@user:tom",
					map[string]any{
						"somecondition": 42,
						"region":        "bad",
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view_not_banned@user:tom",
					map[string]any{
						"somecondition": 41,
						"region":        "good",
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view_not_banned@user:tom",
					map[string]any{
						"somecondition": 42,
						"region":        "good",
					},
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
			},
		},
		{
			"IP Allowlists example",
			`definition user {}

					definition organization {
						relation members: user
						relation ip_allowlist_policy: organization#members
					
						permission policy = ip_allowlist_policy
					}
					
					definition repository {
						relation owner: organization
						relation reader: user
					
						permission read = reader & owner->policy
					}`,
			map[string]caveatDefinition{
				"ip_allowlist": {
					"user_ip.in_cidr(cidr)",
					map[string]caveats.VariableType{
						"user_ip": caveats.IPAddressType,
						"cidr":    caveats.StringType,
					},
				},
			},
			[]caveatedUpdate{
				{
					core.RelationTupleUpdate_CREATE,
					"repository:foobar#owner@organization:myorg",
					"",
					nil,
				},
				{
					core.RelationTupleUpdate_CREATE,
					"organization:myorg#members@user:johndoe",
					"",
					nil,
				},
				{
					core.RelationTupleUpdate_CREATE,
					"repository:foobar#reader@user:johndoe",
					"",
					nil,
				},
				{
					core.RelationTupleUpdate_CREATE,
					"organization:myorg#ip_allowlist_policy@organization:myorg#members",
					"ip_allowlist",
					map[string]any{
						"cidr": "192.168.0.0/16",
					},
				},
			},
			[]check{
				{
					"repository:foobar#read@user:johndoe",
					nil,
					v1.ResourceCheckResult_CAVEATED_MEMBER,
					[]string{"user_ip"},
					"",
				},
				{
					"repository:foobar#read@user:johndoe",
					map[string]any{
						"user_ip": customtypes.MustParseIPAddress("192.168.0.1"),
					},
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
				{
					"repository:foobar#read@user:johndoe",
					map[string]any{
						"user_ip": customtypes.MustParseIPAddress("9.2.3.1"),
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(t, err)

			dispatch := NewLocalOnlyDispatcher(10)
			ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
			require.NoError(t, datastoremw.SetInContext(ctx, ds))

			revision, err := writeCaveatedTuples(ctx, ds, tt.schema, tt.caveats, tt.updates)
			require.NoError(t, err)

			for _, r := range tt.checks {
				t.Run(r.check, func(t *testing.T) {
					rel := tuple.MustParse(r.check)

					result, _, err := ComputeCheck(ctx, dispatch,
						CheckParameters{
							ResourceType: &core.RelationReference{
								Namespace: rel.ResourceAndRelation.Namespace,
								Relation:  rel.ResourceAndRelation.Relation,
							},
							ResourceID:         rel.ResourceAndRelation.ObjectId,
							Subject:            rel.Subject,
							CaveatContext:      r.context,
							AtRevision:         revision,
							MaximumDepth:       50,
							IsDebuggingEnabled: true,
						})

					if r.error != "" {
						require.Equal(t, err.Error(), r.error)
					} else {
						require.NoError(t, err)
						require.Equal(t, v1.ResourceCheckResult_Membership_name[int32(r.member)], v1.ResourceCheckResult_Membership_name[int32(result.Membership)], "mismatch for %s with context %v", r.check, r.context)

						if result.Membership == v1.ResourceCheckResult_CAVEATED_MEMBER {
							require.Equal(t, r.expectedMissingFields, result.MissingExprFields)
						}
					}
				})
			}
		})
	}
}

func TestComputeCheckError(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	dispatch := NewLocalOnlyDispatcher(10)
	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
	require.NoError(t, datastoremw.SetInContext(ctx, ds))

	_, _, err = ComputeCheck(ctx, dispatch, CheckParameters{
		ResourceType: &core.RelationReference{
			Namespace: "a",
			Relation:  "b",
		},
		ResourceID:         "id",
		Subject:            &core.ObjectAndRelation{},
		CaveatContext:      nil,
		AtRevision:         datastore.NoRevision,
		MaximumDepth:       50,
		IsDebuggingEnabled: true,
	})
	require.Error(t, err)
}

func writeCaveatedTuples(ctx context.Context, ds datastore.Datastore, schema string, definedCaveats map[string]caveatDefinition, updates []caveatedUpdate) (datastore.Revision, error) {
	empty := ""
	defs, err := compiler.Compile([]compiler.InputSchema{
		{Source: "schema", SchemaString: schema},
	}, &empty)
	if err != nil {
		return datastore.NoRevision, err
	}

	compiledCaveats := make([]*core.Caveat, 0, len(definedCaveats))
	for name, c := range definedCaveats {
		e, err := caveats.EnvForVariables(c.env)
		if err != nil {
			return datastore.NoRevision, err
		}

		compiled, err := caveats.CompileCaveatWithName(e, c.expression, name)
		if err != nil {
			return datastore.NoRevision, err
		}
		serialized, err := compiled.Serialize()
		if err != nil {
			return datastore.NoRevision, err
		}
		compiledCaveats = append(compiledCaveats, &core.Caveat{Name: name, Expression: serialized})
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
				Tuple:     caveatedRelationTuple(updt.tuple, updt.caveatName, updt.context),
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
