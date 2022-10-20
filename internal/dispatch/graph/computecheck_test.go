package graph

import (
	"context"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/stretchr/testify/require"
)

type caveatDefinition struct {
	expression string
	env        map[string]types.VariableType
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
					map[string]types.VariableType{
						"somecondition": types.IntType,
						"somebool":      types.BooleanType,
					},
				},
				"anothercaveat": {
					"int(anothercondition) == 15",
					map[string]types.VariableType{
						"anothercondition": types.UIntType,
					},
				},
			},
			[]caveatedUpdate{
				{core.RelationTupleUpdate_CREATE, "organization:someorg#admin@user:sarah", "testcaveat", nil},
				{core.RelationTupleUpdate_CREATE, "organization:someorg#admin@user:john", "testcaveat", map[string]any{"somecondition": "42", "somebool": true}},
				{core.RelationTupleUpdate_CREATE, "organization:someorg#admin@user:jane", "", nil},
				{core.RelationTupleUpdate_CREATE, "document:foo#org@organization:someorg", "anothercaveat", nil},
				{core.RelationTupleUpdate_CREATE, "document:bar#org@organization:someorg", "", nil},
				{core.RelationTupleUpdate_CREATE, "document:foo#editor@user:vic", "testcaveat", map[string]any{"somecondition": "42", "somebool": true}},
				{core.RelationTupleUpdate_CREATE, "document:foo#viewer@user:vic", "testcaveat", map[string]any{"somecondition": "42", "somebool": true}},
				{core.RelationTupleUpdate_CREATE, "document:foo#viewer@user:blippy", "testcaveat", map[string]any{"somecondition": "42", "somebool": false}},
				{core.RelationTupleUpdate_CREATE, "document:foo#viewer@user:noa", "testcaveat", map[string]any{"somecondition": "42", "somebool": false}},
				{core.RelationTupleUpdate_CREATE, "document:foo#editor@user:noa", "testcaveat", map[string]any{"somecondition": "42", "somebool": false}},
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
						"somecondition": "42",
					},
					v1.ResourceCheckResult_CAVEATED_MEMBER,
					[]string{"anothercondition"},
					"",
				},
				{
					"document:foo#view@user:sarah",
					map[string]any{
						"anothercondition": "15",
					},
					v1.ResourceCheckResult_CAVEATED_MEMBER,
					[]string{"somecondition"},
					"",
				},
				{
					"document:foo#view@user:sarah",
					map[string]any{
						"somecondition":    "42",
						"anothercondition": "15",
						"somebool":         true,
					},
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view@user:john",
					map[string]any{
						"anothercondition": "14",
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view@user:john",
					map[string]any{
						"anothercondition": "15",
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
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view@user:noa",
					nil,
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
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
					map[string]types.VariableType{
						"somecondition": types.IntType,
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
					map[string]types.VariableType{
						"somecondition": types.IntType,
					},
				},
				"editcaveat": {
					"today == 'tuesday'",
					map[string]types.VariableType{
						"today": types.StringType,
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
						"somecondition": "42",
						"today":         "wednesday",
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view_and_edit@user:tom",
					map[string]any{
						"somecondition": "41",
						"today":         "tuesday",
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view_and_edit@user:tom",
					map[string]any{
						"somecondition": "42",
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
					map[string]types.VariableType{
						"somecondition": types.IntType,
					},
				},
				"bannedcaveat": {
					"region == 'bad'",
					map[string]types.VariableType{
						"region": types.StringType,
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
						"somecondition": "42",
						"region":        "bad",
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view_not_banned@user:tom",
					map[string]any{
						"somecondition": "41",
						"region":        "good",
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"document:foo#view_not_banned@user:tom",
					map[string]any{
						"somecondition": "42",
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
					map[string]types.VariableType{
						"user_ip": types.IPAddressType,
						"cidr":    types.StringType,
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
						"user_ip": types.MustParseIPAddress("192.168.0.1"),
					},
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
				{
					"repository:foobar#read@user:johndoe",
					map[string]any{
						"user_ip": types.MustParseIPAddress("9.2.3.1"),
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"repository:foobar#read@user:johndoe",
					map[string]any{
						"user_ip": "192.168.0.1",
					},
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
			},
		},
		{
			"App attributes example",
			`definition application {}
			definition group {
				relation member: application
				permission allowed = member
			}`,
			map[string]caveatDefinition{
				"attributes_match": {
					"expected.all(x, expected[x] == provided[x])",
					map[string]types.VariableType{
						"expected": types.MapType(types.AnyType),
						"provided": types.MapType(types.AnyType),
					},
				},
			},
			[]caveatedUpdate{
				{
					core.RelationTupleUpdate_CREATE,
					"group:ui_apps#member@application:frontend_app",
					"attributes_match",
					map[string]any{
						"expected": map[string]any{"type": "frontend", "region": "eu"},
					},
				},
				{
					core.RelationTupleUpdate_CREATE,
					"group:backend_apps#member@application:backend_app",
					"attributes_match",
					map[string]any{
						"expected": map[string]any{
							"type": "backend", "region": "us",
							"additional_attrs": map[string]any{
								"tag1": 100,
								"tag2": false,
							},
						},
					},
				},
			},
			[]check{
				{
					"group:ui_apps#allowed@application:frontend_app",
					map[string]any{
						"provided": map[string]any{"type": "frontend", "region": "eu", "team": "shop"},
					},
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
				{
					"group:ui_apps#allowed@application:frontend_app",
					map[string]any{
						"provided": map[string]any{"type": "frontend", "region": "us"},
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
				{
					"group:backend_apps#allowed@application:backend_app",
					map[string]any{
						"provided": map[string]any{
							"type": "backend", "region": "us", "team": "shop",
							"additional_attrs": map[string]any{
								"tag1": 100,
								"tag2": false,
							},
						},
					},
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
				{
					"group:backend_apps#allowed@application:backend_app",
					map[string]any{
						"provided": map[string]any{
							"type": "backend", "region": "us", "team": "shop",
							"additional_attrs": map[string]any{
								"tag1": 200.0,
								"tag2": false,
							},
						},
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
			},
		},
		{
			"authorize if resource was created before subject",
			`definition root {
				relation actors: actor
			}
			definition resource {
				relation creation_policy: root#actors
				permission tag = creation_policy
			}
			
			definition actor {}`,
			map[string]caveatDefinition{
				"created_before": {
					"timestamp(actor_created_at) > timestamp(created_at)",
					map[string]types.VariableType{
						"created_at":       types.StringType,
						"actor_created_at": types.StringType,
					},
				},
			},
			[]caveatedUpdate{
				{
					core.RelationTupleUpdate_CREATE,
					"resource:foo#creation_policy@root:root#actors",
					"created_before",
					map[string]any{
						"created_at": "2022-01-01T10:00:00.021Z",
					},
				},
				{
					core.RelationTupleUpdate_CREATE,
					"root:root#actors@actor:johndoe",
					"",
					nil,
				},
			},
			[]check{
				{
					"resource:foo#tag@actor:johndoe",
					map[string]any{
						"actor_created_at": "2022-01-01T11:00:00.021Z",
					},
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
				{
					"resource:foo#tag@actor:johndoe",
					map[string]any{
						"actor_created_at": "2022-01-01T09:00:00.021Z",
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
			},
		},
		{
			"time-bound permission",
			`definition resource {
				relation reader: user
				permission view = reader
			}
			
			definition user {}`,
			map[string]caveatDefinition{
				"expired": {
					"timestamp(now) < timestamp(expiration)",
					map[string]types.VariableType{
						"expiration": types.StringType,
						"now":        types.StringType,
					},
				},
			},
			[]caveatedUpdate{
				{
					core.RelationTupleUpdate_CREATE,
					"resource:foo#reader@user:sarah",
					"expired",
					map[string]any{
						"expiration": "2030-01-01T10:00:00.021Z",
						"now":        "2020-01-01T10:00:00.021Z",
					},
				},
				{
					core.RelationTupleUpdate_CREATE,
					"resource:foo#reader@user:john",
					"expired",
					map[string]any{
						"expiration": "2020-01-01T10:00:00.021Z",
						"now":        "2020-01-01T10:00:00.021Z",
					},
				},
			},
			[]check{
				{
					"resource:foo#view@user:sarah",
					nil,
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
				{
					"resource:foo#view@user:john",
					nil,
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
			},
		},
		{
			"legal-guardian example",
			`definition claim {
				relation claimer: user
				relation dependent_of: user#dependent_of
			  
				permission view = claimer + dependent_of
			}
			
			definition user {
				relation dependent_of: user
			}`,
			map[string]caveatDefinition{
				"legal_guardian": {
					`age < 12 || (class != "sensitive" && age > 12 && age < 18)`,
					map[string]types.VariableType{
						"age":   types.IntType,
						"class": types.StringType,
					},
				},
			},
			[]caveatedUpdate{
				{
					core.RelationTupleUpdate_CREATE,
					"user:son#dependent_of@user:father",
					"",
					nil,
				},
				{
					core.RelationTupleUpdate_CREATE,
					"claim:broken_leg#dependent_of@user:son#dependent_of",
					"legal_guardian",
					map[string]any{
						"age":   10,
						"class": "non-sensitive",
					},
				},
				{
					core.RelationTupleUpdate_CREATE,
					"user:daughter#dependent_of@user:father",
					"",
					nil,
				},
				{
					core.RelationTupleUpdate_CREATE,
					"claim:broken_arm#dependent_of@user:daughter#dependent_of",
					"legal_guardian",
					map[string]any{
						"age":   14,
						"class": "non-sensitive",
					},
				},
				{
					core.RelationTupleUpdate_CREATE,
					"claim:sensitive_matter#dependent_of@user:daughter#dependent_of",
					"legal_guardian",
					map[string]any{
						"age":   14,
						"class": "sensitive",
					},
				},
			},
			[]check{
				{
					"claim:broken_leg#view@user:father",
					nil,
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
				{
					"claim:broken_arm#view@user:father",
					nil,
					v1.ResourceCheckResult_MEMBER,
					nil,
					"",
				},
				{
					"claim:sensitive_matter#view@user:father",
					nil,
					v1.ResourceCheckResult_NOT_MEMBER,
					nil,
					"",
				},
			},
		},
		{
			"context type error test",
			`definition user {}

			definition document {
				relation viewer: user

				permission view = viewer
			}`,
			map[string]caveatDefinition{
				"testcaveat": {
					"somecondition == 42",
					map[string]types.VariableType{
						"somecondition": types.UIntType,
					},
				},
			},
			[]caveatedUpdate{
				{core.RelationTupleUpdate_CREATE, "document:foo#viewer@user:sarah", "testcaveat", nil},
			},
			[]check{
				{
					"document:foo#view@user:sarah",
					map[string]any{
						"somecondition": "43a",
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					[]string{},
					"type error for parameters for caveat `testcaveat`: could not convert context parameter `somecondition`: for uint: a uint64 value is required, but found invalid string value `43a`",
				},
				{
					"document:foo#view@user:sarah",
					map[string]any{
						"somecondition": "-43",
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					[]string{},
					"type error for parameters for caveat `testcaveat`: could not convert context parameter `somecondition`: for uint: a uint value is required, but found int64 value `-43`",
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

			revision, err := writeCaveatedTuples(ctx, t, ds, tt.schema, tt.caveats, tt.updates)
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
						require.NotNil(t, err, "missing required error: %s", r.error)
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

func writeCaveatedTuples(ctx context.Context, t *testing.T, ds datastore.Datastore, schema string, definedCaveats map[string]caveatDefinition, updates []caveatedUpdate) (datastore.Revision, error) {
	empty := ""
	defs, err := compiler.Compile([]compiler.InputSchema{
		{Source: "schema", SchemaString: schema},
	}, &empty)
	if err != nil {
		return datastore.NoRevision, err
	}

	compiledCaveats := make([]*core.CaveatDefinition, 0, len(definedCaveats))
	for name, c := range definedCaveats {
		e, err := caveats.EnvForVariables(c.env)
		if err != nil {
			return datastore.NoRevision, err
		}

		require.True(t, len(e.EncodedParametersTypes()) > 0)

		compiled, err := caveats.CompileCaveatWithName(e, c.expression, name)
		if err != nil {
			return datastore.NoRevision, err
		}
		serialized, err := compiled.Serialize()
		if err != nil {
			return datastore.NoRevision, err
		}
		compiledCaveats = append(compiledCaveats, &core.CaveatDefinition{
			Name:                 name,
			SerializedExpression: serialized,
			ParameterTypes:       types.EncodeParameterTypes(c.env),
		})
	}

	return ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		if err := rwt.WriteNamespaces(defs...); err != nil {
			return err
		}

		if err := rwt.WriteCaveats(compiledCaveats); err != nil {
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
	c := tuple.MustParse(relationTuple)
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
