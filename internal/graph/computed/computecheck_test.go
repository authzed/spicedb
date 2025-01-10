package computed_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/graph/computed"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/stretchr/testify/require"
)

type caveatedUpdate struct {
	Operation  tuple.UpdateOperation
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
		updates []caveatedUpdate
		checks  []check
	}{
		{
			"simple test",
			`definition user {}

			definition organization {
				relation admin: user | user with testcaveat
			}
					
			definition document {
				relation org: organization | organization with anothercaveat
				relation viewer: user | user with testcaveat
				relation editor: user | user with testcaveat

				permission edit = editor + org->admin
				permission view = viewer + edit
			}
			
			caveat testcaveat(somecondition int, somebool bool) {
				somecondition == 42 && somebool
			}

			caveat anothercaveat(anothercondition uint) {
				anothercondition == 15
			}
			`,
			[]caveatedUpdate{
				{tuple.UpdateOperationCreate, "organization:someorg#admin@user:sarah", "testcaveat", nil},
				{tuple.UpdateOperationCreate, "organization:someorg#admin@user:john", "testcaveat", map[string]any{"somecondition": "42", "somebool": true}},
				{tuple.UpdateOperationCreate, "organization:someorg#admin@user:jane", "", nil},
				{tuple.UpdateOperationCreate, "document:foo#org@organization:someorg", "anothercaveat", nil},
				{tuple.UpdateOperationCreate, "document:bar#org@organization:someorg", "", nil},
				{tuple.UpdateOperationCreate, "document:foo#editor@user:vic", "testcaveat", map[string]any{"somecondition": "42", "somebool": true}},
				{tuple.UpdateOperationCreate, "document:foo#viewer@user:vic", "testcaveat", map[string]any{"somecondition": "42", "somebool": true}},
				{tuple.UpdateOperationCreate, "document:foo#viewer@user:blippy", "testcaveat", map[string]any{"somecondition": "42", "somebool": false}},
				{tuple.UpdateOperationCreate, "document:foo#viewer@user:noa", "testcaveat", map[string]any{"somecondition": "42", "somebool": false}},
				{tuple.UpdateOperationCreate, "document:foo#editor@user:noa", "testcaveat", map[string]any{"somecondition": "42", "somebool": false}},
				{tuple.UpdateOperationCreate, "document:foo#editor@user:wayne", "invalid", nil},
			},

			[]check{
				{
					"document:foo#view@user:sarah",
					nil,
					v1.ResourceCheckResult_CAVEATED_MEMBER,
					[]string{"anothercondition", "somecondition", "somebool"},
					"",
				},
				{
					"document:foo#view@user:sarah",
					map[string]any{
						"somecondition": "42",
					},
					v1.ResourceCheckResult_CAVEATED_MEMBER,
					[]string{"anothercondition", "somebool"},
					"",
				},
				{
					"document:foo#view@user:sarah",
					map[string]any{
						"anothercondition": "15",
					},
					v1.ResourceCheckResult_CAVEATED_MEMBER,
					[]string{"somecondition", "somebool"},
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
				relation viewer: user | user with testcaveat
				permission view = viewer
			}
			
			caveat testcaveat(somecondition int) {
				somecondition == 42
			}
			`,
			[]caveatedUpdate{
				{tuple.UpdateOperationCreate, "document:foo#viewer@user:tom", "testcaveat", map[string]any{
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
				relation viewer: user | user with viewcaveat
				relation editor: user | user with editcaveat
				permission view_and_edit = viewer & editor
			}
			
			caveat viewcaveat(somecondition int) {
				somecondition == 42
			}

			caveat editcaveat(today string) {
				today == 'tuesday'
			}
			`,
			[]caveatedUpdate{
				{
					tuple.UpdateOperationCreate,
					"document:foo#viewer@user:tom",
					"viewcaveat",
					nil,
				},
				{
					tuple.UpdateOperationCreate,
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
				relation viewer: user | user with viewcaveat
				relation banned: user | user with bannedcaveat
				permission view_not_banned = viewer - banned
			}
			
			caveat viewcaveat(somecondition int) {
				somecondition == 42
			}
			
			caveat bannedcaveat(region string) {
				region == 'bad'
			}
			`,
			[]caveatedUpdate{
				{
					tuple.UpdateOperationCreate,
					"document:foo#viewer@user:tom",
					"viewcaveat",
					nil,
				},
				{
					tuple.UpdateOperationCreate,
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
				relation ip_allowlist_policy:  organization#members | organization#members with ip_allowlist
			
				permission policy = ip_allowlist_policy
			}
			
			definition repository {
				relation owner: organization
				relation reader: user
			
				permission read = reader & owner->policy
			}
			
			caveat ip_allowlist(user_ip ipaddress, cidr string) {
				user_ip.in_cidr(cidr)
			}
			`,
			[]caveatedUpdate{
				{
					tuple.UpdateOperationCreate,
					"repository:foobar#owner@organization:myorg",
					"",
					nil,
				},
				{
					tuple.UpdateOperationCreate,
					"organization:myorg#members@user:johndoe",
					"",
					nil,
				},
				{
					tuple.UpdateOperationCreate,
					"repository:foobar#reader@user:johndoe",
					"",
					nil,
				},
				{
					tuple.UpdateOperationCreate,
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
				relation member: application | application with attributes_match
				permission allowed = member
			}
			
			caveat attributes_match(expected map<any>, provided map<any>) {
				expected.isSubtreeOf(provided)
			}
			`,
			[]caveatedUpdate{
				{
					tuple.UpdateOperationCreate,
					"group:ui_apps#member@application:frontend_app",
					"attributes_match",
					map[string]any{
						"expected": map[string]any{"type": "frontend", "region": "eu"},
					},
				},
				{
					tuple.UpdateOperationCreate,
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
								"tag1": 100.0,
								"tag2": false,
								"tag3": "hi",
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
				relation creation_policy: root#actors | root#actors with created_before
				permission tag = creation_policy
			}
			
			definition actor {}
			
			caveat created_before(actor_created_at string, created_at string) {
				timestamp(actor_created_at) > timestamp(created_at)
			}
			`,
			[]caveatedUpdate{
				{
					tuple.UpdateOperationCreate,
					"resource:foo#creation_policy@root:root#actors",
					"created_before",
					map[string]any{
						"created_at": "2022-01-01T10:00:00.021Z",
					},
				},
				{
					tuple.UpdateOperationCreate,
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
				relation reader: user | user with not_expired
				permission view = reader
			}
			
			caveat not_expired(expiration string, now string) {
				timestamp(now) < timestamp(expiration)
			}

			definition user {}`,
			[]caveatedUpdate{
				{
					tuple.UpdateOperationCreate,
					"resource:foo#reader@user:sarah",
					"not_expired",
					map[string]any{
						"expiration": "2030-01-01T10:00:00.021Z",
						"now":        "2020-01-01T10:00:00.021Z",
					},
				},
				{
					tuple.UpdateOperationCreate,
					"resource:foo#reader@user:john",
					"not_expired",
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
				relation dependent_of: user#dependent_of | user#dependent_of with legal_guardian
			  
				permission view = claimer + dependent_of
			}

			caveat legal_guardian(age int, class string) {
				age < 12 || (class != "sensitive" && age > 12 && age < 18)
			}
			
			definition user {
				relation dependent_of: user
			}`,
			[]caveatedUpdate{
				{
					tuple.UpdateOperationCreate,
					"user:son#dependent_of@user:father",
					"",
					nil,
				},
				{
					tuple.UpdateOperationCreate,
					"claim:broken_leg#dependent_of@user:son#dependent_of",
					"legal_guardian",
					map[string]any{
						"age":   10,
						"class": "non-sensitive",
					},
				},
				{
					tuple.UpdateOperationCreate,
					"user:daughter#dependent_of@user:father",
					"",
					nil,
				},
				{
					tuple.UpdateOperationCreate,
					"claim:broken_arm#dependent_of@user:daughter#dependent_of",
					"legal_guardian",
					map[string]any{
						"age":   14,
						"class": "non-sensitive",
					},
				},
				{
					tuple.UpdateOperationCreate,
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
				relation viewer: user | user with testcaveat

				permission view = viewer
			}
			
			caveat testcaveat(somecondition uint) {
				somecondition == 42
			}
			`,
			[]caveatedUpdate{
				{tuple.UpdateOperationCreate, "document:foo#viewer@user:sarah", "testcaveat", nil},
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
		{
			"schema caveat test",
			`
			caveat testcaveat(somecondition uint) {
				somecondition == 42
			}

			definition user {}

			definition document {
				relation viewer: user with testcaveat

				permission view = viewer
			}`,
			[]caveatedUpdate{
				{tuple.UpdateOperationCreate, "document:foo#viewer@user:sarah", "testcaveat", nil},
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
				{
					"document:foo#view@user:sarah",
					map[string]any{
						"somecondition": "41",
					},
					v1.ResourceCheckResult_NOT_MEMBER,
					[]string{},
					"",
				},
				{
					"document:foo#view@user:sarah",
					map[string]any{
						"somecondition": "42",
					},
					v1.ResourceCheckResult_MEMBER,
					[]string{},
					"",
				},
			},
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			require.NoError(t, err)

			dispatch := graph.NewLocalOnlyDispatcher(10, 100)
			ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
			require.NoError(t, datastoremw.SetInContext(ctx, ds))

			revision, err := writeCaveatedTuples(ctx, t, ds, tt.schema, tt.updates)
			require.NoError(t, err)

			for _, r := range tt.checks {
				r := r
				t.Run(fmt.Sprintf("%s::%v", r.check, r.context), func(t *testing.T) {
					rel := tuple.MustParse(r.check)

					result, _, err := computed.ComputeCheck(ctx, dispatch,
						computed.CheckParameters{
							ResourceType:  rel.Resource.RelationReference(),
							Subject:       rel.Subject,
							CaveatContext: r.context,
							AtRevision:    revision,
							MaximumDepth:  50,
							DebugOption:   computed.BasicDebuggingEnabled,
						},
						rel.Resource.ObjectID,
						100,
					)

					if r.error != "" {
						require.NotNil(t, err, "missing required error: %s", r.error)
						require.Equal(t, err.Error(), r.error)
					} else {
						require.NoError(t, err)
						require.Equal(t, v1.ResourceCheckResult_Membership_name[int32(r.member)], v1.ResourceCheckResult_Membership_name[int32(result.Membership)], "mismatch for %s with context %v", r.check, r.context)

						if result.Membership == v1.ResourceCheckResult_CAVEATED_MEMBER {
							foundFields := result.MissingExprFields
							sort.Strings(foundFields)
							sort.Strings(r.expectedMissingFields)

							require.Equal(t, r.expectedMissingFields, foundFields)
						}
					}
				})
			}
		})
	}
}

func TestComputeCheckError(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	dispatch := graph.NewLocalOnlyDispatcher(10, 100)
	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
	require.NoError(t, datastoremw.SetInContext(ctx, ds))

	_, _, err = computed.ComputeCheck(ctx, dispatch,
		computed.CheckParameters{
			ResourceType:  tuple.RR("a", "b"),
			Subject:       tuple.ONR("c", "d", "..."),
			CaveatContext: nil,
			AtRevision:    datastore.NoRevision,
			MaximumDepth:  50,
			DebugOption:   computed.BasicDebuggingEnabled,
		},
		"id",
		100,
	)
	require.Error(t, err)
}

func TestComputeBulkCheck(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	dispatch := graph.NewLocalOnlyDispatcher(10, 100)
	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
	require.NoError(t, datastoremw.SetInContext(ctx, ds))

	revision, err := writeCaveatedTuples(ctx, t, ds, `
	definition user {}

	caveat somecaveat(somecondition int) {
		somecondition == 42
	}

	definition document {
		relation viewer: user | user with somecaveat
		permission view = viewer
	}
	`, []caveatedUpdate{
		{tuple.UpdateOperationCreate, "document:direct#viewer@user:tom", "", nil},
		{tuple.UpdateOperationCreate, "document:first#viewer@user:tom", "somecaveat", map[string]any{
			"somecondition": 42,
		}},
		{tuple.UpdateOperationCreate, "document:second#viewer@user:tom", "somecaveat", map[string]any{}},
		{tuple.UpdateOperationCreate, "document:third#viewer@user:tom", "somecaveat", map[string]any{
			"somecondition": 32,
		}},
	})
	require.NoError(t, err)

	resp, _, _, err := computed.ComputeBulkCheck(ctx, dispatch,
		computed.CheckParameters{
			ResourceType:  tuple.RR("document", "view"),
			Subject:       tuple.ONR("user", "tom", "..."),
			CaveatContext: nil,
			AtRevision:    revision,
			MaximumDepth:  50,
			DebugOption:   computed.NoDebugging,
		},
		[]string{"direct", "first", "second", "third"},
		100,
	)
	require.NoError(t, err)

	require.Equal(t, resp["direct"].Membership, v1.ResourceCheckResult_MEMBER)
	require.Equal(t, resp["first"].Membership, v1.ResourceCheckResult_MEMBER)
	require.Equal(t, resp["second"].Membership, v1.ResourceCheckResult_CAVEATED_MEMBER)
	require.Equal(t, resp["third"].Membership, v1.ResourceCheckResult_NOT_MEMBER)
}

func writeCaveatedTuples(ctx context.Context, _ *testing.T, ds datastore.Datastore, schema string, updates []caveatedUpdate) (datastore.Revision, error) {
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       "schema",
		SchemaString: schema,
	}, compiler.AllowUnprefixedObjectType())
	if err != nil {
		return datastore.NoRevision, err
	}

	return ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		if err := rwt.WriteNamespaces(ctx, compiled.ObjectDefinitions...); err != nil {
			return err
		}

		if err := rwt.WriteCaveats(ctx, compiled.CaveatDefinitions); err != nil {
			return err
		}

		var rtu []tuple.RelationshipUpdate
		for _, updt := range updates {
			rtu = append(rtu, tuple.RelationshipUpdate{
				Operation:    updt.Operation,
				Relationship: caveatedRelationTuple(updt.tuple, updt.caveatName, updt.context),
			})
		}

		return rwt.WriteRelationships(ctx, rtu)
	})
}

func caveatedRelationTuple(relationTuple string, caveatName string, context map[string]any) tuple.Relationship {
	c := tuple.MustParse(relationTuple)
	strct, err := structpb.NewStruct(context)
	if err != nil {
		panic(err)
	}
	if caveatName != "" {
		c.OptionalCaveat = &core.ContextualizedCaveat{
			CaveatName: caveatName,
			Context:    strct,
		}
	}
	return c
}
