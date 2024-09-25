//go:build !skipintegrationtests
// +build !skipintegrationtests

package integrationtesting_test

import (
	"context"
	"fmt"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

type stcOp interface {
	Execute(tester opsTester) error
}

type stcStep struct {
	op            stcOp
	expectedError string
}

type schemaTestCase struct {
	name  string
	steps []stcStep
}

type writeSchema struct{ schemaText string }

func (ws writeSchema) Execute(tester opsTester) error {
	return tester.WriteSchema(context.Background(), ws.schemaText)
}

type readSchema struct{ expectedSchemaText string }

func (rs readSchema) Execute(tester opsTester) error {
	schemaText, err := tester.ReadSchema(context.Background())
	if err != nil {
		return err
	}
	if schemaText != rs.expectedSchemaText {
		return fmt.Errorf("unexpected schema: %#v", schemaText)
	}
	return nil
}

type createCaveatedRelationship struct {
	relString  string
	caveatName string
	context    map[string]any
}

func (wr createCaveatedRelationship) Execute(tester opsTester) error {
	ctx, err := structpb.NewStruct(wr.context)
	if err != nil {
		return err
	}

	rel := tuple.MustParse(wr.relString)
	rel.OptionalCaveat = &core.ContextualizedCaveat{
		CaveatName: wr.caveatName,
		Context:    ctx,
	}
	return tester.CreateRelationship(context.Background(), rel)
}

type touchCaveatedRelationship struct {
	relString  string
	caveatName string
	context    map[string]any
}

func (wr touchCaveatedRelationship) Execute(tester opsTester) error {
	ctx, err := structpb.NewStruct(wr.context)
	if err != nil {
		return err
	}

	rel := tuple.MustParse(wr.relString)
	rel.OptionalCaveat = &core.ContextualizedCaveat{
		CaveatName: wr.caveatName,
		Context:    ctx,
	}
	return tester.TouchRelationship(context.Background(), rel)
}

type createRelationship struct{ relString string }

func (wr createRelationship) Execute(tester opsTester) error {
	return tester.CreateRelationship(context.Background(), tuple.MustParse(wr.relString))
}

type deleteRelationship struct{ relString string }

func (dr deleteRelationship) Execute(tester opsTester) error {
	return tester.DeleteRelationship(context.Background(), tuple.MustParse(dr.relString))
}

type deleteCaveatedRelationship struct {
	relString  string
	caveatName string
}

func (dr deleteCaveatedRelationship) Execute(tester opsTester) error {
	rel := tuple.MustParse(dr.relString)
	rel.OptionalCaveat = &core.ContextualizedCaveat{
		CaveatName: dr.caveatName,
	}

	return tester.DeleteRelationship(context.Background(), rel)
}

func TestSchemaAndRelationshipsOperations(t *testing.T) {
	t.Parallel()

	tcs := []schemaTestCase{
		// Test: write a basic, valid schema.
		{
			"basic valid schema write",
			[]stcStep{
				{writeSchema{`definition user {}`}, ""},
			},
		},

		// Test: write a basic, invalid schema.
		{
			"basic invalid schema write",
			[]stcStep{
				{writeSchema{`definitin user {}`}, "Unexpected token at root level"},
			},
		},

		// Test: try to remove a relation that has at least one relationship.
		{
			"try to remove relation with data",
			[]stcStep{
				// Write the schema.
				{
					writeSchema{`
						definition user {}
					
						definition document {
							relation viewer: user
						}
					`}, "",
				},
				// Write a relationship using the relation.
				{
					createRelationship{"document:foo#viewer@user:tom"}, "",
				},
				// Remove the relation, which should fail.
				{
					writeSchema{`
						definition user {}
					
						definition document {
						}
					`}, "cannot delete relation",
				},
				// Delete the relationship.
				{
					deleteRelationship{"document:foo#viewer@user:tom"}, "",
				},
				// Remove the relation, which should now succeed.
				{
					writeSchema{`
							definition user {}
						
							definition document {
							}
					`}, "",
				},
			},
		},

		// Test: try to change the types allowed on a relation when existing relationships
		// use the former type.
		{
			"try to modify relation type with data",
			[]stcStep{
				// Write the schema.
				{
					writeSchema{`
						definition user {}
						definition anotheruser {}
					
						definition document {
							relation viewer: user | anotheruser
						}
					`}, "",
				},
				// Write a relationship using the relation.
				{
					createRelationship{"document:foo#viewer@user:tom"}, "",
				},
				// Remove the relation's type, which should fail.
				{
					writeSchema{`
						definition user {}
						definition anotheruser {}
					
						definition document {
							relation viewer: anotheruser
						}
					`}, "cannot remove allowed type `user`",
				},
				// Delete the relationship.
				{
					deleteRelationship{"document:foo#viewer@user:tom"}, "",
				},
				// Remove the relation, which should now succeed.
				{
					writeSchema{`
						definition user {}
						definition anotheruser {}

						definition document {
							relation viewer: anotheruser
						}
					`}, "",
				},
			},
		},

		// Test: try to modify the allowed caveat type when it is being used by relationships.
		{
			"try to modify relation caveat with data",
			[]stcStep{
				// Write the schema.
				{
					writeSchema{`
						caveat somecaveat(someParam int) {
							someParam == 42
						}

						definition user {}
					
						definition document {
							relation viewer: user with somecaveat | user
						}
					`}, "",
				},
				// Write some relationships using the relation.
				{
					createRelationship{"document:foo#viewer@user:tom"}, "",
				},
				{
					createCaveatedRelationship{"document:foo#viewer@user:sarah", "somecaveat", nil}, "",
				},
				// Remove the caveat from the relation, which should fail.
				{
					writeSchema{`
						caveat somecaveat(someParam int) {
							someParam == 42
						}

						definition user {}
					
						definition document {
							relation viewer: user
						}
					`}, "cannot remove allowed type `user with somecaveat`",
				},
				// Delete the relationship.
				{
					deleteCaveatedRelationship{"document:foo#viewer@user:sarah", "somecaveat"}, "",
				},
				// Remove the caveat from the relation, which should now succeed.
				{
					writeSchema{`
						caveat somecaveat(someParam int) {
							someParam == 42
						}

						definition user {}
						definition anotheruser {}

						definition document {
							relation viewer: user
						}
					`}, "",
				},
			},
		},

		// Test: try to rename a relation that has relationships.
		{
			"try to rename relation with data",
			[]stcStep{
				// Write the schema.
				{
					writeSchema{`
						definition user {}			

						definition document {
							relation viewer: user
						}
					`}, "",
				},
				// Write a relationship using the relation.
				{
					createRelationship{"document:foo#viewer@user:tom"}, "",
				},
				// Rename the relation, which should fail.
				{
					writeSchema{`
						definition user {}
					
						definition document {
							relation viewuser: user
						}
					`}, "cannot delete relation `viewer`",
				},
				// Delete the relationship.
				{
					deleteRelationship{"document:foo#viewer@user:tom"}, "",
				},
				// Rename the relation, which should now succeed.
				{
					writeSchema{`
						definition user {}

						definition document {
							relation viewuser: user
						}
					`}, "",
				},
			},
		},

		// Test: attempt to write a relationship with an unknown resource type.
		{
			"write relationship with unknown resource type",
			[]stcStep{
				// Write the schema.
				{
					writeSchema{`
						definition user {}

						definition document {
							relation viewer: user
						}
					`}, "",
				},
				// Write a relationship using the relation, but with an undefined type, which should fail.
				{
					createRelationship{"doc:foo#viewer@user:tom"},
					"object definition `doc` not found",
				},
			},
		},

		// Test: attempt to write a relationship with an unknown relation.
		{
			"write relationship with unknown relation",
			[]stcStep{
				// Write the schema.
				{
					writeSchema{`
						definition user {}

						definition document {
							relation viewer: user
						}
					`}, "",
				},
				// Write a relationship using an unknown relation, which should fail.
				{
					createRelationship{"document:foo#viewguy@user:tom"},
					"relation/permission `viewguy` not found",
				},
			},
		},

		// Test: attempt to write a relationship with an unknown subject type.
		{
			"write relationship with unknown subject type",
			[]stcStep{
				// Write the schema.
				{
					writeSchema{`
						definition user {}

						definition document {
							relation viewer: user
						}
					`}, "",
				},
				// Write a relationship using the relation, but with an undefined type, which should fail.
				{
					createRelationship{"document:foo#viewer@anothersubject:tom"},
					"object definition `anothersubject` not found",
				},
			},
		},

		// Test: attempt to write a relationship with a disallowed subject type.
		{
			"try to write relationship subject type that is not allowed",
			[]stcStep{
				// Write the schema.
				{
					writeSchema{`
						definition user {}
						definition anothersubject {}

						definition document {
							relation viewer: user
						}
					`}, "",
				},
				// Write a relationship using the relation, but with the wrong type, which should fail.
				{
					createRelationship{"document:foo#viewer@anothersubject:tom"},
					"subjects of type `anothersubject` are not allowed",
				},
				// Update the schema to add.
				{
					writeSchema{`
						definition user {}
						definition anothersubject {}

						definition document {
							relation viewer: user | anothersubject
						}
					`}, "",
				},
				// Write a relationship, which should succeed now.
				{
					createRelationship{"document:foo#viewer@anothersubject:tom"}, "",
				},
			},
		},

		// Test: attempt to write a relationship with a disallowed caveat type.
		{
			"try to write relationship subject caveat type that is not allowed",
			[]stcStep{
				// Write the schema.
				{
					writeSchema{`
						definition user {}

						definition document {
							relation viewer: user
						}
					`}, "",
				},
				// Write a relationship using the relation, but with the wrong caveat, which should fail.
				{
					createCaveatedRelationship{"document:foo#viewer@user:tom", "somecaveat", nil},
					"subjects of type `user with somecaveat` are not allowed",
				},
			},
		},

		// Test: attempt to write a relationship with the wrong context parameter type.
		{
			"try to write relationship subject caveat with wrong context parameter type",
			[]stcStep{
				// Write the schema.
				{
					writeSchema{`
						caveat somecaveat(someParam int) {
							someParam == 42
						}

						definition user {}

						definition document {
							relation viewer: user with somecaveat
						}
					`}, "",
				},
				// Write a relationship using the caveat, but with the wrong context value.
				{
					createCaveatedRelationship{
						"document:foo#viewer@user:tom",
						"somecaveat",
						map[string]any{
							"someParam": "42e",
						},
					},
					"a int64 value is required, but found invalid string value `42e`",
				},
				// Write a relationship using the caveat, but with the correct context value.
				{
					createCaveatedRelationship{
						"document:foo#viewer@user:tom",
						"somecaveat",
						map[string]any{
							"someParam": "42",
						},
					},
					"",
				},
			},
		},

		// Test: attempt to write a relationship with an unknown context parameter type.
		{
			"try to write relationship subject caveat with an unknown context parameter type",
			[]stcStep{
				// Write the schema.
				{
					writeSchema{`
						caveat somecaveat(someParam int) {
							someParam == 42
						}

						definition user {}

						definition document {
							relation viewer: user with somecaveat
						}
					`}, "",
				},
				// Write a relationship using the caveat, but with an unknown context value.
				{
					createCaveatedRelationship{
						"document:foo#viewer@user:tom",
						"somecaveat",
						map[string]any{
							"someUnknownParam": "",
						},
					},
					"unknown parameter `someUnknownParam`",
				},
			},
		},

		// Test: attempt to change the parameters on a caveat.
		{
			"caveat parameter changes",
			[]stcStep{
				// Write the initial schema.
				{
					writeSchema{`
						caveat somecaveat(someParam int, anotherParam int) {
							someParam == 42 && anotherParam == 43
						}

						definition user {}

						definition document {
							relation viewer: user with somecaveat
						}
					`}, "",
				},
				// Try to add a parameter to the caveat, which should succeed.
				{
					writeSchema{`
							caveat somecaveat(someParam int, anotherParam int, newParam int) {
								someParam == 42 && anotherParam == 43 && newParam == 44
							}
	
							definition user {}
	
							definition document {
								relation viewer: user with somecaveat
							}
						`}, "",
				},
				// Try to remove a parameter from the caveat, which should fail.
				{
					writeSchema{`
							caveat somecaveat(someParam int, anotherParam int) {
								someParam == 42 && anotherParam == 43
							}
	
							definition user {}
	
							definition document {
								relation viewer: user with somecaveat
							}
						`}, "cannot remove parameter `newParam` on caveat `somecaveat`",
				},
				// Try to change the type of a parameter on the caveat, which should fail.
				{
					writeSchema{`
							caveat somecaveat(someParam int, anotherParam int, newParam bool) {
								someParam == 42 && anotherParam == 43 && newParam
							}
	
							definition user {}
	
							definition document {
								relation viewer: user with somecaveat
							}
						`}, "cannot change the type of parameter `newParam` on caveat `somecaveat`",
				},
			},
		},

		// Test: write relationships differing only by caveat
		{
			"attempt to write two relationships on the same relation, one without caveat and one with",
			[]stcStep{
				// Write the schema.
				{
					writeSchema{`
							caveat somecaveat(someParam int) {
								someParam == 42
							}

							definition user {}

							definition document {
								relation viewer: user with somecaveat | user
							}
						`}, "",
				},
				// Write a relationship without the caveat.
				{
					createRelationship{
						"document:foo#viewer@user:tom",
					}, "",
				},
				// Write a relationship using the caveat, which should fail since the relationship already exists.
				{
					createCaveatedRelationship{
						"document:foo#viewer@user:tom",
						"somecaveat",
						nil,
					},
					"as it already existed",
				},
			},
		},

		// Test: touch relationships differing only by caveat
		{
			"touch a relationship, changing its caveat",
			[]stcStep{
				// Write the schema.
				{
					writeSchema{`
							caveat somecaveat(someParam int) {
								someParam == 42
							}

							definition user {}

							definition document {
								relation viewer: user with somecaveat | user
							}
						`}, "",
				},
				// Create a relationship without the caveat.
				{
					createRelationship{
						"document:foo#viewer@user:tom",
					}, "",
				},
				// Touch a relationship using the caveat, which should update the caveat reference.
				{
					touchCaveatedRelationship{
						"document:foo#viewer@user:tom",
						"somecaveat",
						nil,
					},
					"",
				},
				// Attempt to remove the caveat, which should fail since there is a relationship using it.
				{
					writeSchema{`
							caveat somecaveat(someParam int) {
								someParam == 42
							}

							definition user {}

							definition document {
								relation viewer: user
							}
						`}, "cannot remove allowed type `user with somecaveat`",
				},
			},
		},

		// Test: write a schema, add a caveat definition, read back, then remove, and continue.
		{
			"add and remove caveat in schema",
			[]stcStep{
				// Write the initial schema.
				{
					writeSchema{`
							definition user {}

							definition document {
								relation viewer: user
							}
						`}, "",
				},
				// Read back.
				{
					readSchema{"definition document {\n\trelation viewer: user\n}\n\ndefinition user {}"}, "",
				},
				// Add a caveat definition.
				{
					writeSchema{`
							definition user {}

							caveat someCaveat(somecondition int) {
								somecondition == 42
							}

							definition document {
								relation viewer: user
							}
						`}, "",
				},
				// Read back.
				{
					readSchema{"caveat someCaveat(somecondition int) {\n\tsomecondition == 42\n}\n\ndefinition document {\n\trelation viewer: user\n}\n\ndefinition user {}"}, "",
				},
				// Remove the caveat definition.
				{
					writeSchema{`
							definition user {}

							definition document {
								relation viewer: user
							}
						`}, "",
				},
				// Read back.
				{
					readSchema{"definition document {\n\trelation viewer: user\n}\n\ndefinition user {}"}, "",
				},
			},
		},
	}

	testers := map[string]func(conn grpc.ClientConnInterface) opsTester{
		"v1": func(conn grpc.ClientConnInterface) opsTester {
			return v1OpsTester{
				schemaClient:  v1.NewSchemaServiceClient(conn),
				baseOpsTester: baseOpsTester{permClient: v1.NewPermissionsServiceClient(conn)},
			}
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, testerName := range []string{"v1"} {
				testerName := testerName
				t.Run(testerName, func(t *testing.T) {
					conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, false, tf.EmptyDatastore)
					t.Cleanup(cleanup)

					tester := testers[testerName](conn)

					for _, step := range tc.steps {
						err := step.op.Execute(tester)
						if step.expectedError != "" {
							require.NotNil(t, err)
							require.Contains(t, err.Error(), step.expectedError)
						} else {
							require.NoError(t, err)
						}
					}
				})
			}
		})
	}
}

type opsTester interface {
	Name() string
	ReadSchema(ctx context.Context) (string, error)
	WriteSchema(ctx context.Context, schemaString string) error
	CreateRelationship(ctx context.Context, relationship tuple.Relationship) error
	TouchRelationship(ctx context.Context, relationship tuple.Relationship) error
	DeleteRelationship(ctx context.Context, relationship tuple.Relationship) error
}

type baseOpsTester struct {
	permClient v1.PermissionsServiceClient
}

func (st baseOpsTester) CreateRelationship(ctx context.Context, relationship tuple.Relationship) error {
	_, err := st.permClient.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Create(relationship))},
	})
	return err
}

func (st baseOpsTester) TouchRelationship(ctx context.Context, relationship tuple.Relationship) error {
	_, err := st.permClient.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Touch(relationship))},
	})
	return err
}

func (st baseOpsTester) DeleteRelationship(ctx context.Context, relationship tuple.Relationship) error {
	_, err := st.permClient.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Delete(relationship))},
	})
	return err
}

type v1OpsTester struct {
	baseOpsTester
	schemaClient v1.SchemaServiceClient
}

func (st v1OpsTester) Name() string {
	return "v1"
}

func (st v1OpsTester) WriteSchema(ctx context.Context, schemaString string) error {
	_, err := st.schemaClient.WriteSchema(ctx, &v1.WriteSchemaRequest{
		Schema: schemaString,
	})
	return err
}

func (st v1OpsTester) ReadSchema(ctx context.Context) (string, error) {
	resp, err := st.schemaClient.ReadSchema(ctx, &v1.ReadSchemaRequest{})
	if err != nil {
		return "", err
	}
	return resp.SchemaText, nil
}
