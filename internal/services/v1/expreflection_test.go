package v1

import (
	"reflect"
	"strings"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/ettle/strcase"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore/revisionparsing"
	"github.com/authzed/spicedb/pkg/diff"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/testutil"
)

func TestConvertDiff(t *testing.T) {
	tcs := []struct {
		name             string
		existingSchema   string
		comparisonSchema string
		expectedResponse *v1.ExperimentalSchemaDiffResponse
	}{
		{
			"no diff",
			`definition user {}`,
			`definition user {}`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{},
			},
		},
		{
			"add namespace",
			``,
			`definition user {}`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_DefinitionAdded{
							DefinitionAdded: &v1.ExpDefinition{
								Name:    "user",
								Comment: "",
							},
						},
					},
				},
			},
		},
		{
			"remove namespace",
			`definition user {}`,
			``,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_DefinitionRemoved{
							DefinitionRemoved: &v1.ExpDefinition{
								Name:    "user",
								Comment: "",
							},
						},
					},
				},
			},
		},
		{
			"change namespace comment",
			`definition user {}`,
			`// user has a comment
			definition user {}`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_DefinitionDocCommentChanged{
							DefinitionDocCommentChanged: &v1.ExpDefinition{
								Name:    "user",
								Comment: "// user has a comment",
							},
						},
					},
				},
			},
		},
		{
			"add caveat",
			``,
			`caveat someCaveat(someparam int) { someparam < 42 }`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_CaveatAdded{
							CaveatAdded: &v1.ExpCaveat{
								Name:       "someCaveat",
								Comment:    "",
								Expression: "someparam < 42",
								Parameters: []*v1.ExpCaveatParameter{
									{
										Name:             "someparam",
										Type:             "int",
										ParentCaveatName: "someCaveat",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			"remove caveat",
			`caveat someCaveat(someparam int) { someparam < 42 }`,
			``,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_CaveatRemoved{
							CaveatRemoved: &v1.ExpCaveat{
								Name:       "someCaveat",
								Comment:    "",
								Expression: "someparam < 42",
								Parameters: []*v1.ExpCaveatParameter{
									{
										Name:             "someparam",
										Type:             "int",
										ParentCaveatName: "someCaveat",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			"change caveat comment",
			`// someCaveat has a comment
			caveat someCaveat(someparam int) { someparam < 42 }`,
			`// someCaveat has b comment
			caveat someCaveat(someparam int) { someparam < 42 }`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_CaveatDocCommentChanged{
							CaveatDocCommentChanged: &v1.ExpCaveat{
								Name:       "someCaveat",
								Comment:    "// someCaveat has b comment",
								Expression: "someparam < 42",
								Parameters: []*v1.ExpCaveatParameter{
									{
										Name:             "someparam",
										Type:             "int",
										ParentCaveatName: "someCaveat",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			"added relation",
			`definition user {}`,
			`definition user { relation somerel: user; }`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_RelationAdded{
							RelationAdded: &v1.ExpRelation{
								Name:                 "somerel",
								Comment:              "",
								ParentDefinitionName: "user",
								SubjectTypes: []*v1.ExpTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			"removed relation",
			`definition user { relation somerel: user; }`,
			`definition user {}`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_RelationRemoved{
							RelationRemoved: &v1.ExpRelation{
								Name:                 "somerel",
								Comment:              "",
								ParentDefinitionName: "user",
								SubjectTypes: []*v1.ExpTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			"relation type added",
			`definition user {}
			
			 definition anon {}

			 definition resource {
				relation viewer: anon
			 }
			`,
			`definition user {}
			
			definition anon {}

			definition resource {
			   relation viewer: user | anon
			}
		   `,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_RelationSubjectTypeAdded{
							RelationSubjectTypeAdded: &v1.ExpRelationSubjectTypeChange{
								Relation: &v1.ExpRelation{
									Name:                 "viewer",
									Comment:              "",
									ParentDefinitionName: "resource",
									SubjectTypes: []*v1.ExpTypeReference{
										{
											SubjectDefinitionName: "user",
											Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
										},
										{
											SubjectDefinitionName: "anon",
											Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
										},
									},
								},
								ChangedSubjectType: &v1.ExpTypeReference{
									SubjectDefinitionName: "user",
									Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
								},
							},
						},
					},
				},
			},
		},
		{
			"relation type removed",
			`definition user {}
			
			 definition anon {}

			 definition resource {
				relation viewer: anon | user
			 }
			`,
			`definition user {}
			
			definition anon {}

			definition resource {
			   relation viewer: user
			}
		   `,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_RelationSubjectTypeRemoved{
							RelationSubjectTypeRemoved: &v1.ExpRelationSubjectTypeChange{
								Relation: &v1.ExpRelation{
									Name:                 "viewer",
									Comment:              "",
									ParentDefinitionName: "resource",
									SubjectTypes: []*v1.ExpTypeReference{
										{
											SubjectDefinitionName: "user",
											Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
										},
									},
								},
								ChangedSubjectType: &v1.ExpTypeReference{
									SubjectDefinitionName: "anon",
									Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
								},
							},
						},
					},
				},
			},
		},
		{
			"relation comment changed",
			`definition user {}

			 definition resource {
				relation viewer: user
			 }`,
			`definition user {}

			definition resource {
				// viewer has a comment
				relation viewer: user
			}`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_RelationDocCommentChanged{
							RelationDocCommentChanged: &v1.ExpRelation{
								Name:                 "viewer",
								Comment:              "// viewer has a comment",
								ParentDefinitionName: "resource",
								SubjectTypes: []*v1.ExpTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			"added permission",
			`definition user {}
			
			 definition resource {
			 }
			`,
			`definition user {}
			
			definition resource {
				permission foo = nil
			}`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_PermissionAdded{
							PermissionAdded: &v1.ExpPermission{
								Name:                 "foo",
								Comment:              "",
								ParentDefinitionName: "resource",
							},
						},
					},
				},
			},
		},
		{
			"removed permission",
			`definition user {}

			 definition resource {
				permission foo = nil
			 }`,
			`definition user {}

			definition resource {
			}`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_PermissionRemoved{
							PermissionRemoved: &v1.ExpPermission{
								Name:                 "foo",
								Comment:              "",
								ParentDefinitionName: "resource",
							},
						},
					},
				},
			},
		},
		{
			"permission comment changed",
			`definition user {}

			 definition resource {
				// foo has a comment
				permission foo = nil
			 }`,
			`definition user {}
			
			definition resource {
				// foo has a new comment
				permission foo = nil
			}`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_PermissionDocCommentChanged{
							PermissionDocCommentChanged: &v1.ExpPermission{
								Name:                 "foo",
								Comment:              "// foo has a new comment",
								ParentDefinitionName: "resource",
							},
						},
					},
				},
			},
		},
		{
			"permission expression changed",
			`definition resource {
				permission foo = nil
			}`,
			`definition resource {
				permission foo = foo
			}`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_PermissionExprChanged{
							PermissionExprChanged: &v1.ExpPermission{
								Name:                 "foo",
								Comment:              "",
								ParentDefinitionName: "resource",
							},
						},
					},
				},
			},
		},
		{
			"caveat parameter added",
			`caveat someCaveat(someparam int) { someparam < 42 }`,
			`caveat someCaveat(someparam int, someparam2 string) { someparam < 42 }`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_CaveatParameterAdded{
							CaveatParameterAdded: &v1.ExpCaveatParameter{
								Name:             "someparam2",
								Type:             "string",
								ParentCaveatName: "someCaveat",
							},
						},
					},
				},
			},
		},
		{
			"caveat parameter removed",
			`caveat someCaveat(someparam int, someparam2 string) { someparam < 42 }`,
			`caveat someCaveat(someparam int) { someparam < 42 }`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_CaveatParameterRemoved{
							CaveatParameterRemoved: &v1.ExpCaveatParameter{
								Name:             "someparam2",
								Type:             "string",
								ParentCaveatName: "someCaveat",
							},
						},
					},
				},
			},
		},
		{
			"caveat parameter type changed",
			`caveat someCaveat(someparam int) { someparam < 42 }`,
			`caveat someCaveat(someparam uint) { someparam < 42 }`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_CaveatParameterTypeChanged{
							CaveatParameterTypeChanged: &v1.ExpCaveatParameterTypeChange{
								Parameter: &v1.ExpCaveatParameter{
									Name:             "someparam",
									Type:             "uint",
									ParentCaveatName: "someCaveat",
								},
								PreviousType: "int",
							},
						},
					},
				},
			},
		},
		{
			"caveat expression changes",
			`caveat someCaveat(someparam int) { someparam < 42 }`,
			`caveat someCaveat(someparam int) { someparam < 43 }`,
			&v1.ExperimentalSchemaDiffResponse{
				Diffs: []*v1.ExpSchemaDiff{
					{
						Diff: &v1.ExpSchemaDiff_CaveatExprChanged{
							CaveatExprChanged: &v1.ExpCaveat{
								Name:       "someCaveat",
								Comment:    "",
								Expression: "someparam < 43",
								Parameters: []*v1.ExpCaveatParameter{
									{
										Name:             "someparam",
										Type:             "int",
										ParentCaveatName: "someCaveat",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	encounteredDiffTypes := mapz.NewSet[string]()
	casesRun := 0

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			casesRun++

			existingSchema, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: tc.existingSchema,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			comparisonSchema, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: tc.comparisonSchema,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			es := diff.NewDiffableSchemaFromCompiledSchema(existingSchema)
			cs := diff.NewDiffableSchemaFromCompiledSchema(comparisonSchema)

			diff, err := diff.DiffSchemas(es, cs)
			require.NoError(t, err)

			resp, err := convertDiff(
				diff,
				&es,
				&cs,
				revisionparsing.MustParseRevisionForTest("1"),
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			require.NotNil(t, resp.ReadAt)
			resp.ReadAt = nil

			testutil.RequireProtoEqual(t, tc.expectedResponse, resp, "got mismatch")

			for _, diff := range resp.Diffs {
				name := reflect.TypeOf(diff.GetDiff()).String()
				encounteredDiffTypes.Add(strings.ToLower(strings.Split(name, "_")[1]))
			}
		})
	}

	if casesRun == len(tcs) {
		msg := &v1.ExpSchemaDiff{}

		allDiffTypes := mapz.NewSet[string]()
		fields := msg.ProtoReflect().Descriptor().Oneofs().ByName("diff").Fields()
		for i := 0; i < fields.Len(); i++ {
			allDiffTypes.Add(strings.ToLower(strcase.ToCamel(string(fields.Get(i).Name()))))
		}

		require.Empty(t, allDiffTypes.Subtract(encounteredDiffTypes).AsSlice())
	}
}
