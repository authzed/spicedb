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
		expectedResponse *v1.DiffSchemaResponse
	}{
		{
			"no diff",
			`definition user {}`,
			`definition user {}`,
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{},
			},
		},
		{
			"add namespace",
			``,
			`definition user {}`,
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_DefinitionAdded{
							DefinitionAdded: &v1.ReflectionDefinition{
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_DefinitionRemoved{
							DefinitionRemoved: &v1.ReflectionDefinition{
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_DefinitionDocCommentChanged{
							DefinitionDocCommentChanged: &v1.ReflectionDefinition{
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_CaveatAdded{
							CaveatAdded: &v1.ReflectionCaveat{
								Name:       "someCaveat",
								Comment:    "",
								Expression: "someparam < 42",
								Parameters: []*v1.ReflectionCaveatParameter{
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_CaveatRemoved{
							CaveatRemoved: &v1.ReflectionCaveat{
								Name:       "someCaveat",
								Comment:    "",
								Expression: "someparam < 42",
								Parameters: []*v1.ReflectionCaveatParameter{
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_CaveatDocCommentChanged{
							CaveatDocCommentChanged: &v1.ReflectionCaveat{
								Name:       "someCaveat",
								Comment:    "// someCaveat has b comment",
								Expression: "someparam < 42",
								Parameters: []*v1.ReflectionCaveatParameter{
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_RelationAdded{
							RelationAdded: &v1.ReflectionRelation{
								Name:                 "somerel",
								Comment:              "",
								ParentDefinitionName: "user",
								SubjectTypes: []*v1.ReflectionTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_RelationRemoved{
							RelationRemoved: &v1.ReflectionRelation{
								Name:                 "somerel",
								Comment:              "",
								ParentDefinitionName: "user",
								SubjectTypes: []*v1.ReflectionTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_RelationSubjectTypeAdded{
							RelationSubjectTypeAdded: &v1.ReflectionRelationSubjectTypeChange{
								Relation: &v1.ReflectionRelation{
									Name:                 "viewer",
									Comment:              "",
									ParentDefinitionName: "resource",
									SubjectTypes: []*v1.ReflectionTypeReference{
										{
											SubjectDefinitionName: "user",
											Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
										},
										{
											SubjectDefinitionName: "anon",
											Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
										},
									},
								},
								ChangedSubjectType: &v1.ReflectionTypeReference{
									SubjectDefinitionName: "user",
									Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_RelationSubjectTypeRemoved{
							RelationSubjectTypeRemoved: &v1.ReflectionRelationSubjectTypeChange{
								Relation: &v1.ReflectionRelation{
									Name:                 "viewer",
									Comment:              "",
									ParentDefinitionName: "resource",
									SubjectTypes: []*v1.ReflectionTypeReference{
										{
											SubjectDefinitionName: "user",
											Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
										},
									},
								},
								ChangedSubjectType: &v1.ReflectionTypeReference{
									SubjectDefinitionName: "anon",
									Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_RelationDocCommentChanged{
							RelationDocCommentChanged: &v1.ReflectionRelation{
								Name:                 "viewer",
								Comment:              "// viewer has a comment",
								ParentDefinitionName: "resource",
								SubjectTypes: []*v1.ReflectionTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_PermissionAdded{
							PermissionAdded: &v1.ReflectionPermission{
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_PermissionRemoved{
							PermissionRemoved: &v1.ReflectionPermission{
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_PermissionDocCommentChanged{
							PermissionDocCommentChanged: &v1.ReflectionPermission{
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_PermissionExprChanged{
							PermissionExprChanged: &v1.ReflectionPermission{
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_CaveatParameterAdded{
							CaveatParameterAdded: &v1.ReflectionCaveatParameter{
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_CaveatParameterRemoved{
							CaveatParameterRemoved: &v1.ReflectionCaveatParameter{
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_CaveatParameterTypeChanged{
							CaveatParameterTypeChanged: &v1.ReflectionCaveatParameterTypeChange{
								Parameter: &v1.ReflectionCaveatParameter{
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
			&v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_CaveatExprChanged{
							CaveatExprChanged: &v1.ReflectionCaveat{
								Name:       "someCaveat",
								Comment:    "",
								Expression: "someparam < 43",
								Parameters: []*v1.ReflectionCaveatParameter{
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
		msg := &v1.ReflectionSchemaDiff{}

		allDiffTypes := mapz.NewSet[string]()
		fields := msg.ProtoReflect().Descriptor().Oneofs().ByName("diff").Fields()
		for i := 0; i < fields.Len(); i++ {
			allDiffTypes.Add(strings.ToLower(strcase.ToCamel(string(fields.Get(i).Name()))))
		}

		require.Empty(t, allDiffTypes.Subtract(encounteredDiffTypes).AsSlice())
	}
}

type filterCheck func(sf *schemaFilters) bool

func TestSchemaFiltering(t *testing.T) {
	tcs := []struct {
		name     string
		filters  []*v1.ReflectionSchemaFilter
		checkers []filterCheck
	}{
		{
			"no filters",
			[]*v1.ReflectionSchemaFilter{},
			[]filterCheck{
				func(sf *schemaFilters) bool { return sf.HasNamespaces() },
				func(sf *schemaFilters) bool { return sf.HasCaveats() },
				func(sf *schemaFilters) bool { return sf.HasNamespace("foo") },
				func(sf *schemaFilters) bool { return sf.HasCaveat("foo") },
				func(sf *schemaFilters) bool { return sf.HasRelation("document", "viewer") },
				func(sf *schemaFilters) bool { return sf.HasPermission("document", "view") },
			},
		},
		{
			"namespace filter",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
				},
			},
			[]filterCheck{
				func(sf *schemaFilters) bool { return sf.HasNamespaces() },
				func(sf *schemaFilters) bool { return !sf.HasCaveats() },
				func(sf *schemaFilters) bool { return sf.HasNamespace("document") },
				func(sf *schemaFilters) bool { return !sf.HasNamespace("foo") },
				func(sf *schemaFilters) bool { return sf.HasRelation("document", "viewer") },
				func(sf *schemaFilters) bool { return sf.HasPermission("document", "view") },
			},
		},
		{
			"caveat filter",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalCaveatNameFilter: "somec",
				},
			},
			[]filterCheck{
				func(sf *schemaFilters) bool { return !sf.HasNamespaces() },
				func(sf *schemaFilters) bool { return sf.HasCaveats() },
				func(sf *schemaFilters) bool { return sf.HasCaveat("somecaveat") },
				func(sf *schemaFilters) bool { return !sf.HasCaveat("foo") },
			},
		},
		{
			"multiple namespace filters",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
				},
				{
					OptionalDefinitionNameFilter: "user",
				},
			},
			[]filterCheck{
				func(sf *schemaFilters) bool { return sf.HasNamespaces() },
				func(sf *schemaFilters) bool { return !sf.HasCaveats() },
				func(sf *schemaFilters) bool { return sf.HasNamespace("document") },
				func(sf *schemaFilters) bool { return sf.HasNamespace("user") },
				func(sf *schemaFilters) bool { return !sf.HasNamespace("foo") },
				func(sf *schemaFilters) bool { return sf.HasRelation("document", "viewer") },
				func(sf *schemaFilters) bool { return sf.HasPermission("document", "view") },
				func(sf *schemaFilters) bool { return sf.HasRelation("user", "viewer") },
				func(sf *schemaFilters) bool { return sf.HasPermission("user", "view") },
			},
		},
		{
			"multiple caveat filters",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalCaveatNameFilter: "somec",
				},
				{
					OptionalCaveatNameFilter: "somec2",
				},
			},
			[]filterCheck{
				func(sf *schemaFilters) bool { return !sf.HasNamespaces() },
				func(sf *schemaFilters) bool { return sf.HasCaveats() },
				func(sf *schemaFilters) bool { return sf.HasCaveat("somecaveat") },
				func(sf *schemaFilters) bool { return sf.HasCaveat("somecaveat2") },
				func(sf *schemaFilters) bool { return !sf.HasCaveat("foo") },
			},
		},
		{
			"namespace and caveat filters",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
				},
				{
					OptionalCaveatNameFilter: "somec",
				},
			},
			[]filterCheck{
				func(sf *schemaFilters) bool { return sf.HasNamespaces() },
				func(sf *schemaFilters) bool { return sf.HasCaveats() },
				func(sf *schemaFilters) bool { return sf.HasNamespace("document") },
				func(sf *schemaFilters) bool { return sf.HasCaveat("somecaveat") },
				func(sf *schemaFilters) bool { return !sf.HasNamespace("foo") },
				func(sf *schemaFilters) bool { return !sf.HasCaveat("foo") },
			},
		},
		{
			"relation filter",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalRelationNameFilter:   "v",
				},
			},
			[]filterCheck{
				func(sf *schemaFilters) bool { return sf.HasNamespaces() },
				func(sf *schemaFilters) bool { return !sf.HasCaveats() },
				func(sf *schemaFilters) bool { return sf.HasNamespace("document") },
				func(sf *schemaFilters) bool { return sf.HasRelation("document", "viewer") },
				func(sf *schemaFilters) bool { return !sf.HasRelation("document", "foo") },
			},
		},
		{
			"permission filter",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalPermissionNameFilter: "v",
				},
			},
			[]filterCheck{
				func(sf *schemaFilters) bool { return sf.HasNamespaces() },
				func(sf *schemaFilters) bool { return !sf.HasCaveats() },
				func(sf *schemaFilters) bool { return sf.HasNamespace("document") },
				func(sf *schemaFilters) bool { return sf.HasPermission("document", "view") },
				func(sf *schemaFilters) bool { return !sf.HasPermission("document", "foo") },
			},
		},
		{
			"permission and relation filter",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalPermissionNameFilter: "r",
				},
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalRelationNameFilter:   "v",
				},
			},
			[]filterCheck{
				func(sf *schemaFilters) bool { return sf.HasNamespaces() },
				func(sf *schemaFilters) bool { return !sf.HasCaveats() },
				func(sf *schemaFilters) bool { return sf.HasNamespace("document") },
				func(sf *schemaFilters) bool { return sf.HasRelation("document", "viewer") },
				func(sf *schemaFilters) bool { return sf.HasPermission("document", "read") },
				func(sf *schemaFilters) bool { return !sf.HasRelation("document", "foo") },
				func(sf *schemaFilters) bool { return !sf.HasPermission("document", "foo") },
			},
		},
		{
			"permission and relation filter over different definitions",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalPermissionNameFilter: "r",
				},
				{
					OptionalDefinitionNameFilter: "user",
					OptionalRelationNameFilter:   "v",
				},
			},
			[]filterCheck{
				func(sf *schemaFilters) bool { return sf.HasNamespaces() },
				func(sf *schemaFilters) bool { return !sf.HasCaveats() },
				func(sf *schemaFilters) bool { return sf.HasNamespace("document") },
				func(sf *schemaFilters) bool { return sf.HasNamespace("user") },
				func(sf *schemaFilters) bool { return sf.HasRelation("user", "viewer") },
				func(sf *schemaFilters) bool { return sf.HasPermission("document", "read") },
				func(sf *schemaFilters) bool { return !sf.HasRelation("document", "viewer") },
				func(sf *schemaFilters) bool { return !sf.HasPermission("user", "read") },
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sf, err := newSchemaFilters(tc.filters)
			require.NoError(t, err)

			for index, check := range tc.checkers {
				require.True(t, check(sf), "check failed: #%d", index)
			}
		})
	}
}

func TestNewSchemaFilters(t *testing.T) {
	tcs := []struct {
		name    string
		filters []*v1.ReflectionSchemaFilter
		err     string
	}{
		{
			"no filters",
			[]*v1.ReflectionSchemaFilter{},
			"",
		},
		{
			"namespace filter",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
				},
			},
			"",
		},
		{
			"caveat filter",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalCaveatNameFilter: "somec",
				},
			},
			"",
		},
		{
			"relation filter",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalRelationNameFilter:   "v",
				},
			},
			"",
		},
		{
			"permission filter",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalPermissionNameFilter: "v",
				},
			},
			"",
		},
		{
			"permission and relation filter",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalPermissionNameFilter: "r",
				},
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalRelationNameFilter:   "v",
				},
			},
			"",
		},
		{
			"relation filter without definition",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalRelationNameFilter: "v",
				},
			},
			"relation name match requires definition name match",
		},
		{
			"permission filter without definition",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalPermissionNameFilter: "v",
				},
			},
			"permission name match requires definition name match",
		},
		{
			"filter with both definition and caveat",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalCaveatNameFilter:     "somec",
				},
			},
			"cannot filter by both definition and caveat name",
		},
		{
			"filter with both relation and permission",
			[]*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalRelationNameFilter:   "v",
					OptionalPermissionNameFilter: "r",
				},
			},
			"cannot filter by both relation and permission name",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := newSchemaFilters(tc.filters)
			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.err)
			}
		})
	}
}
