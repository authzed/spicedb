package schema

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestTypeSystemConcurrency(t *testing.T) {
	emptyEnv := caveats.NewEnvironmentWithDefaultTypeSet()
	setup := &PredefinedElements{
		Definitions: []*core.NamespaceDefinition{
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil,
					ns.AllowedRelationWithExpiration("user", "..."),
					ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("definedcaveat")),
				),
			),
			ns.Namespace("user"),
			ns.Namespace("team",
				ns.MustRelation("member", nil),
			),
		},
		Caveats: []*core.CaveatDefinition{
			ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
		},
	}

	var wg sync.WaitGroup
	ctx := t.Context()
	ts := NewTypeSystem(ResolverForPredefinedDefinitions(*setup))
	require := require.New(t)
	for range 10 {
		wg.Add(1)
		go func() {
			for range 20 {
				for _, n := range []string{"document", "user", "team"} {
					_, err := ts.GetValidatedDefinition(ctx, n)
					require.NoError(err)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestGetDeprecationForRelation(t *testing.T) {
	t.Parallel()

	emptyEnv := caveats.NewEnvironmentWithDefaultTypeSet()

	type deprecationTest struct {
		name                string
		element             PredefinedElements
		testObject          string
		testRel             string
		expectedDeprecation *core.Deprecation
		expectedError       string
	}

	tests := []deprecationTest{
		{
			"deprecated relation with a caveat in a object reference",
			PredefinedElements{
				Definitions: []*core.NamespaceDefinition{
					ns.WithDeprecation(
						"document",
						ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "some important error here"),
						ns.MustRelationWithDeprecation("viewer", ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "some error"),
							nil,
							ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("definedcaveat")),
						),
					),
					ns.Namespace("user"),
				},
				Caveats: []*core.CaveatDefinition{
					ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
				},
			},
			"document",
			"viewer",
			ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "some error"),
			"",
		},
		{
			"simple relation deprecation",
			PredefinedElements{
				Definitions: []*core.NamespaceDefinition{
					ns.Namespace(
						"document",
						ns.MustRelationWithDeprecation("editor", ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "testerr"),
							nil,
							ns.AllowedRelation("user", "..."),
						),
					),
					ns.Namespace("user"),
				},
				Caveats: []*core.CaveatDefinition{
					ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
				},
			},
			"document",
			"editor",
			ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "testerr"),
			"",
		},
		{
			"relation deprecation with caveat and expiration in a object reference",
			PredefinedElements{
				Definitions: []*core.NamespaceDefinition{
					ns.Namespace(
						"document",
						ns.MustRelationWithDeprecation("editor", ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_WARNING, "testerr"),
							nil,
							ns.AllowedRelationWithCaveatAndExpiration("user", "...", ns.AllowedCaveat("definedcaveat")),
						),
					),
					ns.WithDeprecation("user", ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "caveat test dep err")),
				},
				Caveats: []*core.CaveatDefinition{
					ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
				},
			},
			"document",
			"editor",
			ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_WARNING, "testerr"),
			"",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)
			ctx := t.Context()
			ts := NewTypeSystem(ResolverForPredefinedDefinitions(test.element))
			dep, _, err := ts.GetDeprecationForRelation(ctx, test.testObject, test.testRel)
			if test.expectedError != "" {
				require.Equal(err.Error(), test.expectedError)
			}
			require.Equal(dep, test.expectedDeprecation)
		})
	}
}

func TestGetDeprecationForObjectType(t *testing.T) {
	t.Parallel()

	emptyEnv := caveats.NewEnvironmentWithDefaultTypeSet()

	type deprecationTest struct {
		name                string
		element             PredefinedElements
		testObject          string
		expectedDeprecation *core.Deprecation
		expectedError       string
	}

	tests := []deprecationTest{
		{
			"subject deprecation",
			PredefinedElements{
				Definitions: []*core.NamespaceDefinition{
					ns.WithDeprecation(
						"document",
						ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "some important error here"),
						ns.MustRelationWithDeprecation("viewer", ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "some error"),
							nil,
							ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("definedcaveat")),
						),
					),
					ns.Namespace("user"),
				},
				Caveats: []*core.CaveatDefinition{
					ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
				},
			},
			"document",
			ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "some important error here"),
			"",
		},
		{
			"object deprecation",
			PredefinedElements{
				Definitions: []*core.NamespaceDefinition{
					ns.Namespace(
						"document",
						ns.MustRelation("viewer",
							nil,
							ns.AllowedRelation("user", "..."),
						),
					),
					ns.WithDeprecation("user", ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "other error here")),
				},
				Caveats: []*core.CaveatDefinition{
					ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
				},
			},
			"user",
			ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "other error here"),
			"",
		},
		{
			"deprecation in object reference with a caveat",
			PredefinedElements{
				Definitions: []*core.NamespaceDefinition{
					ns.Namespace(
						"document",
						ns.MustRelation("viewer",
							nil,
							ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("definedcaveat")),
						),
					),
					ns.WithDeprecation("user", ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "caveat test dep err")),
				},
				Caveats: []*core.CaveatDefinition{
					ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
				},
			},
			"user",
			ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "caveat test dep err"),
			"",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)
			ctx := t.Context()
			ts := NewTypeSystem(ResolverForPredefinedDefinitions(test.element))
			dep, _, err := ts.GetDeprecationForObjectType(ctx, test.testObject)
			if test.expectedError != "" {
				require.Equal(err.Error(), test.expectedError)
			}
			require.Equal(dep, test.expectedDeprecation)
		})
	}
}

func TestGetDeprecationForAllowedRelation(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name                string
		element             PredefinedElements
		args                AllowedRelationTraits
		expectedDeprecation *core.Deprecation
	}

	emptyEnv := caveats.NewEnvironmentWithDefaultTypeSet()
	tests := []testCase{
		{
			name: "allowed relation with caveat and deprecation",
			element: PredefinedElements{
				Definitions: []*core.NamespaceDefinition{
					ns.Namespace("document",
						ns.MustRelation("viewer", nil,
							ns.AllowedRelationWithDeprecation(
								ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("definedcaveat")),
								ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_WARNING, "caveat warning"),
							),
						),
					),
					ns.Namespace("user"),
				},
				Caveats: []*core.CaveatDefinition{
					ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
				},
			},
			args: AllowedRelationTraits{
				ResourceNamespace: "document",
				ResourceRelation:  "viewer",
				SubjectNamespace:  "user",
				SubjectRelation:   "...",
				IsWildcard:        false,
				HasCaveat:         true,
				HasExpiration:     false,
			},

			expectedDeprecation: ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_WARNING, "caveat warning"),
		},
		{
			name: "allowed relation with expiration and deprecation",
			element: PredefinedElements{
				Definitions: []*core.NamespaceDefinition{
					ns.Namespace("document",
						ns.MustRelation("editor", nil,
							ns.AllowedRelationWithDeprecation(
								ns.AllowedRelationWithExpiration("team", "..."),
								ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "expired deprecation"),
							),
						),
					),
					ns.Namespace("team"),
				},
			},
			args: AllowedRelationTraits{
				ResourceNamespace: "document",
				ResourceRelation:  "editor",
				SubjectNamespace:  "team",
				SubjectRelation:   "...",
				IsWildcard:        false,
				HasCaveat:         false,
				HasExpiration:     true,
			},
			expectedDeprecation: ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "expired deprecation"),
		},
		{
			name: "allowed relation with caveat, expiration and deprecation",
			element: PredefinedElements{
				Definitions: []*core.NamespaceDefinition{
					ns.Namespace("document",
						ns.MustRelation("viewer", nil,
							ns.AllowedRelationWithDeprecation(
								ns.AllowedRelationWithCaveatAndExpiration(
									"user",
									"...",
									ns.AllowedCaveat("definedcaveat"),
								),
								ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "all features"),
							),
						),
					),
					ns.Namespace("user"),
				},
				Caveats: []*core.CaveatDefinition{
					ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "true"),
				},
			},
			args: AllowedRelationTraits{
				ResourceNamespace: "document",
				ResourceRelation:  "viewer",
				SubjectNamespace:  "user",
				SubjectRelation:   "...",
				IsWildcard:        false,
				HasCaveat:         true,
				HasExpiration:     true,
			},
			expectedDeprecation: ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_ERROR, "all features"),
		},
		{
			name: "allowed relation with wildcard",
			element: PredefinedElements{
				Definitions: []*core.NamespaceDefinition{
					ns.Namespace("document",
						ns.MustRelation("viewer", nil,
							ns.AllowedPublicNamespaceWithDeprecation("user", ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_WARNING, "wildcard")),
						),
					),
					ns.Namespace("user"),
				},
			},
			args: AllowedRelationTraits{
				ResourceNamespace: "document",
				ResourceRelation:  "viewer",
				SubjectNamespace:  "user",
				SubjectRelation:   "*",
				IsWildcard:        true,
				HasCaveat:         false,
				HasExpiration:     false,
			},
			expectedDeprecation: ns.Deprecation(core.DeprecationType_DEPRECATED_TYPE_WARNING, "wildcard"),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)
			ctx := t.Context()
			ts := NewTypeSystem(ResolverForPredefinedDefinitions(tc.element))
			dep, _, err := ts.GetDeprecationForAllowedRelation(ctx, tc.args)
			require.NoError(err)
			require.Equal(tc.expectedDeprecation, dep)
		})
	}
}
