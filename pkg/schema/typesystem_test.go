package schema

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
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

	// 10 outer iterations, 20 inner iterations, three namespaces
	errs := make(chan error, 600)

	for range 10 {
		wg.Add(1)
		go func() {
			for range 20 {
				for _, n := range []string{"document", "user", "team"} {
					_, err := ts.GetValidatedDefinition(ctx, n)
					errs <- err
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(err, "expected no errors in concurrent GetValidatedDefinition calls")
	}
}

func TestApplyExpirationFilter(t *testing.T) {
	testCases := []struct {
		name             string
		inputTraits      Traits
		expirationOption datastore.ExpirationFilterOption
		expectedTraits   Traits
		expectedError    bool
		errorContains    string
	}{
		{
			name: "none option - no change",
			inputTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expirationOption: datastore.ExpirationFilterOptionNone,
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
		{
			name: "none option - no change with false traits",
			inputTraits: Traits{
				AllowsCaveats:    false,
				AllowsExpiration: false,
			},
			expirationOption: datastore.ExpirationFilterOptionNone,
			expectedTraits: Traits{
				AllowsCaveats:    false,
				AllowsExpiration: false,
			},
			expectedError: false,
		},
		{
			name: "has expiration - traits support expiration",
			inputTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expirationOption: datastore.ExpirationFilterOptionHasExpiration,
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
		{
			name: "has expiration - traits don't support expiration - empty",
			inputTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: false,
			},
			expirationOption: datastore.ExpirationFilterOptionHasExpiration,
			expectedTraits:   Traits{},
			expectedError:    true,
			errorContains:    "the filter requested relationships with expiration but the filter relation does not support expiration",
		},
		{
			name: "no expiration - force expiration false",
			inputTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expirationOption: datastore.ExpirationFilterOptionNoExpiration,
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: false,
			},
			expectedError: false,
		},
		{
			name: "no expiration - already false expiration",
			inputTraits: Traits{
				AllowsCaveats:    false,
				AllowsExpiration: false,
			},
			expirationOption: datastore.ExpirationFilterOptionNoExpiration,
			expectedTraits: Traits{
				AllowsCaveats:    false,
				AllowsExpiration: false,
			},
			expectedError: false,
		},
		{
			name: "unknown option - no change",
			inputTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expirationOption: datastore.ExpirationFilterOption(999), // invalid option
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			resultTraits, err := applyExpirationFilter(tc.inputTraits, tc.expirationOption)

			if tc.expectedError {
				require.Error(err)
				if tc.errorContains != "" {
					require.Contains(err.Error(), tc.errorContains)
				}
				return
			}
			require.NoError(err)
			require.Equal(tc.expectedTraits.AllowsCaveats, resultTraits.AllowsCaveats, "AllowsCaveats mismatch")
			require.Equal(tc.expectedTraits.AllowsExpiration, resultTraits.AllowsExpiration, "AllowsExpiration mismatch")
		})
	}
}

func TestDirectPossibleTraitsForFilter(t *testing.T) {
	emptyEnv := caveats.NewEnvironmentWithDefaultTypeSet()

	// Create a simple schema for testing direct traits only
	setup := &PredefinedElements{
		Definitions: []*core.NamespaceDefinition{
			ns.Namespace("user"),
			ns.Namespace("resource",
				ns.MustRelation("viewer", nil,
					ns.AllowedRelation("user", "..."),
				),
				ns.MustRelation("editor", nil,
					ns.AllowedRelation("user", "..."),
					ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat")),
				),
				ns.MustRelation("admin", nil,
					ns.AllowedRelationWithExpiration("user", "..."),
				),
				ns.MustRelation("owner", nil,
					ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat")),
					ns.AllowedRelationWithExpiration("user", "..."),
				),
			),
		},
		Caveats: []*core.CaveatDefinition{
			ns.MustCaveatDefinition(emptyEnv, "somecaveat", "1 == 1"),
		},
	}

	ctx := context.Background()
	ts := NewTypeSystem(ResolverForPredefinedDefinitions(*setup))

	testCases := []struct {
		name           string
		filter         datastore.RelationshipsFilter
		expectedTraits Traits
		expectedError  bool
		errorContains  string
		expectError    bool
	}{
		{
			name:   "empty filter - no resource type",
			filter: datastore.RelationshipsFilter{},
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
		{
			name: "unknown resource type",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "nonexistent",
			},
			expectedTraits: Traits{},
			expectedError:  true,
			errorContains:  "resource type 'nonexistent' does not exist in schema",
		},
		{
			name: "resource type only - no relation specified",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "resource",
			},
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
		{
			name: "resource type with relation - no traits",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "viewer",
			},
			expectedTraits: Traits{
				AllowsCaveats:    false,
				AllowsExpiration: false,
			},
			expectedError: false,
		},
		{
			name: "resource type with relation - has caveats only",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "editor",
			},
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: false,
			},
			expectedError: false,
		},
		{
			name: "resource type with relation - has expiration only",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "admin",
			},
			expectedTraits: Traits{
				AllowsCaveats:    false,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
		{
			name: "resource type with relation - has both traits",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "owner",
			},
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
		{
			name: "unknown relation on known resource type",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "nonexistent",
			},
			expectedTraits: Traits{},
			expectedError:  true,
			errorContains:  "relation 'nonexistent' does not exist on resource type 'resource'",
		},
		{
			name: "with subject selector - specific subject type",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "owner",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{OptionalSubjectType: "user"},
				},
			},
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
		{
			name: "with subject selector - unknown subject type fallback",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{OptionalSubjectType: "nonexistent"},
				},
			},
			expectedTraits: Traits{
				AllowsCaveats:    false,
				AllowsExpiration: false,
			},
			expectedError: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			traits, err := ts.directPossibleTraitsForFilter(ctx, tc.filter)

			if tc.expectedError {
				require.Error(err)
				return
			}

			require.NoError(err)
			require.Equal(tc.expectedTraits.AllowsCaveats, traits.AllowsCaveats, "AllowsCaveats mismatch")
			require.Equal(tc.expectedTraits.AllowsExpiration, traits.AllowsExpiration, "AllowsExpiration mismatch")
		})
	}
}

// mockResolver is used to simulate resolver errors for edge case testing
type mockResolver struct {
	returnError error
}

func (mr *mockResolver) LookupDefinition(ctx context.Context, name string) (*core.NamespaceDefinition, bool, error) {
	if mr.returnError != nil {
		return nil, false, mr.returnError
	}
	return nil, false, fmt.Errorf("definition not found: %s", name)
}

func (mr *mockResolver) LookupCaveat(ctx context.Context, name string) (*Caveat, error) {
	return nil, fmt.Errorf("caveat not found: %s", name)
}

func TestDirectPossibleTraitsForFilterErrorCases(t *testing.T) {
	ctx := context.Background()

	// Test case for generic error (not DefinitionNotFoundError)
	t.Run("generic error from resolver", func(t *testing.T) {
		require := require.New(t)
		genericError := fmt.Errorf("database connection failed")
		resolver := &mockResolver{returnError: genericError}
		ts := NewTypeSystem(resolver)

		filter := datastore.RelationshipsFilter{
			OptionalResourceType: "test",
		}

		traits, err := ts.directPossibleTraitsForFilter(ctx, filter)

		require.Error(err)
		require.Equal(genericError, err)
		require.Equal(Traits{}, traits)
	})
}

func TestPossibleTraitsForFilter(t *testing.T) {
	emptyEnv := caveats.NewEnvironmentWithDefaultTypeSet()

	// Create a comprehensive schema with various trait combinations
	setup := &PredefinedElements{
		Definitions: []*core.NamespaceDefinition{
			ns.Namespace("user"),
			ns.Namespace("group",
				ns.MustRelation("member", nil, ns.AllowedRelation("user", "...")),
			),
			ns.Namespace("resource",
				ns.MustRelation("viewer", nil,
					ns.AllowedRelation("user", "..."),
				),
				ns.MustRelation("editor", nil,
					ns.AllowedRelation("user", "..."),
					ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat")),
				),
				ns.MustRelation("admin", nil,
					ns.AllowedRelationWithExpiration("user", "..."),
				),
				ns.MustRelation("owner", nil,
					ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat")),
					ns.AllowedRelationWithExpiration("user", "..."),
					ns.AllowedRelationWithCaveatAndExpiration("user", "...", ns.AllowedCaveat("somecaveat")),
				),
			),
			ns.Namespace("document",
				ns.MustRelation("viewer", nil,
					ns.AllowedRelation("user", "..."),
					ns.AllowedRelation("group", "member"),
				),
				ns.MustRelation("editor", nil,
					ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat")),
					ns.AllowedRelationWithExpiration("group", "member"),
				),
			),
		},
		Caveats: []*core.CaveatDefinition{
			ns.MustCaveatDefinition(emptyEnv, "somecaveat", "1 == 1"),
		},
	}

	ctx := context.Background()
	ts := NewTypeSystem(ResolverForPredefinedDefinitions(*setup))

	testCases := []struct {
		name           string
		filter         datastore.RelationshipsFilter
		expectedTraits Traits
		expectedError  bool
		errorContains  string
	}{
		{
			name:   "empty filter - no resource type",
			filter: datastore.RelationshipsFilter{},
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
		{
			name: "unknown resource type",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "nonexistent",
			},
			expectedTraits: Traits{},
			expectedError:  true,
			errorContains:  "resource type 'nonexistent' does not exist in schema",
		},
		{
			name: "resource type only - no relation specified",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "resource",
			},
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
		{
			name: "resource type with relation - no traits",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "viewer",
			},
			expectedTraits: Traits{
				AllowsCaveats:    false,
				AllowsExpiration: false,
			},
			expectedError: false,
		},
		{
			name: "resource type with relation - has caveats only",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "editor",
			},
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: false,
			},
			expectedError: false,
		},
		{
			name: "resource type with relation - has expiration only",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "admin",
			},
			expectedTraits: Traits{
				AllowsCaveats:    false,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
		{
			name: "resource type with relation - has both traits",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "owner",
			},
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
		{
			name: "resource type with relation and specific subject type - no traits",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{OptionalSubjectType: "user"},
				},
			},
			expectedTraits: Traits{
				AllowsCaveats:    false,
				AllowsExpiration: false,
			},
			expectedError: false,
		},
		{
			name: "resource type with relation and specific subject type - has caveats",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "editor",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{OptionalSubjectType: "user"},
				},
			},
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: false,
			},
			expectedError: false,
		},
		{
			name: "resource type with relation and multiple subject types - union of traits",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "editor",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{OptionalSubjectType: "user"},  // has caveats
					{OptionalSubjectType: "group"}, // has expiration
				},
			},
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
		{
			name: "resource type with relation and empty subject type - fallback to any subject",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "owner",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{OptionalSubjectType: ""}, // empty subject type
				},
			},
			expectedTraits: Traits{
				AllowsCaveats:    true,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
		{
			name: "unknown relation on known resource type",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "nonexistent",
			},
			expectedTraits: Traits{},
			expectedError:  true,
			errorContains:  "relation 'nonexistent' does not exist on resource type 'resource'",
		},
		{
			name: "unknown subject type on known relation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{OptionalSubjectType: "nonexistent"},
				},
			},
			expectedTraits: Traits{
				AllowsCaveats:    false,
				AllowsExpiration: false,
			},
			expectedError: false,
		},
	}

	// Add tests to cover integration with expiration filters
	expirationTestCases := []struct {
		name           string
		filter         datastore.RelationshipsFilter
		expectedTraits Traits
		expectedError  bool
		errorContains  string
	}{
		{
			name: "resource with expiration filter - has expiration",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "admin",
				OptionalExpirationOption: datastore.ExpirationFilterOptionHasExpiration,
			},
			expectedTraits: Traits{
				AllowsCaveats:    false,
				AllowsExpiration: true,
			},
			expectedError: false,
		},
		{
			name: "resource with expiration filter - no expiration on relation that supports expiration",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "admin",
				OptionalExpirationOption: datastore.ExpirationFilterOptionNoExpiration,
			},
			expectedTraits: Traits{
				AllowsCaveats:    false,
				AllowsExpiration: false,
			},
			expectedError: false,
		},
		{
			name: "resource with expiration filter - has expiration but relation doesn't support it",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "resource",
				OptionalResourceRelation: "viewer",
				OptionalExpirationOption: datastore.ExpirationFilterOptionHasExpiration,
			},
			expectedTraits: Traits{},
			expectedError:  true,
			errorContains:  "the filter requested relationships with expiration but the filter relation does not support expiration",
		},
	}

	// Run expiration filter tests for PossibleTraitsForFilter
	for _, tc := range expirationTestCases {
		tc := tc
		t.Run("PossibleTraitsForFilter with expiration: "+tc.name, func(t *testing.T) {
			require := require.New(t)

			traits, err := ts.PossibleTraitsForFilter(ctx, tc.filter)

			if tc.expectedError {
				require.Error(err)
				if tc.errorContains != "" {
					require.Contains(err.Error(), tc.errorContains)
				}
				return
			}

			require.NoError(err)
			require.Equal(tc.expectedTraits.AllowsCaveats, traits.AllowsCaveats, "AllowsCaveats mismatch")
			require.Equal(tc.expectedTraits.AllowsExpiration, traits.AllowsExpiration, "AllowsExpiration mismatch")
		})
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			traits, err := ts.PossibleTraitsForFilter(ctx, tc.filter)

			if tc.expectedError {
				require.Error(err)
				if tc.errorContains != "" {
					require.Contains(err.Error(), tc.errorContains)
				}
				return
			}

			require.NoError(err)
			require.Equal(tc.expectedTraits.AllowsCaveats, traits.AllowsCaveats, "AllowsCaveats mismatch")
			require.Equal(tc.expectedTraits.AllowsExpiration, traits.AllowsExpiration, "AllowsExpiration mismatch")
		})
	}
}
