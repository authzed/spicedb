package development

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestValidateCompiledPartials(t *testing.T) {
	tests := []struct {
		name           string
		schema         string
		expectErrors   bool
		errorSubstring string
		expectCount    int // when >0, asserts exact number of errors
	}{
		{
			name: "partial with undefined object type",
			schema: `
				use partial

				definition user {}

				partial secret {
					relation viewer: notfound
				}
			`,
			expectErrors:   true,
			errorSubstring: "could not lookup definition `notfound`",
		},
		{
			name: "partial referencing a relation only provided by consumer is allowed",
			schema: `
				use partial

				definition user {}

				partial view_partial {
					permission view = viewer
				}

				definition resource {
					relation viewer: user
					...view_partial
				}
			`,
			expectErrors: false,
		},
		{
			name: "partial with bad relation on real type",
			schema: `
				use partial

				definition user {}

				partial secret {
					relation viewer: user#nonexistent
				}
			`,
			expectErrors:   true,
			errorSubstring: "relation/permission `nonexistent` not found",
		},
		{
			name: "partial with undefined caveat",
			schema: `
				use partial

				definition user {}

				partial secret {
					relation viewer: user with missingcaveat
				}
			`,
			expectErrors:   true,
			errorSubstring: "could not lookup caveat `missingcaveat`",
		},
		{
			name: "partial composing another partial via splat is accepted",
			schema: `
				use partial

				definition user {}

				partial base_partial {
					relation owner: user
				}

				partial derived_partial {
					...base_partial
				}
			`,
			expectErrors: false,
		},
		{
			name: "transitive partial error is reported exactly once",
			schema: `
				use partial

				definition user {}

				partial base_partial {
					relation owner: notfound
				}

				partial derived_partial {
					...base_partial
				}
			`,
			expectErrors:   true,
			errorSubstring: "could not lookup definition `notfound`",
			expectCount:    1,
		},
		{
			name: "well-formed partial with consumer is clean",
			schema: `
				use partial

				definition user {}

				partial view_partial {
					relation viewer: user
					permission view = viewer
				}

				definition resource {
					...view_partial
				}
			`,
			expectErrors: false,
		},
		{
			name: "partial referencing another partial as a type is rejected",
			schema: `
				use partial

				definition user {}

				partial holder {
					relation owner: user
				}

				partial bad {
					relation viewer: holder
				}
			`,
			expectErrors:   true,
			errorSubstring: "could not lookup definition `holder`",
		},
		{
			name: "partial referencing itself as a type is rejected",
			schema: `
				use partial

				definition user {}

				partial bad {
					relation viewer: bad
				}
			`,
			expectErrors:   true,
			errorSubstring: "could not lookup definition `bad`",
		},
		{
			name: "broken partial consumed by a definition is not double-reported here",
			schema: `
				use partial

				definition user {}

				partial broken {
					relation viewer: notfound
				}

				definition resource {
					...broken
				}
			`,
			// Consumer validation in loadCompiled will catch this; emitting it
			// here as well would produce duplicate diagnostics.
			expectErrors: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := compiler.Compile(
				compiler.InputSchema{Source: input.Source("test"), SchemaString: tc.schema},
				compiler.AllowUnprefixedObjectType(),
			)
			require.NoError(t, err, "schema should compile")

			errs := validateCompiledPartials(t.Context(), compiled)
			if tc.expectErrors {
				require.NotEmpty(t, errs)
				if tc.expectCount > 0 {
					require.Len(t, errs, tc.expectCount, "errors: %v", errs)
				}
				if tc.errorSubstring != "" {
					var found bool
					for _, e := range errs {
						if strings.Contains(e.GetMessage(), tc.errorSubstring) {
							found = true
							break
						}
					}
					require.True(t, found, "expected error containing %q, got %v", tc.errorSubstring, errs)
				}
			} else {
				require.Empty(t, errs, "unexpected errors: %v", errs)
			}
		})
	}
}

// TestPartialErrorReportedAgainstPartialPath asserts that a partial with a
// schema-level error in its body is reported against the partial's own
// declaration (with the partial-validation path), proving the error is no
// longer attributed solely to whatever definition first inlines the partial.
//
// Beyond emitting an error, the path/line/column must point at the partial's
// source so editors can show the diagnostic in the right place.
func TestPartialErrorReportedAgainstPartialPath(t *testing.T) {
	schema := `
		use partial

		definition user {}

		partial secret {
			relation viewer: notfound
		}
	`
	_, devErrs, err := NewDevContext(
		t.Context(),
		&devinterface.RequestContext{Schema: schema},
	)
	require.NoError(t, err)
	require.NotNil(t, devErrs)

	inputErrors := devErrs.GetInputErrors()
	require.Len(t, inputErrors, 1, "expected exactly one input error, got: %v", inputErrors)

	gotErr := inputErrors[0]
	require.Contains(t, gotErr.GetMessage(), "could not lookup definition `notfound`")
	require.Equal(t, []string{"schema"}, gotErr.GetPath(),
		"error path should point at the partial's source, not the empty/default path")
	require.NotZero(t, gotErr.GetLine(),
		"error line should point at the `notfound` reference inside the partial body")
	require.NotZero(t, gotErr.GetColumn(),
		"error column should point at the `notfound` reference inside the partial body")
}

// TestUnusedPartialWithBadPermissionBodyIsNotFlagged documents the intentional
// scope of validateCompiledPartials: it only resolves allowedDirectRelation and
// requiredCaveat references, NOT computed-userset / TTU operands in permission
// bodies. Those depend on consumer-supplied relations and would produce false
// positives when validated against a partial in isolation. The consumer's
// typesystem pass catches them once the partial is inlined.
func TestUnusedPartialWithBadPermissionBodyIsNotFlagged(t *testing.T) {
	// `view = nonexistent` references an unknown relation; `view2 = parent->bar`
	// references an unknown LHS relation. Neither is resolvable until a consumer
	// supplies those relations, so neither may be flagged here.
	schema := `
		use partial

		definition user {}

		partial bodies {
			permission view  = nonexistent
			permission view2 = parent->bar
		}
	`
	compiled, err := compiler.Compile(
		compiler.InputSchema{Source: input.Source("test"), SchemaString: schema},
		compiler.AllowUnprefixedObjectType(),
	)
	require.NoError(t, err, "schema should compile")

	errs := validateCompiledPartials(t.Context(), compiled)
	require.Empty(t, errs, "permission-body references must not be flagged at partial-definition time, got: %v", errs)
}
