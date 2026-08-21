package decorators

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func allowAll(string) bool { return true }
func denyAll(string) bool  { return false }

func ident(v string) Value { return Value{Kind: ValueKindIdentifier, Raw: v} }
func str(v string) Value   { return Value{Kind: ValueKindString, Raw: v} }

func TestValidate(t *testing.T) {
	tests := []struct {
		name        string
		applied     Applied
		site        Site
		flagEnabled func(string) bool
		expectedErr string
	}{
		{
			name:        "no parameters",
			applied:     Applied{Name: "testdef"},
			site:        SiteDefinition,
			flagEnabled: allowAll,
		},
		{
			name: "all parameter types",
			applied: Applied{Name: "testall", Parameters: []AppliedParameter{
				{Name: "needed", Value: ident("1")},
				{Name: "count", Value: ident("-16")},
				{Name: "label", Value: str("hi")},
				{Name: "on", Value: ident("true")},
				{Name: "mode", Value: ident("hash")},
			}},
			site:        SiteDefinition,
			flagEnabled: allowAll,
		},
		{
			name:        "unknown decorator",
			applied:     Applied{Name: "nope"},
			site:        SiteDefinition,
			flagEnabled: allowAll,
			expectedErr: "unknown decorator `@nope`",
		},
		{
			name:        "flag not enabled",
			applied:     Applied{Name: "testdef"},
			site:        SiteDefinition,
			flagEnabled: denyAll,
			expectedErr: "decorator `@testdef` requires `use " + TestFlag + "`",
		},
		{
			name:        "illegal site",
			applied:     Applied{Name: "testdef"},
			site:        SiteRelation,
			flagEnabled: allowAll,
			expectedErr: "decorator `@testdef` is not permitted on a relation",
		},
		{
			name: "unknown parameter",
			applied: Applied{Name: "testall", Parameters: []AppliedParameter{
				{Name: "needed", Value: ident("1")},
				{Name: "bogus", Value: ident("1")},
			}},
			site:        SiteDefinition,
			flagEnabled: allowAll,
			expectedErr: "unknown parameter `bogus` for decorator `@testall`",
		},
		{
			name: "duplicate parameter",
			applied: Applied{Name: "testall", Parameters: []AppliedParameter{
				{Name: "needed", Value: ident("1")},
				{Name: "needed", Value: ident("2")},
			}},
			site:        SiteDefinition,
			flagEnabled: allowAll,
			expectedErr: "parameter `needed` specified more than once",
		},
		{
			name:        "missing required parameter",
			applied:     Applied{Name: "testall"},
			site:        SiteDefinition,
			flagEnabled: allowAll,
			expectedErr: "missing required parameter `needed` for decorator `@testall`",
		},
		{
			name: "int given a string",
			applied: Applied{Name: "testall", Parameters: []AppliedParameter{
				{Name: "needed", Value: str("1")},
			}},
			site:        SiteDefinition,
			flagEnabled: allowAll,
			expectedErr: "parameter `needed` of decorator `@testall` expects an integer",
		},
		{
			name: "int given a non-number",
			applied: Applied{Name: "testall", Parameters: []AppliedParameter{
				{Name: "needed", Value: ident("hash")},
			}},
			site:        SiteDefinition,
			flagEnabled: allowAll,
			expectedErr: "parameter `needed` of decorator `@testall` expects an integer",
		},
		{
			name: "string given an identifier",
			applied: Applied{Name: "testall", Parameters: []AppliedParameter{
				{Name: "needed", Value: ident("1")},
				{Name: "label", Value: ident("hi")},
			}},
			site:        SiteDefinition,
			flagEnabled: allowAll,
			expectedErr: "parameter `label` of decorator `@testall` expects a quoted string",
		},
		{
			name: "bool given a non-bool",
			applied: Applied{Name: "testall", Parameters: []AppliedParameter{
				{Name: "needed", Value: ident("1")},
				{Name: "on", Value: ident("yes")},
			}},
			site:        SiteDefinition,
			flagEnabled: allowAll,
			expectedErr: "parameter `on` of decorator `@testall` expects true or false",
		},
		{
			name: "enum out of range",
			applied: Applied{Name: "testall", Parameters: []AppliedParameter{
				{Name: "needed", Value: ident("1")},
				{Name: "mode", Value: ident("nope")},
			}},
			site:        SiteDefinition,
			flagEnabled: allowAll,
			expectedErr: "invalid value `nope` for parameter `mode` of decorator `@testall`; expected one of: hash, range",
		},
		{
			// The generator can only regenerate a string value using a delimiter (` or ")
			// the value does not contain, since the DSL has no escape syntax. A value
			// containing both is unrepresentable, so it must be rejected here rather than
			// silently corrupting generated output.
			name: "string containing both quote styles",
			applied: Applied{Name: "testall", Parameters: []AppliedParameter{
				{Name: "needed", Value: ident("1")},
				{Name: "label", Value: str(`he said "it's mine"`)},
			}},
			site:        SiteDefinition,
			flagEnabled: allowAll,
			expectedErr: "parameter `label` of decorator `@testall` contains characters that cannot be represented in a schema string: a value may not contain both quote styles",
		},
		{
			// Single/double-quoted strings in the DSL are single-line only, so a value
			// containing a real newline cannot be regenerated as valid schema source.
			name: "string containing a newline",
			applied: Applied{Name: "testall", Parameters: []AppliedParameter{
				{Name: "needed", Value: ident("1")},
				{Name: "label", Value: str("line one\nline two")},
			}},
			site:        SiteDefinition,
			flagEnabled: allowAll,
			expectedErr: "parameter `label` of decorator `@testall` may not contain a newline",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result, err := TestRegistry.Validate(test.applied, test.site, test.flagEnabled, allowAll)
			if test.expectedErr != "" {
				require.ErrorContains(t, err, test.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.applied.Name, result.GetName())
			require.Equal(t, TestFlag, result.GetRequiredFlag())
		})
	}
}

func TestValidateFlagNotAllowedByDeployment(t *testing.T) {
	t.Parallel()
	_, err := TestRegistry.Validate(Applied{Name: "testdef"}, SiteDefinition, allowAll, denyAll)
	require.ErrorContains(t, err, "the `"+TestFlag+"` flag is not allowed")
}

func TestValidateCoercesValues(t *testing.T) {
	t.Parallel()
	result, err := TestRegistry.Validate(Applied{Name: "testall", Parameters: []AppliedParameter{
		{Name: "needed", Value: ident("7")},
		{Name: "count", Value: ident("-16")},
		{Name: "label", Value: str("hi")},
		{Name: "on", Value: ident("true")},
		{Name: "mode", Value: ident("hash")},
	}}, SiteDefinition, allowAll, allowAll)
	require.NoError(t, err)

	params := result.GetParameters()
	require.Len(t, params, 5)
	require.Equal(t, int64(7), params[0].GetIntValue())
	require.Equal(t, int64(-16), params[1].GetIntValue())
	require.Equal(t, "hi", params[2].GetStringValue())
	require.True(t, params[3].GetBoolValue())
	require.Equal(t, "hash", params[4].GetEnumValue())
}

// TestValidateEmitsSpecOrderRegardlessOfSourceOrder supplies parameters in an order that
// does NOT match the spec's canonical order (mode, label, needed instead of needed, ...,
// mode). If Validate ever emitted parameters in source order instead of spec order, this
// test would catch it: the parameter names, not just their values, are asserted per index.
func TestValidateEmitsSpecOrderRegardlessOfSourceOrder(t *testing.T) {
	t.Parallel()
	result, err := TestRegistry.Validate(Applied{Name: "testall", Parameters: []AppliedParameter{
		{Name: "mode", Value: ident("hash")},
		{Name: "label", Value: str("hi")},
		{Name: "needed", Value: ident("1")},
	}}, SiteDefinition, allowAll, allowAll)
	require.NoError(t, err)

	params := result.GetParameters()
	require.Len(t, params, 3)

	require.Equal(t, "needed", params[0].GetName())
	require.Equal(t, int64(1), params[0].GetIntValue())

	require.Equal(t, "label", params[1].GetName())
	require.Equal(t, "hi", params[1].GetStringValue())

	require.Equal(t, "mode", params[2].GetName())
	require.Equal(t, "hash", params[2].GetEnumValue())
}

// TestValidateSkipsOmittedOptionalParameters pins the `continue` branch that skips an
// absent, non-required parameter: only `needed` and `mode` are supplied, so the compiled
// output must contain exactly those two, in spec order, with nothing emitted for the
// omitted `count`, `label`, and `on`.
func TestValidateSkipsOmittedOptionalParameters(t *testing.T) {
	t.Parallel()
	result, err := TestRegistry.Validate(Applied{Name: "testall", Parameters: []AppliedParameter{
		{Name: "needed", Value: ident("1")},
		{Name: "mode", Value: ident("hash")},
	}}, SiteDefinition, allowAll, allowAll)
	require.NoError(t, err)

	params := result.GetParameters()
	require.Len(t, params, 2)
	require.Equal(t, "needed", params[0].GetName())
	require.Equal(t, "mode", params[1].GetName())
}
