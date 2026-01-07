package tables

import (
	"testing"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/stretchr/testify/require"
)

func TestConsistencyFromFields(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                    string
		fields                  fieldMap[string]
		parameters              []wire.Parameter
		expectedConsistencyType string // Type of consistency requirement
		expectedToken           string // Expected token value for token-based consistency
		expectedError           string
	}{
		{
			name:                    "default - no consistency field",
			fields:                  fieldMap[string]{},
			expectedConsistencyType: "MinimizeLatency",
		},
		{
			name: "explicit minimize_latency",
			fields: fieldMap[string]{
				"consistency": {value: "minimize_latency"},
			},
			expectedConsistencyType: "MinimizeLatency",
		},
		{
			name: "fully_consistent",
			fields: fieldMap[string]{
				"consistency": {value: "fully_consistent"},
			},
			expectedConsistencyType: "FullyConsistent",
		},
		{
			name: "at_least_as_fresh with token",
			fields: fieldMap[string]{
				"consistency": {value: "GhUKEzE3MzU2OTQzMDkwMDAwMDAwMDA="},
			},
			expectedConsistencyType: "AtLeastAsFresh",
			expectedToken:           "GhUKEzE3MzU2OTQzMDkwMDAwMDAwMDA=",
		},
		{
			name: "at_exact_snapshot with @ prefix",
			fields: fieldMap[string]{
				"consistency": {value: "@GhUKEzE3MzU2OTQzMDkwMDAwMDAwMDA="},
			},
			expectedConsistencyType: "AtExactSnapshot",
			expectedToken:           "GhUKEzE3MzU2OTQzMDkwMDAwMDAwMDA=",
		},
		{
			name: "consistency from parameter",
			fields: fieldMap[string]{
				"consistency": {parameterIndex: 1},
			},
			parameters:              []wire.Parameter{mockParameter("param0"), mockParameter("fully_consistent")},
			expectedConsistencyType: "FullyConsistent",
		},
		{
			name: "at_least_as_fresh from parameter",
			fields: fieldMap[string]{
				"consistency": {parameterIndex: 1},
			},
			parameters:              []wire.Parameter{mockParameter("param0"), mockParameter("SomeZedToken123")},
			expectedConsistencyType: "AtLeastAsFresh",
			expectedToken:           "SomeZedToken123",
		},
		{
			name: "at_exact_snapshot from parameter",
			fields: fieldMap[string]{
				"consistency": {parameterIndex: 1},
			},
			parameters:              []wire.Parameter{mockParameter("param0"), mockParameter("@SomeZedToken456")},
			expectedConsistencyType: "AtExactSnapshot",
			expectedToken:           "SomeZedToken456",
		},
		{
			name: "empty string defaults to minimize_latency token",
			fields: fieldMap[string]{
				"consistency": {value: ""},
			},
			expectedConsistencyType: "AtLeastAsFresh",
			expectedToken:           "",
		},
		{
			name: "parameter index out of range",
			fields: fieldMap[string]{
				"consistency": {parameterIndex: 10},
			},
			parameters:    []wire.Parameter{mockParameter("param0")},
			expectedError: "parameter index out of range",
		},
		{
			name: "subquery placeholder not supported",
			fields: fieldMap[string]{
				"consistency": {isSubQueryPlaceholder: true},
			},
			expectedError: "subquery placeholders are not supported in permissions table",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			consistency, err := consistencyFromFields(tc.fields, tc.parameters)

			if tc.expectedError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, consistency)

			// Verify the consistency type
			switch tc.expectedConsistencyType {
			case "MinimizeLatency":
				require.NotNil(t, consistency.GetMinimizeLatency())
				require.True(t, consistency.GetMinimizeLatency())

			case "FullyConsistent":
				require.NotNil(t, consistency.GetFullyConsistent())
				require.True(t, consistency.GetFullyConsistent())

			case "AtLeastAsFresh":
				token := consistency.GetAtLeastAsFresh()
				require.NotNil(t, token)
				require.Equal(t, tc.expectedToken, token.Token)

			case "AtExactSnapshot":
				token := consistency.GetAtExactSnapshot()
				require.NotNil(t, token)
				require.Equal(t, tc.expectedToken, token.Token)

			default:
				t.Fatalf("Unknown expected consistency type: %s", tc.expectedConsistencyType)
			}
		})
	}
}

func TestConsistencyConstants(t *testing.T) {
	t.Parallel()

	// Verify constants are defined correctly
	require.Equal(t, "minimize_latency", consistencyMinimizeLatency)
	require.Equal(t, "fully_consistent", consistencyFullyConsistent)
	require.Equal(t, "@", consistencyExactPrefix)
}

func TestConsistencyField(t *testing.T) {
	t.Parallel()

	// Verify consistencyField is properly configured
	require.Equal(t, "consistency", consistencyField.Name)
	require.NotZero(t, consistencyField.Oid)
	require.Equal(t, int16(256), consistencyField.Width)
}
