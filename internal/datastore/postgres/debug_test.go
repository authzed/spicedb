package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

func TestParseExplain(t *testing.T) {
	tcs := []struct {
		name          string
		input         string
		output        datastore.ParsedExplain
		expectedError string
	}{
		{
			name:          "empty",
			input:         "",
			expectedError: "could not parse explain",
		},
		{
			name: "no indexes used",
			input: `[
				{
					"Plan": {
						"Node Type": "Seq Scan",
						"Relation Name": "relation_tuple"
					}
				}
			]`,
			output: datastore.ParsedExplain{},
		},
		{
			name: "index used",
			input: `[
				{
					"Plan": {
						"Node Type": "Seq Scan",
						"Relation Name": "relation_tuple",
						"Index Name": "idx_relation_tuple_namespace"
					}
				},
				{
					"Plan": {
						"Node Type": "Index Scan",
						"Relation Name": "relation_tuple"
					}
				}
			]`,
			output: datastore.ParsedExplain{
				IndexesUsed: []string{"idx_relation_tuple_namespace"},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := (&pgDatastore{}).ParseExplain(tc.input)

			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.output, parsed)
		})
	}
}
