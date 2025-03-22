package mysql

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
			name: "no index used",
			input: `{
				"query": "select * from something",
				"inputs": [
					{
					"covering": false,
					"table_name": "country",
					"access_type": "some_other_type",
					"estimated_rows": 17.0,
					"estimated_total_cost": 3.668778400708174
					}
				]
			}`,
			output: datastore.ParsedExplain{},
		},
		{
			name: "primary index used",
			input: `{
				"query": "select * from something",
				"inputs": [
					{
					"covering": false,
					"index_name": "PRIMARY",
					"table_name": "country",
					"access_type": "index",
					"estimated_rows": 17.0,
					"index_access_type": "index_range_scan",
					"estimated_total_cost": 3.668778400708174
					}
				]
			}`,
			output: datastore.ParsedExplain{
				IndexesUsed: []string{"PRIMARY"},
			},
		},
		{
			name: "index used",
			input: `{
				"query": "select * from something",
				"inputs": [
					{
					"covering": false,
					"index_name": "ix_some_index",
					"table_name": "country",
					"access_type": "index",
					"estimated_rows": 17.0,
					"index_access_type": "index_range_scan",
					"estimated_total_cost": 3.668778400708174
					}
				]
			}`,
			output: datastore.ParsedExplain{
				IndexesUsed: []string{"ix_some_index"},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := (&Datastore{}).ParseExplain(tc.input)

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
