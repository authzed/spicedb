package crdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

func TestParseExplain(t *testing.T) {
	tcs := []struct {
		name   string
		input  string
		output datastore.ParsedExplain
	}{
		{
			name:   "empty",
			input:  "",
			output: datastore.ParsedExplain{},
		},
		{
			name: "no indexes used",
			input: `  • sort
  │ estimated row count: 12,385
  │ order: +revenue
  │
  └── • filter
      │ estimated row count: 12,385
      │ filter: revenue > 90
      │
      └── • scan
            estimated row count: 125,000 (100% of the table; stats collected 19 minutes ago)
            table: relation_tuple`,
			output: datastore.ParsedExplain{},
		},
		{
			name: "index used",
			input: `  • sort
  │ estimated row count: 12,385
  │ order: +revenue
  │
  └── • filter
      │ estimated row count: 12,385
      │ filter: revenue > 90
      │
      └── • scan
            estimated row count: 125,000 (100% of the table; stats collected 19 minutes ago)
            table: relation_tuple@idx_relation_tuple_namespace`,
			output: datastore.ParsedExplain{
				IndexesUsed: []string{"idx_relation_tuple_namespace"},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := (&crdbDatastore{}).ParseExplain(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.output, parsed)
		})
	}
}
