package validationfile

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestPopulateFromFiles(t *testing.T) {
	tests := []struct {
		name          string
		filePaths     []string
		want          []string
		expectedError string
	}{
		{
			name:      "no comment",
			filePaths: []string{"testdata/loader_no_comment.yaml"},
			want: []string{
				"example/project:pied_piper#owner@example/user:milburga",
				"example/project:pied_piper#reader@example/user:tarben",
				"example/project:pied_piper#writer@example/user:freyja",
			},
			expectedError: "",
		},
		{
			name:      "with comment",
			filePaths: []string{"testdata/loader_with_comment.yaml"},
			want: []string{
				"example/project:pied_piper#owner@example/user:milburga",
				"example/project:pied_piper#reader@example/user:tarben",
				"example/project:pied_piper#writer@example/user:freyja",
			},
			expectedError: "",
		},
		{
			name:      "multiple files",
			filePaths: []string{"testdata/initial_schema_and_rels.yaml", "testdata/just_rels.yaml"},
			want: []string{
				"example/project:pied_piper#owner@example/user:milburga",
				"example/project:pied_piper#reader@example/user:tarben",
				"example/project:pied_piper#writer@example/user:freyja",
				"example/project:pied_piper#owner@example/user:fred",
				"example/project:pied_piper#reader@example/user:tom",
				"example/project:pied_piper#writer@example/user:sarah",
			},
			expectedError: "",
		},
		{
			name:      "multiple files",
			filePaths: []string{"testdata/initial_schema_and_rels.yaml", "testdata/just_rels.yaml"},
			want: []string{
				"example/project:pied_piper#owner@example/user:milburga",
				"example/project:pied_piper#reader@example/user:tarben",
				"example/project:pied_piper#writer@example/user:freyja",
				"example/project:pied_piper#owner@example/user:fred",
				"example/project:pied_piper#reader@example/user:tom",
				"example/project:pied_piper#writer@example/user:sarah",
			},
			expectedError: "",
		},
		{
			name:          "missing schema",
			filePaths:     []string{"testdata/just_rels.yaml"},
			want:          nil,
			expectedError: "object definition `example/project` not found",
		},
		{
			name:          "legacy file",
			filePaths:     []string{"testdata/legacy.yaml"},
			want:          nil,
			expectedError: "relationships must be specified in `relationships`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ds, err := memdb.NewMemdbDatastore(0, 0, 0)
			require.NoError(err)

			parsed, _, err := PopulateFromFiles(ds, tt.filePaths)
			if tt.expectedError == "" {
				require.NoError(err)

				foundRelationships := make([]string, 0, len(parsed.Tuples))
				for _, tpl := range parsed.Tuples {
					foundRelationships = append(foundRelationships, tuple.String(tpl))
				}

				sort.Strings(tt.want)
				sort.Strings(foundRelationships)
				require.Equal(tt.want, foundRelationships)
			} else {
				require.NotNil(err)
				require.Contains(err.Error(), tt.expectedError)
			}
		})
	}
}
