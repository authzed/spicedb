package validationfile

import (
	"context"
	"sort"
	"testing"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/stretchr/testify/require"
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
		{
			name:      "basic caveats",
			filePaths: []string{"testdata/basic_caveats.yaml"},
			want: []string{
				"resource:first#reader@user:sarah[some_caveat:{\"somecondition\":42}]",
				"resource:first#reader@user:tom[some_caveat]",
			},
			expectedError: "",
		},
		{
			name:      "caveat order",
			filePaths: []string{"testdata/caveat_order.yaml"},
			want: []string{
				"resource:first#reader@user:sarah[some_caveat:{\"somecondition\":42}]",
				"resource:first#reader@user:tom[some_caveat]",
			},
			expectedError: "",
		},
		{
			name:          "invalid caveat",
			filePaths:     []string{"testdata/invalid_caveat.yaml"},
			want:          nil,
			expectedError: "could not lookup caveat `some_caveat` for relation `reader`: caveat with name `some_caveat` not found",
		},
		{
			name:          "invalid caveated relationship",
			filePaths:     []string{"testdata/invalid_caveated_rel.yaml"},
			want:          nil,
			expectedError: "subjects of type `user with some_caveat` are not allowed on relation `resource#reader`",
		},
		{
			name:          "invalid caveated relationship syntax",
			filePaths:     []string{"testdata/invalid_caveated_rel_syntax.yaml"},
			want:          nil,
			expectedError: "error parsing relationship",
		},
		{
			name:          "repeated relationship",
			filePaths:     []string{"testdata/repeated_relationship.yaml"},
			want:          nil,
			expectedError: "found repeated relationship `resource:first#reader@user:tom`",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, 0)
			require.NoError(err)

			parsed, _, err := PopulateFromFiles(context.Background(), ds, tt.filePaths)
			if tt.expectedError == "" {
				require.NoError(err)

				foundRelationships := make([]string, 0, len(parsed.Relationships))
				for _, rel := range parsed.Relationships {
					foundRelationships = append(foundRelationships, tuple.MustString(rel))
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

func TestPopulationChunking(t *testing.T) {
	require := require.New(t)

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, 0)
	require.NoError(err)

	cs := txCountingDatastore{delegate: ds}
	_, _, err = PopulateFromFiles(context.Background(), &cs, []string{"testdata/requires_chunking.yaml"})
	require.NoError(err)
	require.Equal(3, cs.count)
}

type txCountingDatastore struct {
	proxy_test.MockDatastore
	count    int
	delegate datastore.Datastore
}

func (c *txCountingDatastore) ReadWriteTx(ctx context.Context, userFunc datastore.TxUserFunc, option ...options.RWTOptionsOption) (datastore.Revision, error) {
	c.count++
	return c.delegate.ReadWriteTx(ctx, userFunc, option...)
}
