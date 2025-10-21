package validationfile

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

type ExpectedError struct {
	message string
	source  string
	line    uint64
	column  uint64
}

func TestPopulateFromFiles(t *testing.T) {
	tests := []struct {
		name          string
		filePaths     []string
		want          []string
		expectedError *ExpectedError
	}{
		{
			name:      "no comment",
			filePaths: []string{"testdata/loader_no_comment.yaml"},
			want: []string{
				"example/project:pied_piper#owner@example/user:milburga",
				"example/project:pied_piper#reader@example/user:tarben",
				"example/project:pied_piper#writer@example/user:freyja",
			},
			expectedError: nil,
		},
		{
			name:      "using schemafile",
			filePaths: []string{"testdata/loader_using_schemafile.yaml"},
			want: []string{
				"example/project:pied_piper#owner@example/user:milburga",
				"example/project:pied_piper#reader@example/user:tarben",
				"example/project:pied_piper#writer@example/user:freyja",
			},
			expectedError: nil,
		},
		{
			name:      "with comment",
			filePaths: []string{"testdata/loader_with_comment.yaml"},
			want: []string{
				"example/project:pied_piper#owner@example/user:milburga",
				"example/project:pied_piper#reader@example/user:tarben",
				"example/project:pied_piper#writer@example/user:freyja",
			},
			expectedError: nil,
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
			expectedError: nil,
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
			expectedError: nil,
		},
		{
			name:          "missing schema",
			filePaths:     []string{"testdata/just_rels.yaml"},
			want:          nil,
			expectedError: &ExpectedError{message: "object definition `example/project` not found"},
		},
		{
			name:          "both schema and schemaFile",
			filePaths:     []string{"testdata/schema_and_schemafile.yaml"},
			want:          nil,
			expectedError: &ExpectedError{message: "only one of schema or schemaFile can be specified"},
		},
		{
			name:      "invalid schemaFile",
			filePaths: []string{"testdata/loader_using_invalid_schemafile.yaml"},
			want:      nil,
			expectedError: &ExpectedError{
				message: "error when parsing schema: Expected one of: [TokenTypeColon], found: TokenTypeIdentifier",
				source:  "example",
				line:    6,
				column:  18,
			},
		},
		{
			name:      "non-local schemaFile",
			filePaths: []string{"testdata/loader_using_non-local_schemafile.yaml"},
			want:      nil,
			expectedError: &ExpectedError{
				message: "schema file \"../schemas/non_local_schemafile.zed\" is not local",
			},
		},
		{
			name:      "missing schemaFile",
			filePaths: []string{"testdata/loader_using_missing_schemafile.yaml"},
			want:      nil,
			expectedError: &ExpectedError{
				message: "error when opening schema file testdata/non_existant_schemafile.zed: open testdata/non_existant_schemafile.zed: no such file or directory",
			},
		},
		{
			name:          "legacy file",
			filePaths:     []string{"testdata/legacy.yaml"},
			want:          nil,
			expectedError: &ExpectedError{message: "relationships must be specified in `relationships`"},
		},
		{
			name:      "basic caveats",
			filePaths: []string{"testdata/basic_caveats.yaml"},
			want: []string{
				"resource:first#reader@user:sarah[some_caveat:{\"somecondition\":42}]",
				"resource:first#reader@user:tom[some_caveat]",
			},
			expectedError: nil,
		},
		{
			name:      "caveat order",
			filePaths: []string{"testdata/caveat_order.yaml"},
			want: []string{
				"resource:first#reader@user:sarah[some_caveat:{\"somecondition\":42}]",
				"resource:first#reader@user:tom[some_caveat]",
			},
			expectedError: nil,
		},
		{
			name:          "invalid caveat",
			filePaths:     []string{"testdata/invalid_caveat.yaml"},
			want:          nil,
			expectedError: &ExpectedError{message: "could not lookup caveat `some_caveat` for relation `reader`: caveat with name `some_caveat` not found"},
		},
		{
			name:          "invalid caveated relationship",
			filePaths:     []string{"testdata/invalid_caveated_rel.yaml"},
			want:          nil,
			expectedError: &ExpectedError{message: "subjects of type `user with some_caveat` are not allowed on relation `resource#reader`"},
		},
		{
			name:          "invalid caveated relationship syntax",
			filePaths:     []string{"testdata/invalid_caveated_rel_syntax.yaml"},
			want:          nil,
			expectedError: &ExpectedError{message: "error parsing relationship"},
		},
		{
			name:          "repeated relationship",
			filePaths:     []string{"testdata/repeated_relationship.yaml"},
			want:          nil,
			expectedError: &ExpectedError{message: "found repeated relationship `resource:first#reader@user:tom`"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, 0)
			require.NoError(err)

			parsed, _, err := PopulateFromFiles(t.Context(), ds, caveattypes.Default.TypeSet, tt.filePaths)
			if tt.expectedError == nil {
				require.NoError(err)

				foundRelationships := make([]string, 0, len(parsed.Relationships))
				for _, rel := range parsed.Relationships {
					foundRelationships = append(foundRelationships, tuple.MustString(rel))
				}

				sort.Strings(tt.want)
				sort.Strings(foundRelationships)
				require.Equal(tt.want, foundRelationships)
			} else {
				require.Error(err)
				if tt.expectedError.message != "" {
					require.Contains(err.Error(), tt.expectedError.message)
				}

				var sourceError *spiceerrors.WithSourceError
				if errors.As(err, &sourceError) {
					if tt.expectedError.source != "" {
						require.Equal(sourceError.SourceCodeString, tt.expectedError.source)
					}

					if tt.expectedError.line > 0 {
						require.Equal(sourceError.LineNumber, tt.expectedError.line)
					}

					if tt.expectedError.column > 0 {
						require.Equal(sourceError.ColumnPosition, tt.expectedError.column)
					}
				}
			}
		})
	}
}

func TestPopulationChunking(t *testing.T) {
	require := require.New(t)

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, 0)
	require.NoError(err)

	cs := txCountingDatastore{delegate: ds}
	_, _, err = PopulateFromFiles(t.Context(), &cs, caveattypes.Default.TypeSet, []string{"testdata/requires_chunking.yaml"})
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
