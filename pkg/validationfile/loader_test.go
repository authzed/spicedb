package validationfile

import (
	"testing"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestPopulateFromFiles(t *testing.T) {
	tests := []struct {
		name      string
		filePaths []string
		want      []*core.RelationTuple
	}{
		{
			name:      "no comment",
			filePaths: []string{"testdata/loader_no_comment.yaml"},
			want: []*core.RelationTuple{
				tuple.Parse("example/project:pied_piper#owner@example/user:milburga"),
				tuple.Parse("example/project:pied_piper#reader@example/user:tarben"),
				tuple.Parse("example/project:pied_piper#writer@example/user:freyja"),
			},
		},
		{
			name:      "with comment",
			filePaths: []string{"testdata/loader_with_comment.yaml"},
			want: []*core.RelationTuple{
				tuple.Parse("example/project:pied_piper#owner@example/user:milburga"),
				tuple.Parse("example/project:pied_piper#reader@example/user:tarben"),
				tuple.Parse("example/project:pied_piper#writer@example/user:freyja"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ds, err := memdb.NewMemdbDatastore(0, 0, 0)
			require.NoError(err)

			parsed, _, err := PopulateFromFiles(ds, tt.filePaths)
			require.NoError(err)
			require.Equal(tt.want, parsed.Tuples)
		})
	}
}
