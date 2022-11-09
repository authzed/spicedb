package validationfile

import (
	"sort"
	"strings"
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
		{
			name:      "multiple files",
			filePaths: []string{"testdata/initial_schema_and_rels.yaml", "testdata/just_rels.yaml"},
			want: []*core.RelationTuple{
				tuple.Parse("example/project:pied_piper#owner@example/user:milburga"),
				tuple.Parse("example/project:pied_piper#reader@example/user:tarben"),
				tuple.Parse("example/project:pied_piper#writer@example/user:freyja"),
				tuple.Parse("example/project:pied_piper#owner@example/user:fred"),
				tuple.Parse("example/project:pied_piper#reader@example/user:tom"),
				tuple.Parse("example/project:pied_piper#writer@example/user:sarah"),
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

			sort.Sort(sortByTuple(tt.want))
			sort.Sort(sortByTuple(parsed.Tuples))

			require.Equal(tt.want, parsed.Tuples)
		})
	}
}

type sortByTuple []*core.RelationTuple

func (a sortByTuple) Len() int      { return len(a) }
func (a sortByTuple) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortByTuple) Less(i, j int) bool {
	return strings.Compare(tuple.String(a[i]), tuple.String(a[j])) < 0
}
