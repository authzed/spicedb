package checkbaseline

import (
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGeneratedExpectations(t *testing.T) {
	datasets := GeneratedDatasets([]Scale{{Name: "small", Fanout: 3, Depth: 3, DirectRelationships: 10}})
	require.NotEmpty(t, datasets)
	for _, d := range datasets {
		t.Run(d.ID, func(t *testing.T) {
			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(t, err)
			defer ds.Close()
			cases, err := d.Setup(t.Context(), ds)
			require.NoError(t, err)
			require.NotEmpty(t, cases)
			seen := map[string]bool{}
			for _, c := range cases {
				require.False(t, seen[c.ID])
				seen[c.ID] = true
				require.NotEmpty(t, c.Expected.Outcome)
				require.NotEmpty(t, c.Query.SubjectRelation)
			}
		})
	}
}
func TestRegistryCatalog(t *testing.T) {
	ds := RegistryDatasets()
	require.GreaterOrEqual(t, len(ds), 8)
	for i := 1; i < len(ds); i++ {
		require.Less(t, ds[i-1].ID, ds[i].ID)
	}
}
