package oracle_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/services/integrationtesting/oracle"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

const exclusionSchema = `
schema: |+
  definition user {}

  definition document {
    relation parent: document
    relation viewer: user
    permission view = viewer - parent->view
  }
`

// TestRecursionThroughExclusion covers schemas that recurse through an exclusion, where the
// answer depends on the shape of the data.
func TestRecursionThroughExclusion(t *testing.T) {
	clingo := oracle.ClingoFromEnv()
	if clingo == nil {
		t.Skip("clingo not found; install it or set SPICEDB_CLINGO")
	}

	tcs := []struct {
		name          string
		relationships string
		models        int // zero means exactly one model, with view as expected below
		view          map[string]oracle.Membership
	}{
		{
			name: "acyclic",
			relationships: `
  document:a#viewer@user:tom
  document:b#viewer@user:tom
  document:b#parent@document:a
  document:c#viewer@user:tom
  document:c#parent@document:b`,
			// a grants tom; b excludes whoever a grants; c's parent b does not grant tom.
			view: map[string]oracle.Membership{"a": oracle.Member, "b": oracle.NotMember, "c": oracle.Member},
		},
		{
			name: "self-cycle",
			relationships: `
  document:a#viewer@user:tom
  document:a#parent@document:a`,
			// tom can view a exactly when tom cannot view a.
			models: 0,
		},
		{
			name: "two-cycle",
			relationships: `
  document:a#viewer@user:tom
  document:b#viewer@user:tom
  document:a#parent@document:b
  document:b#parent@document:a`,
			// Either a or b grants tom, but not both, and nothing says which.
			models: 2,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, time.Second, memdb.DisableGC)
			require.NoError(t, err)
			dl := datalayer.NewDataLayer(ds)
			ctx := datalayer.ContextWithHandle(t.Context())
			require.NoError(t, datalayer.SetInContext(ctx, dl))

			contents := exclusionSchema + "relationships: |" + tc.relationships + "\n"
			populated, _, err := validationfile.PopulateFromFilesContents(ctx, dl, caveattypes.Default.TypeSet, map[string][]byte{"file.yaml": []byte(contents)})
			require.NoError(t, err)

			result, err := oracle.Compute(ctx, clingo, populated, time.Now())
			if tc.view == nil {
				var noUnique oracle.ErrNoUniqueModel
				require.ErrorAs(t, err, &noUnique)
				require.Equal(t, tc.models, noUnique.Models)
				return
			}
			require.False(t, errors.As(err, new(oracle.ErrNoUniqueModel)))
			require.NoError(t, err)

			tom := tuple.ONR("user", "tom", tuple.Ellipsis)
			for id, expected := range tc.view {
				require.Equal(t, expected, result.Membership(tuple.ONR("document", id, "view"), tom), id)
			}
		})
	}
}
