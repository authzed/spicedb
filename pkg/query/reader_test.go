package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/tuple"
)

// recursiveUsersetSchema is the canonical "directly cyclic" userset relation.
// The self-edge probe (SubjectExistsAsRelationship) only fires for schemas of
// this shape, which is why the missing query shape went unnoticed.
const recursiveUsersetSchema = `
definition user {}
definition group {
	relation member: user | group#member
}
`

// TestSubjectExistsAsRelationship_QueryShape verifies that the existence probe
// specifies a query shape. Without it, the probe panics with "query shape is
// unspecified" under the validating datastore that all testfixtures helpers wrap.
func TestSubjectExistsAsRelationship_QueryShape(t *testing.T) {
	require := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(
		t, rawDS, recursiveUsersetSchema,
		[]tuple.Relationship{
			tuple.MustParse("group:a#member@user:tom"),
			tuple.MustParse("group:b#member@group:a#member"),
		},
	)

	reader := NewQueryDatastoreReader(
		datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting),
	)

	// group:a appears as a subject of group:b#member, so the probe must find it.
	exists, err := reader.SubjectExistsAsRelationship(t.Context(), NewObject("group", "a"), "member")
	require.NoError(err)
	require.True(exists)

	// group:c never appears as a subject, so the probe must not find it.
	exists, err = reader.SubjectExistsAsRelationship(t.Context(), NewObject("group", "c"), "member")
	require.NoError(err)
	require.False(exists)
}
