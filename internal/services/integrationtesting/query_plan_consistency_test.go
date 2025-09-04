//go:build !skipintegrationtests
// +build !skipintegrationtests

package integrationtesting_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/services/integrationtesting/consistencytestutil"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/validationfile"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

func TestQueryPlanConsistency(t *testing.T) {
	t.Parallel()
	if os.Getenv("TEST_QUERY_PLAN") == "" {
		t.Skip("Skipping incomplete query plan consistency while it is a work in progress")
		return
	}
	consistencyTestFiles, err := consistencytestutil.ListTestConfigs()
	require.NoError(t, err)
	for _, filePath := range consistencyTestFiles {
		filePath := filePath

		t.Run(filepath.Base(filePath), func(t *testing.T) {
			t.Parallel()
			runQueryPlanConsistencyForFile(t, filePath)
		})
	}
}

type queryPlanConsistencyHandle struct {
	populated *validationfile.PopulatedValidationFile
	schema    *schema.Schema
	revision  datastore.Revision
	ds        datastore.Datastore
}

func (q *queryPlanConsistencyHandle) buildContext(t *testing.T) *query.Context {
	return &query.Context{
		Context:   t.Context(),
		Executor:  query.LocalExecutor{},
		Datastore: q.ds,
		Revision:  q.revision,
	}
}

func runQueryPlanConsistencyForFile(t *testing.T, filePath string) {
	require := require.New(t)

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, testTimedelta, memdb.DisableGC)
	require.NoError(err)
	populated, _, err := validationfile.PopulateFromFiles(t.Context(), ds, caveattypes.Default.TypeSet, []string{filePath})
	require.NoError(err)

	headRevision, err := ds.HeadRevision(t.Context())
	require.NoError(err)

	schemaView, err := schema.BuildSchemaFromDefinitions(populated.NamespaceDefinitions, populated.CaveatDefinitions)
	require.NoError(err)

	handle := &queryPlanConsistencyHandle{
		populated: populated,
		schema:    schemaView,
		revision:  headRevision,
		ds:        ds,
	}
	runQueryPlanAssertions(t, handle)
}

func runQueryPlanAssertions(t *testing.T, handle *queryPlanConsistencyHandle) {
	t.Run("assertions", func(t *testing.T) {
		for _, parsedFile := range handle.populated.ParsedFiles {
			for _, entry := range []struct {
				name                   string
				assertions             []blocks.Assertion
				expectedPermissionship v1.CheckPermissionResponse_Permissionship
			}{
				{
					"true",
					parsedFile.Assertions.AssertTrue,
					v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					"caveated",
					parsedFile.Assertions.AssertCaveated,
					v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
				},
				{
					"false",
					parsedFile.Assertions.AssertFalse,
					v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
				},
			} {
				entry := entry
				t.Run(entry.name, func(t *testing.T) {
					for _, assertion := range entry.assertions {
						assertion := assertion
						t.Run(assertion.RelationshipWithContextString, func(t *testing.T) {
							require := require.New(t)

							rel := assertion.Relationship
							it, err := query.BuildIteratorFromSchema(handle.schema, rel.Resource.ObjectType, rel.Resource.Relation)
							require.NoError(err)

							t.Log(it.Explain())

							qctx := handle.buildContext(t)

							seq, err := qctx.Check(it, []query.Object{query.GetObject(rel.Resource)}, rel.Subject)
							require.NoError(err)

							rels, err := query.CollectAll(seq)
							require.NoError(err)

							switch entry.expectedPermissionship {
							case v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION:
								require.Equal(len(rels), 1)
								require.NotNil(rels[0].OptionalCaveat)
							case v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION:
								require.Equal(len(rels), 1)
								require.Nil(rels[0].OptionalCaveat)
							case v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION:
								require.Equal(len(rels), 0)
							}
						})
					}
				})
			}
		}
	})
}
