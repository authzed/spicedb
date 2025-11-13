//go:build !skipintegrationtests

package integrationtesting_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/caveats"
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
		Context:      t.Context(),
		Executor:     query.LocalExecutor{},
		Reader:       q.ds.SnapshotReader(q.revision),
		CaveatRunner: caveats.NewCaveatRunner(caveattypes.Default.TypeSet),
		TraceLogger:  query.NewTraceLogger(), // Enable tracing for debugging
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
				t.Run(entry.name, func(t *testing.T) {
					for _, assertion := range entry.assertions {
						t.Run(assertion.RelationshipWithContextString, func(t *testing.T) {
							// Run both unoptimized and optimized versions
							for _, optimizationMode := range []struct {
								name     string
								optimize bool
							}{
								{"unoptimized", false},
								{"optimized", true},
							} {
								t.Run(optimizationMode.name, func(t *testing.T) {
									require := require.New(t)

									rel := assertion.Relationship
									it, err := query.BuildIteratorFromSchema(handle.schema, rel.Resource.ObjectType, rel.Resource.Relation)
									require.NoError(err)

									// Apply static optimizations if requested
									if optimizationMode.optimize {
										it, _, err = query.ApplyOptimizations(it, query.StaticOptimizations)
										require.NoError(err)
									}

									qctx := handle.buildContext(t)

									// Add caveat context from assertion if available
									if len(assertion.CaveatContext) > 0 {
										qctx.CaveatContext = assertion.CaveatContext
									}

									seq, err := qctx.Check(it, []query.Object{query.GetObject(rel.Resource)}, rel.Subject)
									require.NoError(err)

									rels, err := query.CollectAll(seq)
									require.NoError(err)

									// Print trace if test fails
									if qctx.TraceLogger != nil {
										defer func() {
											if t.Failed() {
												t.Logf("Trace for %s:\n%s", entry.name, qctx.TraceLogger.DumpTrace())
												// Also print the tree structure for debugging
												if it != nil {
													t.Logf("Tree structure:\n%s", it.Explain().String())
												}
											}
										}()
									}

									switch entry.expectedPermissionship {
									case v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION:
										require.Len(rels, 1)
										require.NotNil(rels[0].Caveat)
									case v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION:
										require.Len(rels, 1)
										require.Nil(rels[0].Caveat)
									case v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION:
										if len(rels) != 0 && qctx.TraceLogger != nil {
											t.Logf("Expected 0 relations but got %d. Trace:\n%s", len(rels), qctx.TraceLogger.DumpTrace())
										}
										require.Len(rels, 0)
									}
								})
							}
						})
					}
				})
			}
		}
	})
}
