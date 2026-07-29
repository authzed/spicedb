//go:build integration || datastoreconsistency

package integrationtesting_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/services/integrationtesting/consistencytestutil"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/development"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

const testTimedelta = 1 * time.Second

// TestConsistencyWithSelfKeyword exercises the `self` keyword schema, which
// cannot flow through the generic consistency suite (TestConsistencyPerDatastore)
// because the integration test harness can't deal with logic that doesn't have a
// concrete relation associated with it as of time of writing. It runs against the
// in-memory datastore only, as the behavior under test is engine-independent.
// TODO: remove this test and rename self.yaml.skip to self.yaml
func TestConsistencyWithSelfKeyword(t *testing.T) {
	options := []server.ConfigOption{
		server.WithDispatchChunkSize(5),
		server.WithEnableExperimentalLookupResources(true),
		server.WithExperimentalLookupResourcesVersion("lr3"),
	}

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, testTimedelta, memdb.DisableGC)
	require.NoError(t, err)

	dl := datalayer.NewDataLayer(ds)
	dsCtx := datalayer.ContextWithHandle(t.Context())
	require.NoError(t, datalayer.SetInContext(dsCtx, dl))

	populated, _, err := validationfile.PopulateFromFiles(t.Context(), dl, types.Default.TypeSet, []string{"testconfigs/self.yaml.skip"})
	require.NoError(t, err)

	connections := testserver.TestClusterWithDispatch(t, 1, ds, options...)

	cad := consistencytestutil.ConsistencyClusterAndData{
		Conn:      connections[0],
		DataStore: ds,
		Ctx:       dsCtx,
		Populated: populated,
	}

	// Validate the type system for each namespace.
	headRevisionResult, err := ds.HeadRevision(cad.Ctx)
	require.NoError(t, err)
	headRevision := headRevisionResult.Revision

	ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(ds.SnapshotReader(headRevision)))

	for _, nsDef := range cad.Populated.NamespaceDefinitions {
		_, err := ts.GetValidatedDefinition(cad.Ctx, nsDef.Name)
		require.NoError(t, err)

		_, err = development.AddDefinitionWarnings(cad.Ctx, nsDef, ts)
		// We would normally check to see if there were any warnings; however, some of the integration tests are
		// here to intentionally test things that are warnings-but-not-errors in a schema (like arrow-references-relation).
		// So instead, just validate that warning validation succeeds.
		require.NoError(t, err)
	}

	tester := consistencytestutil.NewServiceTester(cad.Conn)
	accessibilitySet := consistencytestutil.BuildAccessibilitySet(t, cad.Ctx, cad.Populated, cad.DataStore)

	dispatcher := consistencytestutil.CreateDispatcherForTesting(t, false)

	vctx := consistencytestutil.ValidationContext{
		ClusterAndData:   cad,
		AccessibilitySet: accessibilitySet,
		ServiceTester:    tester,
		Revision:         headRevision,
		Dispatcher:       dispatcher,
	}

	// Call a write on each relationship to make sure it type checks.
	consistencytestutil.EnsureRelationshipWrites(t, vctx)

	// Call a read on each relationship resource type and ensure it finds all expected relationships.
	consistencytestutil.ValidateRelationshipReads(t, vctx)

	// Run the assertions defined in the file.
	consistencytestutil.RunAssertions(t, vctx)

	// Run basic expansion on each relation and ensure no errors are raised.
	consistencytestutil.EnsureNoExpansionErrors(t, vctx)

	// We skip validating expansion subjects for this case

	// Validate lookup resources manually for this schema

	// Look for resources for alice
	foundResources, _, err := vctx.ServiceTester.LookupResources(
		t.Context(),
		tuple.RelationReference{
			ObjectType: "user",
			Relation:   "me_or_related",
		},
		tuple.ObjectAndRelation{
			ObjectType: "user",
			ObjectID:   "alice",
			Relation:   tuple.Ellipsis,
		},
		vctx.Revision,
		nil,
		10,
		nil,
	)
	require.NoError(t, err)
	resourceIds := make([]string, 0, len(foundResources))
	for _, resource := range foundResources {
		resourceIds = append(resourceIds, resource.ResourceObjectId)
	}

	// We expect that alice is related to himself and bob
	require.ElementsMatch(t, []string{"alice", "bob"}, resourceIds, "expected both bob and alice as resources")

	// Look for resources for alice
	foundResources, _, err = vctx.ServiceTester.LookupResources(
		t.Context(),
		tuple.RelationReference{
			ObjectType: "user",
			Relation:   "me_or_related",
		},
		tuple.ObjectAndRelation{
			ObjectType: "user",
			ObjectID:   "bob",
			Relation:   tuple.Ellipsis,
		},
		vctx.Revision,
		nil,
		10,
		nil,
	)
	require.NoError(t, err)
	resourceIds = make([]string, 0, len(foundResources))
	for _, resource := range foundResources {
		resourceIds = append(resourceIds, resource.ResourceObjectId)
	}

	// We expect that bob is related only to himself
	require.ElementsMatch(t, []string{"bob"}, resourceIds, "expected just bob as resource")

	// Validate lookup subjects manually for this schema
	// Look for subjects for alice
	foundSubjects, err := vctx.ServiceTester.LookupSubjects(
		t.Context(),
		tuple.ObjectAndRelation{
			ObjectType: "user",
			ObjectID:   "alice",
			Relation:   "me_or_related",
		},
		tuple.RelationReference{
			ObjectType: "user",
		},
		vctx.Revision,
		nil,
	)
	require.NoError(t, err)
	subjectIds := make([]string, 0, len(foundSubjects))
	for _, subject := range foundSubjects {
		subjectIds = append(subjectIds, subject.Subject.SubjectObjectId)
	}

	// We expect that alice is related to himself
	require.ElementsMatch(t, []string{"alice"}, subjectIds, "expected just alice as subject")

	// Look for subjects for bob
	foundSubjects, err = vctx.ServiceTester.LookupSubjects(
		t.Context(),
		tuple.ObjectAndRelation{
			ObjectType: "user",
			ObjectID:   "bob",
			Relation:   "me_or_related",
		},
		tuple.RelationReference{
			ObjectType: "user",
		},
		vctx.Revision,
		nil,
	)
	require.NoError(t, err)
	subjectIds = make([]string, 0, len(foundSubjects))
	for _, subject := range foundSubjects {
		subjectIds = append(subjectIds, subject.Subject.SubjectObjectId)
	}

	// We expect that bob is related to himself and to alice
	require.ElementsMatch(t, []string{"bob", "alice"}, subjectIds, "expected bob and alice as subjects")

	// We skip validating the development expected relations for this schema

	// Ensure that the set of reachable subject types matches the actual reachable subjects.
	consistencytestutil.ValidateReachableSubjectTypes(t, vctx)
}
