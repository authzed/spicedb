package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/tuple"
)

// GCRunInterval is how often the background GC worker wakes up to delete data
// that has aged out of the retention window.
//
// It affects disk usage only: it never changes which revisions are readable.
// A datastore whose worker never ticks still rejects revisions older than its
// GCRetentionWindow, because validity is computed from the window and the
// current clock rather than from what has physically been deleted.
//
// It is a defined type rather than a plain time.Duration so that it cannot be
// passed where a GCRetentionWindow is expected.
type GCRunInterval time.Duration

// GCRetentionWindow is how far into the past revisions remain readable.
//
// Revisions older than it are rejected as stale, whether or not their data has
// actually been deleted yet. This is the knob that governs revision validity.
//
// It is a defined type rather than a plain time.Duration so that it cannot be
// passed where a GCRunInterval is expected.
type GCRetentionWindow time.Duration

const (
	// RetainAllRevisions is a retention window large enough that every revision
	// written during a test stays readable for the whole test.
	RetainAllRevisions GCRetentionWindow = GCRetentionWindow(90000 * time.Second)

	// DisableBackgroundGC is a run interval large enough that the background GC
	// worker never ticks during a test. It does not extend revision validity;
	// it only prevents aged-out data from being deleted at an arbitrary moment.
	// Tests that need collection to happen call datastore.RunGarbageCollection
	// directly, at a deterministic point.
	DisableBackgroundGC GCRunInterval = GCRunInterval(90000 * time.Second)
)

// RevisionParameters configures the revision-related behavior of a datastore
// under test. Build one with DefaultRevisionParameters and override only what
// the test actually exercises.
type RevisionParameters struct {
	// Quantization is the boundary interval to which revisions are rounded.
	// Zero disables quantization.
	Quantization time.Duration

	// GCRunInterval is how often the background GC worker runs.
	GCRunInterval GCRunInterval

	// GCRetentionWindow is how far into the past revisions remain readable.
	GCRetentionWindow GCRetentionWindow
}

// DefaultRevisionParameters returns the parameters appropriate for a test that
// is not itself about revisions or garbage collection: quantization off, the
// background GC worker disabled, and every revision retained.
//
// This keeps unrelated tests insulated from both sources of nondeterminism — a
// GC pass firing mid-test, and a revision going stale underneath an assertion.
func DefaultRevisionParameters() RevisionParameters {
	return RevisionParameters{
		Quantization:      0,
		GCRunInterval:     DisableBackgroundGC,
		GCRetentionWindow: RetainAllRevisions,
	}
}

// WithQuantization returns a copy with the revision quantization set.
func (rp RevisionParameters) WithQuantization(quantization time.Duration) RevisionParameters {
	rp.Quantization = quantization
	return rp
}

// WithGCRunInterval returns a copy with the background GC run interval set.
// Use this only when the test asserts that the worker actually ran.
func (rp RevisionParameters) WithGCRunInterval(interval GCRunInterval) RevisionParameters {
	rp.GCRunInterval = interval
	return rp
}

// WithGCRetentionWindow returns a copy with the retention window set.
// Use this when the test needs revisions to go stale within its lifetime.
func (rp RevisionParameters) WithGCRetentionWindow(window GCRetentionWindow) RevisionParameters {
	rp.GCRetentionWindow = window
	return rp
}

// DatastoreTester provides a generic datastore suite a means of initializing
// a particular datastore.
//
// Each datastore package implements it exactly once, as a named struct whose
// fields spell out the variations that package tests (integrity on or off, a
// migration phase, whether the background GC worker runs, and so on). Read
// that one implementation to know how the suite's datastores are built for
// that engine.
type DatastoreTester interface {
	// New creates a new datastore instance for a single test.
	New(tb testing.TB, revisionParameters RevisionParameters, watchBufferLength uint16) (datastore.Datastore, error)
}

type Categories map[string]struct{}

func (c Categories) GC() bool {
	_, ok := c[GCCategory]
	return ok
}

func (c Categories) Stats() bool {
	_, ok := c[StatsCategory]
	return ok
}

func (c Categories) Watch() bool {
	_, ok := c[WatchCategory]
	return ok
}

func (c Categories) Transaction() bool {
	_, ok := c[TransactionCategory]
	return ok
}

func (c Categories) WatchSchema() bool {
	_, ok := c[WatchSchemaCategory]
	return ok
}

func (c Categories) WatchCheckpoints() bool {
	_, ok := c[WatchCheckpointsCategory]
	return ok
}

func (c Categories) ConcurrentWrite() bool {
	_, ok := c[ConcurrentWriteCategory]
	return ok
}

func (c Categories) Migration() bool {
	_, ok := c[MigrationCategory]
	return ok
}

var noException = Categories{}

const (
	GCCategory               = "GC"
	WatchCategory            = "Watch"
	WatchSchemaCategory      = "WatchSchema"
	WatchCheckpointsCategory = "WatchCheckpoints"
	StatsCategory            = "Stats"
	TransactionCategory      = "Transaction"
	// ConcurrentWriteCategory marks tests that open two write transactions concurrently.
	// Datastores with a global write lock (e.g. memdb) must exclude this category.
	ConcurrentWriteCategory = "ConcurrentWrite"
	// MigrationCategory marks tests that check that database migrations apply
	// successfully and preserve the data written before them.
	MigrationCategory = "Migration"
)

func WithCategories(cats ...string) Categories {
	c := Categories{}
	for _, cat := range cats {
		c[cat] = struct{}{}
	}
	return c
}

func serial(tester DatastoreTester, tt func(t *testing.T, tester DatastoreTester)) func(t *testing.T) {
	return func(t *testing.T) {
		tt(t, tester)
	}
}

// AllWithExceptions runs all generic datastore tests on a DatastoreTester, except
// those specified test categories.
//
// retryErr must be an error the engine under test classifies as retryable, so
// that RetryTest can drive the engine's retry loop from inside a transaction.
func AllWithExceptions(t *testing.T, tester DatastoreTester, except Categories, retryErr error) {
	runner := serial

	t.Run("TestUniqueID", func(t *testing.T) { runner(tester, UniqueIDTest) })
	t.Run("TestUseAfterClose", runner(tester, UseAfterCloseTest))
	t.Run("TestReadyStateWhenPaused", runner(tester, ReadyStateWhenPausedTest))

	t.Run("TestNamespaceNotFound", runner(tester, NamespaceNotFoundTest))
	t.Run("TestNamespaceWrite", runner(tester, NamespaceWriteTest))
	t.Run("TestNamespaceDelete", runner(tester, NamespaceDeleteTest))
	t.Run("TestNamespaceDeleteNoRelationships", runner(tester, NamespaceDeleteNoRelationshipsTest))
	t.Run("TestNamespaceMultiDelete", runner(tester, NamespaceMultiDeleteTest))
	t.Run("TestEmptyNamespaceDelete", runner(tester, EmptyNamespaceDeleteTest))
	t.Run("TestStableNamespaceReadWrite", runner(tester, StableNamespaceReadWriteTest))
	t.Run("TestNamespaceDeleteInvalidNamespace", runner(tester, NamespaceDeleteInvalidNamespaceTest))

	t.Run("TestSimple", runner(tester, SimpleTest))
	t.Run("TestObjectIDs", runner(tester, ObjectIDsTest))
	t.Run("TestCaseSensitivity", runner(tester, CaseSensitivityTest))
	t.Run("TestDeleteRelationships", runner(tester, DeleteRelationshipsTest))
	t.Run("TestDeleteNonExistant", runner(tester, DeleteNotExistantTest))
	t.Run("TestDeleteAlreadyDeleted", runner(tester, DeleteAlreadyDeletedTest))
	t.Run("TestRecreateRelationshipsAfterDeleteWithFilter", runner(tester, RecreateRelationshipsAfterDeleteWithFilter))
	t.Run("TestWriteDeleteWrite", runner(tester, WriteDeleteWriteTest))
	t.Run("TestCreateAlreadyExisting", runner(tester, CreateAlreadyExistingTest))
	t.Run("TestTouchAlreadyExistingWithoutCaveat", runner(tester, TouchAlreadyExistingTest))
	t.Run("TestCreateDeleteTouch", runner(tester, CreateDeleteTouchTest))
	t.Run("TestDeleteOneThousandIndividualInOneCall", runner(tester, DeleteOneThousandIndividualInOneCallTest))
	t.Run("TestCreateTouchDeleteTouch", runner(tester, CreateTouchDeleteTouchTest))
	t.Run("TestTouchAlreadyExistingCaveated", runner(tester, TouchAlreadyExistingCaveatedTest))
	t.Run("TestBulkDeleteRelationships", runner(tester, BulkDeleteRelationshipsTest))
	t.Run("TestDeleteCaveatedTuple", runner(tester, DeleteCaveatedTupleTest))
	t.Run("TestDeleteWithLimit", runner(tester, DeleteWithLimitTest))
	t.Run("TestDeleteWithInvalidPrefix", runner(tester, DeleteWithInvalidPrefixTest))
	t.Run("TestDeleteWithPrefix", runner(tester, DeleteWithPrefixTest))
	t.Run("TestQueryRelationshipsWithVariousFilters", runner(tester, QueryRelationshipsWithVariousFiltersTest))
	t.Run("TestDeleteRelationshipsWithVariousFilters", runner(tester, DeleteRelationshipsWithVariousFiltersTest))
	t.Run("TestTouchTypedAlreadyExistingWithoutCaveat", runner(tester, TypedTouchAlreadyExistingTest))
	t.Run("TestTouchTypedAlreadyExistingWithCaveat", runner(tester, TypedTouchAlreadyExistingWithCaveatTest))
	t.Run("TestRelationshipExpiration", runner(tester, RelationshipExpirationTest))
	t.Run("TestMixedWriteOperations", runner(tester, MixedWriteOperationsTest))
	t.Run("TestRelationshipCaveatFiltering", runner(tester, RelationshipCaveatFilteringTest))

	t.Run("TestMultipleReadsInRWT", runner(tester, MultipleReadsInRWTTest))
	if !except.ConcurrentWrite() {
		t.Run("TestConcurrentWriteSerialization", runner(tester, ConcurrentWriteSerializationTest))
		t.Run("TestConcurrentWriteDeadlock", runner(tester, ConcurrentWriteDeadlockTest))
	}
	// Not gated on the ConcurrentWrite category: the overlapping transactions
	// never contend for the write lock, so even datastores with a global write
	// lock (e.g. memdb) must pass it. See the test's doc comment.
	t.Run("TestConcurrentWriteRevisionVisibility", runner(tester, ConcurrentWriteRevisionVisibilityTest))

	t.Run("TestOrdering", runner(tester, OrderingTest))
	t.Run("TestLimit", runner(tester, LimitTest))
	t.Run("TestOrderedLimit", runner(tester, OrderedLimitTest))
	t.Run("TestResume", runner(tester, ResumeTest))
	t.Run("TestReverseQueryCursor", runner(tester, ReverseQueryCursorTest))
	t.Run("TestReverseQueryFilteredCursor", runner(tester, ReverseQueryFilteredOverMultipleValuesCursorTest))

	t.Run("TestRevisionQuantization", runner(tester, RevisionQuantizationTest))
	t.Run("TestRevisionSerialization", runner(tester, RevisionSerializationTest))
	t.Run("TestSequentialRevisions", runner(tester, SequentialRevisionsTest))
	t.Run("TestConcurrentRevisions", runner(tester, ConcurrentRevisionsTest))
	t.Run("TestCheckRevisions", runner(tester, CheckRevisionsTest))

	if !except.GC() {
		OnlyGCTests(t, tester)
	}

	t.Run("TestBulkUpload", runner(tester, BulkUploadTest))
	t.Run("TestBulkUploadErrors", runner(tester, BulkUploadErrorsTest))
	t.Run("TestBulkUploadAlreadyExistsError", runner(tester, BulkUploadAlreadyExistsErrorTest))
	t.Run("TestBulkUploadAlreadyExistsSameCallError", runner(tester, BulkUploadAlreadyExistsSameCallErrorTest))
	t.Run("TestBulkUploadEditCaveat", runner(tester, BulkUploadEditCaveat))
	t.Run("TestBulkUploadWithCaveats", runner(tester, BulkUploadWithCaveats))
	t.Run("TestBulkUploadWithExpiration", runner(tester, BulkUploadWithExpiration))

	if !except.Stats() {
		t.Run("TestStats", runner(tester, StatsTest))
	}

	t.Run("TestRetries", func(t *testing.T) { RetryTest(t, tester, retryErr) })

	t.Run("TestCaveatNotFound", runner(tester, CaveatNotFoundTest))
	t.Run("TestWriteReadDeleteCaveat", runner(tester, WriteReadDeleteCaveatTest))
	t.Run("TestWriteCaveatedRelationship", runner(tester, WriteCaveatedRelationshipTest))
	t.Run("TestCaveatedRelationshipFilter", runner(tester, CaveatedRelationshipFilterTest))
	t.Run("TestCaveatSnapshotReads", runner(tester, CaveatSnapshotReadsTest))

	if !except.Watch() {
		t.Run("TestWatchBasic", runner(tester, WatchTest))
		t.Run("TestWatchCancel", runner(tester, WatchCancelTest))
		t.Run("TestCaveatedRelationshipWatch", runner(tester, CaveatedRelationshipWatchTest))
		t.Run("TestWatchWithTouch", runner(tester, WatchWithTouchTest))
		t.Run("TestWatchWithDelete", runner(tester, WatchWithDeleteTest))
		t.Run("TestWatchWithMetadata", runner(tester, WatchWithMetadataTest))
		t.Run("TestWatchWithExpiration", runner(tester, WatchWithExpirationTest))
		t.Run("TestWatchEmissionStrategy", runner(tester, WatchEmissionStrategyTest))
	}

	if !except.Watch() && !except.WatchSchema() {
		t.Run("TestWatchSchema", runner(tester, WatchSchemaTest))
		t.Run("TestWatchRelationshipsAndSchemaChanges", runner(tester, WatchRelationshipsAndSchemaChangesTest))
	}

	if !except.Watch() && !except.WatchCheckpoints() {
		t.Run("TestWatchObservesEveryReturnedRevision", runner(tester, WatchObservesEveryReturnedRevisionTest))
		t.Run("TestWatchEmitsCheckpointAfterWriteWithChanges", runner(tester, WatchEmitsCheckpointAfterWriteWithChangesTest))
	}

	if !except.Watch() && !except.WatchCheckpoints() && !except.WatchSchema() {
		t.Run("TestWatchRelationshipsAndSchemaAndCheckpoints", runner(tester, WatchRelationshipsAndSchemaAndCheckpointsTest))
	}

	if !except.Transaction() {
		t.Run("TestWriteAndReadInRWT", runner(tester, WriteAndReadInRWT))
	}

	t.Run("TestRelationshipCounters", runner(tester, RelationshipCountersTest))
	t.Run("TestUpdateRelationshipCounter", runner(tester, UpdateRelationshipCounterTest))
	t.Run("TestDeleteAllData", runner(tester, DeleteAllDataTest))
	t.Run("TestRelationshipCounterOverExpired", runner(tester, RelationshipCounterOverExpiredTest))
	t.Run("TestRegisterRelationshipCountersInParallel", runner(tester, RegisterRelationshipCountersInParallelTest))
	t.Run("TestRelationshipCountersWithOddFilter", runner(tester, RelationshipCountersWithOddFilterTest))
	t.Run("TestCounterCaseSensitivity", runner(tester, CounterCaseSensitivityTest))

	t.Run("TestStoredSchemaNotFound", runner(tester, StoredSchemaNotFoundTest))
	t.Run("TestStoredSchemaWriteRead", runner(tester, StoredSchemaWriteReadTest))
	t.Run("TestStoredSchemaRevision", runner(tester, StoredSchemaRevisionTest))
	t.Run("TestStoredSchemaUpdate", runner(tester, StoredSchemaUpdateTest))
	t.Run("TestStoredSchemaMultipleRevisions", runner(tester, StoredSchemaMultipleRevisionsTest))
	if !except.Transaction() {
		t.Run("TestStoredSchemaReadWithinTransaction", runner(tester, StoredSchemaReadWithinTransactionTest))
	}
	t.Run("TestStoredSchemaStableText", runner(tester, StoredSchemaStableTextTest))
	t.Run("TestStoredSchemaLarge", runner(tester, StoredSchemaLargeTest))
	t.Run("TestStoredSchemaPhaseMigration", runner(tester, StoredSchemaPhaseMigrationTest))
	t.Run("TestStoredSchemaViaDataLayer", runner(tester, StoredSchemaViaDataLayerTest))
	if !except.Transaction() {
		t.Run("TestStoredSchemaReadInWriteTxMemoized", runner(tester, StoredSchemaReadInWriteTxMemoizedTest))
	}
	t.Run("TestStoredSchemaAssertHashPrecondition", runner(tester, StoredSchemaAssertHashPreconditionTest))
	if !except.ConcurrentWrite() {
		t.Run("TestStoredSchemaConcurrentSchemaWins", runner(tester, StoredSchemaConcurrentSchemaWinsTest))
		t.Run("TestStoredSchemaWriteSucceedsUnderRelWriteTraffic", runner(tester, StoredSchemaWriteSucceedsUnderRelWriteTrafficTest))
	}
	t.Run("TestHeadRevisionSchemaHash", runner(tester, HeadRevisionSchemaHashTest))
	t.Run("TestOptimizedRevisionSchemaHash", runner(tester, OptimizedRevisionSchemaHashTest))

	if !except.Migration() {
		t.Run("TestMigration", runner(tester, MigrationTest))
	}
}

func OnlyGCTests(t *testing.T, tester DatastoreTester) {
	runner := serial

	t.Run("TestRevisionGC", runner(tester, RevisionGCTest))
	t.Run("TestInvalidReads", runner(tester, InvalidReadsTest))
	t.Run("TestGCProcessRuns", runner(tester, GCProcessRunTest))
}

// All runs all generic datastore tests on a DatastoreTester.
func All(t *testing.T, tester DatastoreTester, retryErr error) {
	AllWithExceptions(t, tester, noException, retryErr)
}

var testResourceNS = namespace.Namespace(
	testResourceNamespace,
	namespace.MustRelation(testReaderRelation, nil),
)

var testGroupNS = namespace.Namespace(
	testGroupNamespace,
	namespace.MustRelation(testMemberRelation, nil),
)

var testUserNS = namespace.Namespace(testUserNamespace)

func makeTestRel(resourceID, userID string) tuple.Relationship {
	return tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ObjectAndRelation{
				ObjectType: testResourceNamespace,
				ObjectID:   resourceID,
				Relation:   testReaderRelation,
			},
			Subject: tuple.ObjectAndRelation{
				ObjectType: testUserNamespace,
				ObjectID:   userID,
				Relation:   ellipsis,
			},
		},
	}
}

func setupDatastore(t *testing.T, ds datastore.Datastore) datastore.Revision {
	ctx := t.Context()

	schemaDefinitions := []datastore.SchemaDefinition{testGroupNS.CloneVT(), testResourceNS.CloneVT(), testUserNS.CloneVT()}
	compilerDefinitions := slicez.Map(schemaDefinitions, func(def datastore.SchemaDefinition) compiler.SchemaDefinition {
		return def.(compiler.SchemaDefinition)
	})
	// TODO: is this really necessary? Might be worth refactoring.
	schemaText, _, err := generator.GenerateSchema(ctx, compilerDefinitions)

	require.NoError(t, err)
	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		// TODO: add cache to this?
		_, _, err := datalayer.WriteSchemaViaStoredSchema(ctx, rwt, schemaDefinitions, schemaText)
		return err
	})
	require.NoError(t, err)

	return revision
}
