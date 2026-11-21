package test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

const waitForChangesTimeout = 20 * time.Second // FIXME, for reasons unknown, tests against Spanner emulator time out if this is lower

// Don't shorten this: at 10s the Spanner emulator times out even on writes that
// have nothing to do with schema.
const watchSchemaChangeTimeout = waitForChangesTimeout

// WatchTest tests whether or not the requirements for watching changes hold
// for a particular datastore.
func WatchTest(t *testing.T, tester DatastoreTester) {
	testCases := []struct {
		numTuples        int
		bufferTimeout    time.Duration
		expectFallBehind bool
	}{
		{
			numTuples:        1,
			bufferTimeout:    1 * time.Minute,
			expectFallBehind: false,
		},
		{
			numTuples:        2,
			bufferTimeout:    1 * time.Minute,
			expectFallBehind: false,
		},
		{
			numTuples:        256,
			bufferTimeout:    1 * time.Minute,
			expectFallBehind: false,
		},
		{
			numTuples:        256,
			bufferTimeout:    1 * time.Nanosecond,
			expectFallBehind: true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d-%v", tc.numTuples, tc.expectFallBehind), func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(t, DefaultRevisionParameters(), 16)
			require.NoError(err)

			setupDatastore(t, ds)

			lowestRevisionResult, err := ds.HeadRevision(t.Context())
			require.NoError(err)
			lowestRevision := lowestRevisionResult.Revision

			opts := datastore.WatchOptions{
				Content:                 datastore.WatchRelationships,
				WatchBufferLength:       50,
				WatchBufferWriteTimeout: tc.bufferTimeout,
			}
			changes, errchan := ds.Watch(t.Context(), lowestRevision, opts)
			require.Empty(errchan)

			var testUpdates [][]tuple.RelationshipUpdate
			var bulkDeletes []tuple.RelationshipUpdate
			for i := 0; i < tc.numTuples; i++ {
				newRelationship := makeTestRel(fmt.Sprintf("relation%d", i), "test_user")

				newUpdate := tuple.Touch(newRelationship)
				testUpdates = append(testUpdates, []tuple.RelationshipUpdate{newUpdate})

				_, err := common.UpdateRelationshipsInDatastore(t.Context(), ds, newUpdate)
				require.NoError(err)

				if i != 0 {
					bulkDeletes = append(bulkDeletes, tuple.Delete(newRelationship))
				}
			}

			updateUpdate := tuple.Touch(tuple.MustWithCaveat(makeTestRel("relation0", "test_user"), "somecaveat"))
			createUpdate := tuple.Touch(makeTestRel("another_relation", "somestuff"))

			_, err = common.UpdateRelationshipsInDatastore(t.Context(), ds, updateUpdate, createUpdate)
			require.NoError(err)

			deleteUpdate := tuple.Delete(makeTestRel("relation0", "test_user"))
			_, err = common.UpdateRelationshipsInDatastore(t.Context(), ds, deleteUpdate)
			require.NoError(err)

			testUpdates = append(testUpdates, []tuple.RelationshipUpdate{updateUpdate, createUpdate}, []tuple.RelationshipUpdate{deleteUpdate})

			_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				_, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
					ResourceType:     testResourceNamespace,
					OptionalRelation: testReaderRelation,
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       testUserNamespace,
						OptionalSubjectId: "test_user",
					},
				})
				require.NoError(err)
				return err
			})
			require.NoError(err)

			if len(bulkDeletes) > 0 {
				testUpdates = append(testUpdates, bulkDeletes)
			}

			VerifyUpdates(t, require, testUpdates, changes, errchan, tc.expectFallBehind)

			// Test the catch-up case
			changes, errchan = ds.Watch(t.Context(), lowestRevision, opts)
			VerifyUpdates(t, require, testUpdates, changes, errchan, tc.expectFallBehind)
		})
	}
}

func VerifyUpdates(
	t *testing.T,
	require *require.Assertions,
	testUpdates [][]tuple.RelationshipUpdate,
	changes <-chan datastore.RevisionChanges,
	errchan <-chan error,
	expectDisconnect bool,
) {
	t.Helper()
	for _, expected := range testUpdates {
		changeWait := time.NewTimer(waitForChangesTimeout)
		select {
		case change, ok := <-changes:
			if !ok {
				require.True(expectDisconnect, "unexpected disconnect")
				errWait := time.NewTimer(waitForChangesTimeout)
				select {
				case err := <-errchan:
					require.ErrorAs(err, &datastore.WatchDisconnectedError{})
					return
				case <-errWait.C:
					require.Fail("Timed out waiting for WatchDisconnectedError")
				}
				return
			}

			expectedChangeSet := setOfChanges(expected)
			actualChangeSet := setOfChanges(change.RelationshipChanges)

			missingExpected := expectedChangeSet.Difference(actualChangeSet)
			unexpected := actualChangeSet.Difference(expectedChangeSet)

			require.True(missingExpected.IsEmpty(), "expected changes missing: %s", missingExpected)
			require.True(unexpected.IsEmpty(), "unexpected changes: %s", unexpected)

			time.Sleep(1 * time.Millisecond)
		case <-changeWait.C:
			require.Fail("Timed out", "waited for changes: %s", expected)
		}
	}

	require.False(expectDisconnect, "all changes verified without expected disconnect")
}

func VerifyUpdatesWithMetadata(
	t *testing.T,
	require *require.Assertions,
	testUpdates []updateWithMetadata,
	changes <-chan datastore.RevisionChanges,
	errchan <-chan error,
	expectDisconnect bool,
) {
	t.Helper()
	for _, expected := range testUpdates {
		changeWait := time.NewTimer(waitForChangesTimeout)
		select {
		case change, ok := <-changes:
			if !ok {
				require.True(expectDisconnect, "unexpected disconnect")
				errWait := time.NewTimer(waitForChangesTimeout)
				select {
				case err := <-errchan:
					require.ErrorAs(err, &datastore.WatchDisconnectedError{})
					return
				case <-errWait.C:
					require.Fail("Timed out waiting for WatchDisconnectedError")
				}
				return
			}

			expectedChangeSet := setOfChanges(expected.updates)
			actualChangeSet := setOfChanges(change.RelationshipChanges)

			missingExpected := expectedChangeSet.Difference(actualChangeSet)
			unexpected := actualChangeSet.Difference(expectedChangeSet)

			require.True(missingExpected.IsEmpty(), "expected changes missing: %s", missingExpected)
			require.True(unexpected.IsEmpty(), "unexpected changes: %s", unexpected)

			if len(expected.metadata) == 0 {
				require.Empty(change.Metadatas, "expected no metadata, but found some: %v", change.Metadatas)
			} else {
				require.NotNil(change.Metadatas, "expected metadata, but found none")
				require.Len(change.Metadatas, 1, "expected single metadata entry, but found: %v", change.Metadatas)
			}

			time.Sleep(1 * time.Millisecond)
		case <-changeWait.C:
			require.Fail("Timed out", "waited for changes: %s", expected)
		}
	}

	require.False(expectDisconnect, "all changes verified without expected disconnect")
}

func setOfChanges(changes []tuple.RelationshipUpdate) *mapz.Set[string] {
	changeSet := mapz.NewSet[string]()
	for _, change := range changes {
		changeSet.Add(change.DebugString())
	}
	return changeSet
}

// WatchCancelTest tests whether or not the requirements for cancelling watches
// hold for a particular datastore.
func WatchCancelTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, DefaultRevisionParameters(), 1)
	require.NoError(err)

	startWatchRevision := setupDatastore(t, ds)

	ctx, cancel := context.WithCancel(t.Context())
	changes, errchan := ds.Watch(ctx, startWatchRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, makeTestRel("test", "test"))
	require.NoError(err)

	cancel()

	for {
		changeWait := time.NewTimer(waitForChangesTimeout)
		select {
		case created, ok := <-changes:
			if ok {
				foundDiff := cmp.Diff(
					[]tuple.RelationshipUpdate{tuple.Touch(makeTestRel("test", "test"))},
					created.RelationshipChanges,
					protocmp.Transform(),
				)
				require.Empty(foundDiff)
			} else {
				errWait := time.NewTimer(waitForChangesTimeout)
				require.Zero(created)
				select {
				case err := <-errchan:
					require.ErrorAs(err, &datastore.WatchCanceledError{})
					return
				case <-errWait.C:
					require.Fail("Timed out waiting for WatchCanceledError")
				}
				return
			}
		case <-changeWait.C:
			require.Fail("deadline exceeded waiting for cancellation")
		}
	}
}

func WatchWithTouchTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, DefaultRevisionParameters(), 16)
	require.NoError(err)

	setupDatastore(t, ds)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	lowestRevisionResult, err := ds.HeadRevision(ctx)
	require.NoError(err)
	lowestRevision := lowestRevisionResult.Revision

	// TOUCH a relationship and ensure watch sees it.
	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	afterTouchRevision, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)
	require.NoError(err)

	ensureRelationships(ctx, require, ds,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	VerifyUpdates(t, require, [][]tuple.RelationshipUpdate{
		{
			tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:tom")),
			tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:sarah")),
			tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]")),
		},
	},
		changes,
		errchan,
		false,
	)

	// TOUCH the relationship again with no changes and ensure it does *not* appear in the watch.
	changes, errchan = ds.Watch(ctx, afterTouchRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tuple.MustParse("document:firstdoc#viewer@user:tom"))
	require.NoError(err)

	ensureRelationships(ctx, require, ds,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	verifyNoUpdates(require,
		changes,
		errchan,
		false,
	)

	// TOUCH the relationship again with a caveat name change and ensure it does appear in the watch.
	changes, errchan = ds.Watch(ctx, afterTouchRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	afterNameChange, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tuple.MustParse("document:firstdoc#viewer@user:tom[somecaveat]"))
	require.NoError(err)

	ensureRelationships(ctx, require, ds,
		tuple.MustParse("document:firstdoc#viewer@user:tom[somecaveat]"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	VerifyUpdates(t, require, [][]tuple.RelationshipUpdate{
		{tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:tom[somecaveat]"))},
	},
		changes,
		errchan,
		false,
	)

	// TOUCH the relationship again with a caveat context change and ensure it does appear in the watch.
	changes, errchan = ds.Watch(ctx, afterNameChange, datastore.WatchJustRelationships())
	require.Empty(errchan)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tuple.MustParse("document:firstdoc#viewer@user:tom[somecaveat:{\"somecondition\": 42}]"))
	require.NoError(err)

	ensureRelationships(ctx, require, ds,
		tuple.MustParse("document:firstdoc#viewer@user:tom[somecaveat:{\"somecondition\": 42}]"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	VerifyUpdates(t, require, [][]tuple.RelationshipUpdate{
		{tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:tom[somecaveat:{\"somecondition\": 42}]"))},
	},
		changes,
		errchan,
		false,
	)
}

func WatchWithExpirationTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, DefaultRevisionParameters(), 16)
	require.NoError(err)

	setupDatastore(t, ds)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	lowestRevisionResult, err := ds.HeadRevision(ctx)
	require.NoError(err)
	lowestRevision := lowestRevisionResult.Revision

	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	metadata, err := structpb.NewStruct(map[string]any{"somekey": "somevalue"})
	require.NoError(err)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("document:firstdoc#viewer@user:tom[expiration:2321-01-01T00:00:00Z]")),
		})
	}, options.WithMetadata(metadata))
	require.NoError(err)

	VerifyUpdates(t, require, [][]tuple.RelationshipUpdate{
		{tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:tom[expiration:2321-01-01T00:00:00Z]"))},
	},
		changes,
		errchan,
		false,
	)
}

type updateWithMetadata struct {
	updates  []tuple.RelationshipUpdate
	metadata map[string]any
}

func WatchWithMetadataTest(t *testing.T, tester DatastoreTester) {
	if strings.Contains(t.Name(), "Spanner") {
		t.Skip("Spanner emulator does not support transaction tags. See https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/280")
	}

	require := require.New(t)

	ds, err := tester.New(t, DefaultRevisionParameters(), 16)
	require.NoError(err)

	setupDatastore(t, ds)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	lowestRevisionResult, err := ds.HeadRevision(ctx)
	require.NoError(err)
	lowestRevision := lowestRevisionResult.Revision

	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	metadata, err := structpb.NewStruct(map[string]any{"somekey": "somevalue"})
	require.NoError(err)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("document:firstdoc#viewer@user:tom")),
		})
	}, options.WithMetadata(metadata))
	require.NoError(err)

	VerifyUpdatesWithMetadata(t, require, []updateWithMetadata{
		{
			updates:  []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:tom"))},
			metadata: map[string]any{"somekey": "somevalue"},
		},
	},
		changes,
		errchan,
		false,
	)
}

func WatchWithDeleteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, DefaultRevisionParameters(), 16)
	require.NoError(err)

	setupDatastore(t, ds)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	lowestRevisionResult, err := ds.HeadRevision(ctx)
	require.NoError(err)
	lowestRevision := lowestRevisionResult.Revision

	// TOUCH a relationship and ensure watch sees it.
	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	afterTouchRevision, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)
	require.NoError(err)

	ensureRelationships(ctx, require, ds,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	VerifyUpdates(t, require, [][]tuple.RelationshipUpdate{
		{
			tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:tom")),
			tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:sarah")),
			tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]")),
		},
	},
		changes,
		errchan,
		false,
	)

	// DELETE the relationship
	changes, errchan = ds.Watch(ctx, afterTouchRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationDelete, tuple.MustParse("document:firstdoc#viewer@user:tom"))
	require.NoError(err)

	ensureRelationships(ctx, require, ds,
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	VerifyUpdates(t, require, [][]tuple.RelationshipUpdate{
		{tuple.Delete(tuple.MustParse("document:firstdoc#viewer@user:tom"))},
	},
		changes,
		errchan,
		false,
	)
}

func verifyNoUpdates(
	require *require.Assertions,
	changes <-chan datastore.RevisionChanges,
	errchan <-chan error,
	expectDisconnect bool,
) {
	changeWait := time.NewTimer(waitForChangesTimeout)
	select {
	case changes, ok := <-changes:
		if !ok {
			require.True(expectDisconnect, "unexpected disconnect")
			errWait := time.NewTimer(waitForChangesTimeout)
			select {
			case err := <-errchan:
				require.ErrorAs(err, &datastore.WatchDisconnectedError{})
				return
			case <-errWait.C:
				require.Fail("Timed out waiting for WatchDisconnectedError")
			}
			return
		}

		require.Empty(changes.RelationshipChanges, "expected no changes")
	case <-changeWait.C:
		return
	}
}

// Every schema mode. Only some of them save the schema in the older format that
// Watch reads, so running all four shows whether Watch works either way.
var watchSchemaModes = []struct {
	name string
	mode datalayer.SchemaMode
}{
	{"read-legacy-write-legacy", datalayer.SchemaModeReadLegacyWriteLegacy},
	{"read-legacy-write-both", datalayer.SchemaModeReadLegacyWriteBoth},
	{"read-new-write-both", datalayer.SchemaModeReadNewWriteBoth},
	{"read-new-write-new", datalayer.SchemaModeReadNewWriteNew},
}

// schemaState tracks the whole schema and writes it the way a running server
// does. The old API took one change at a time; this one takes the full schema
// and works out what changed, so each step here rewrites all of it.
type schemaState struct {
	t    *testing.T
	ctx  context.Context
	dl   datalayer.DataLayer
	defs map[string]datastore.SchemaDefinition
}

// Writes the test setup's schema once up front. Without it, the next write
// would look like every definition had just been created.
func newSchemaState(t *testing.T, ctx context.Context, ds datastore.Datastore, mode datalayer.SchemaMode) *schemaState {
	t.Helper()

	s := &schemaState{
		t:    t,
		ctx:  ctx,
		dl:   datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(mode)),
		defs: make(map[string]datastore.SchemaDefinition, 4),
	}

	_, err := s.put(testGroupNS.CloneVT(), testResourceNS.CloneVT(), testUserNS.CloneVT())
	require.NoError(t, err)
	return s
}

// put adds or replaces definitions, then writes the whole schema.
func (s *schemaState) put(defs ...datastore.SchemaDefinition) (datastore.Revision, error) {
	for _, def := range defs {
		s.defs[def.GetName()] = def
	}
	return s.write()
}

// remove drops definitions, then writes the whole schema. Leaving them out is
// what turns into a delete.
func (s *schemaState) remove(names ...string) (datastore.Revision, error) {
	for _, name := range names {
		delete(s.defs, name)
	}
	return s.write()
}

func (s *schemaState) write() (datastore.Revision, error) {
	s.t.Helper()

	names := make([]string, 0, len(s.defs))
	for name := range s.defs {
		names = append(names, name)
	}
	sort.Strings(names)

	defs := make([]datastore.SchemaDefinition, 0, len(names))
	compilerDefs := make([]compiler.SchemaDefinition, 0, len(names))
	for _, name := range names {
		defs = append(defs, s.defs[name])
		compilerDefs = append(compilerDefs, s.defs[name].(compiler.SchemaDefinition))
	}

	schemaText, _, err := generator.GenerateSchema(s.ctx, compilerDefs)
	require.NoError(s.t, err)

	return s.dl.ReadWriteTx(s.ctx, func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
}

// Builds a caveat that actually parses. The write path rejects ones that don't.
func mustCaveatDefinition(t *testing.T, name string, expr string) *core.CaveatDefinition {
	t.Helper()

	env, err := caveats.EnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
		"a": caveattypes.Default.IntType,
		"b": caveattypes.Default.IntType,
	})
	require.NoError(t, err)

	compiled, err := caveats.CompileCaveatWithName(env, expr, name)
	require.NoError(t, err)

	serialized, err := compiled.Serialize()
	require.NoError(t, err)

	return &core.CaveatDefinition{
		Name:                 name,
		SerializedExpression: serialized,
		ParameterTypes:       env.EncodedParametersTypes(),
	}
}

func WatchSchemaTest(t *testing.T, tester DatastoreTester) {
	for _, tc := range watchSchemaModes {
		t.Run(tc.name, func(t *testing.T) {
			watchSchemaTestForMode(t, tester, tc.mode)
		})
	}
}

func watchSchemaTestForMode(t *testing.T, tester DatastoreTester, mode datalayer.SchemaMode) {
	require := require.New(t)

	ds, err := tester.New(t, DefaultRevisionParameters(), 16)
	require.NoError(err)

	setupDatastore(t, ds)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	state := newSchemaState(t, ctx, ds, mode)

	lowestRevisionResult, err := ds.HeadRevision(ctx)
	require.NoError(err)
	lowestRevision := lowestRevisionResult.Revision

	// Watch through the DataLayer, as a running server does: it selects the schema
	// storage from which schema changes are sourced, based on the schema mode.
	changes, errchan := state.dl.Watch(ctx, lowestRevision, datastore.WatchJustSchema())
	require.Empty(errchan)

	// Addition
	// Write an updated schema and ensure the changes are returned.
	_, err = state.put(
		&core.NamespaceDefinition{Name: "somenewnamespace"},
		mustCaveatDefinition(t, "somenewcaveat", "a == b"),
	)
	require.NoError(err)

	verifyMixedUpdates(require, mode, [][]string{
		{
			"changed:somenewnamespace",
			"changed:somenewcaveat",
		},
	}, changes, errchan, false)

	// Changed
	_, err = state.put(
		&core.NamespaceDefinition{
			Name: "somenewnamespace",
			Relation: []*core.Relation{
				{
					Name: "anotherrelation",
				},
			},
		},
		mustCaveatDefinition(t, "somenewcaveat", "a != b"),
	)
	require.NoError(err)

	verifyMixedUpdates(require, mode, [][]string{
		{
			"changed:somenewnamespace",
			"changed:somenewcaveat",
		},
	}, changes, errchan, false)

	// Removed
	// Delete some namespaces and caveats.
	_, err = state.remove("somenewnamespace", "somenewcaveat")
	require.NoError(err)

	verifyMixedUpdates(require, mode, [][]string{
		{
			"deleted-ns:somenewnamespace",
			"deleted-caveat:somenewcaveat",
		},
	}, changes, errchan, false)
}

func WatchRelationshipsAndSchemaChangesTest(t *testing.T, tester DatastoreTester) {
	for _, tc := range watchSchemaModes {
		t.Run(tc.name, func(t *testing.T) {
			watchRelationshipsAndSchemaChangesTestForMode(t, tester, tc.mode)
		})
	}
}

func watchRelationshipsAndSchemaChangesTestForMode(t *testing.T, tester DatastoreTester, mode datalayer.SchemaMode) {
	require := require.New(t)

	ds, err := tester.New(t, DefaultRevisionParameters(), 16)
	require.NoError(err)

	setupDatastore(t, ds)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	state := newSchemaState(t, ctx, ds, mode)

	lowestRevisionResult, err := ds.HeadRevision(ctx)
	require.NoError(err)
	lowestRevision := lowestRevisionResult.Revision

	changes, errchan := state.dl.Watch(ctx, lowestRevision, datastore.WatchOptions{
		Content: datastore.WatchRelationships | datastore.WatchSchema,
	})
	require.Empty(errchan)

	// Write an updated schema.
	_, err = state.put(
		&core.NamespaceDefinition{Name: "somenewnamespace"},
		mustCaveatDefinition(t, "somenewcaveat", "a == b"),
	)
	require.NoError(err)

	verifyMixedUpdates(require, mode, [][]string{
		{
			"changed:somenewnamespace",
			"changed:somenewcaveat",
		},
	}, changes, errchan, false)

	// Write some relationships.
	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)
	require.NoError(err)

	verifyMixedUpdates(require, mode, [][]string{
		{
			"rel:TOUCH(document:firstdoc#viewer@user:fred)",
			"rel:TOUCH(document:firstdoc#viewer@user:sarah)",
			"rel:TOUCH(document:firstdoc#viewer@user:tom)",
		},
	}, changes, errchan, false)

	// Delete some namespaces and caveats.
	_, err = state.remove("somenewnamespace", "somenewcaveat")
	require.NoError(err)

	verifyMixedUpdates(require, mode, [][]string{
		{
			"deleted-ns:somenewnamespace",
			"deleted-caveat:somenewcaveat",
		},
	}, changes, errchan, false)
}

// Explains a missing schema event, so a failure says why instead of just
// timing out.
func schemaModeFailureContext(mode datalayer.SchemaMode) string {
	if mode.WritesToLegacy() {
		return fmt.Sprintf("schema mode %q writes to legacy storage", mode)
	}

	return fmt.Sprintf("schema mode %q writes only the unified stored schema, so Watch schema events must be derived from it "+
		"rather than from the legacy namespace/caveat tables", mode)
}

func verifyMixedUpdates(
	require *require.Assertions,
	mode datalayer.SchemaMode,
	expectedUpdates [][]string,
	changes <-chan datastore.RevisionChanges,
	errchan <-chan error,
	expectDisconnect bool,
) {
	for _, expected := range expectedUpdates {
		changeWait := time.NewTimer(watchSchemaChangeTimeout)
		select {
		case change, ok := <-changes:
			if !ok {
				require.True(expectDisconnect, "unexpected disconnect")
				errWait := time.NewTimer(waitForChangesTimeout)
				select {
				case err := <-errchan:
					require.ErrorAs(err, &datastore.WatchDisconnectedError{})
					return
				case <-errWait.C:
					require.Fail("Timed out waiting for WatchDisconnectedError")
				}
				return
			}

			foundChanges := mapz.NewSet[string]()
			for _, changedDef := range change.ChangedDefinitions {
				foundChanges.Insert("changed:" + changedDef.GetName())
			}
			for _, deleted := range change.DeletedNamespaces {
				foundChanges.Insert("deleted-ns:" + deleted)
			}
			for _, deleted := range change.DeletedCaveats {
				foundChanges.Insert("deleted-caveat:" + deleted)
			}

			for _, update := range change.RelationshipChanges {
				foundChanges.Insert("rel:" + update.DebugString())
			}

			found := foundChanges.AsSlice()

			sort.Strings(expected)
			sort.Strings(found)

			require.Equal(expected, found, "found unexpected changes; %s", schemaModeFailureContext(mode))
			time.Sleep(1 * time.Millisecond)
		case <-changeWait.C:
			require.Fail("Timed out", "waited %s for changes: %s; %s", watchSchemaChangeTimeout, expected, schemaModeFailureContext(mode))
		}
	}

	require.False(expectDisconnect, "all changes verified without expected disconnect")
}

func WatchRelationshipsAndSchemaAndCheckpointsTest(t *testing.T, tester DatastoreTester) {
	for _, tc := range watchSchemaModes {
		t.Run(tc.name, func(t *testing.T) {
			watchRelationshipsAndSchemaAndCheckpointsTestForMode(t, tester, tc.mode)
		})
	}
}

func watchRelationshipsAndSchemaAndCheckpointsTestForMode(t *testing.T, tester DatastoreTester, mode datalayer.SchemaMode) {
	require := require.New(t)

	ds, err := tester.New(t, DefaultRevisionParameters(), 16)
	require.NoError(err)

	setupDatastore(t, ds)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	state := newSchemaState(t, ctx, ds, mode)

	lowestRevisionResult, err := ds.HeadRevision(ctx)
	require.NoError(err)
	lowestRevision := lowestRevisionResult.Revision

	changes, errchan := state.dl.Watch(ctx, lowestRevision, datastore.WatchOptions{
		Content:            datastore.WatchCheckpoints | datastore.WatchRelationships | datastore.WatchSchema,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	afterTouchRevision, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
	)
	require.NoError(err)

	verifyCheckpointUpdate(require, mode, afterTouchRevision, changes)

	afterTouchRevision, err = state.put(&core.NamespaceDefinition{Name: "doesnotexist"})
	require.NoError(err)

	verifyCheckpointUpdate(require, mode, afterTouchRevision, changes)
}

func WatchEmissionStrategyTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, DefaultRevisionParameters(), 16)
	require.NoError(err)

	setupDatastore(t, ds)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	features, err := ds.Features(ctx)
	require.NoError(err)

	expectsWatchError := (features.WatchEmitsImmediately.Status != datastore.FeatureSupported)

	lowestRevisionResult, err := ds.HeadRevision(ctx)
	require.NoError(err)
	lowestRevision := lowestRevisionResult.Revision

	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchOptions{
		Content:            datastore.WatchCheckpoints | datastore.WatchRelationships,
		CheckpointInterval: 100 * time.Millisecond,
		EmissionStrategy:   datastore.EmitImmediatelyStrategy,
	})
	if expectsWatchError {
		require.NotEmpty(errchan)
		err := <-errchan
		require.ErrorContains(err, "emit immediately strategy is unsupported")
		return
	}
	require.Empty(errchan)

	// since changes are streamed immediately, we expect changes to be streamed as independent change events,
	// whereas with the default emission strategy it would be accumulated and normalized. For examples, the default
	// strategy would accumulate everything into a single Change sent over the channel, like an added relationship
	// and its associated transaction metadata. The emit immediately strategy will emit both
	// the rel touch and tx metadata as independent changes as it won't accumulate and normalize it.
	testTuple := tuple.MustParse("document:firstdoc#viewer@user:" + uuid.NewString())
	testMetadata, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(err)
	targetRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(testTuple),
		})
	}, options.WithMetadata(testMetadata))
	require.NoError(err)

	var relTouchEmitted, metadataEmitted, checkpointEmitted bool
	changeWait := time.NewTimer(waitForChangesTimeout)
	var changeCount int
	for {
		select {
		case change, ok := <-changes:
			require.True(ok)
			for _, relChange := range change.RelationshipChanges {
				if relChange.Relationship.WithoutIntegrity() == testTuple && relChange.Operation == tuple.UpdateOperationTouch {
					relTouchEmitted = true
					changeCount++
					require.True(targetRev.Equal(change.Revision))
				}

				continue // we expect each change to come in individual change event
			}

			if len(change.Metadatas) == 1 {
				require.Contains(change.Metadatas[0].AsMap(), "foo")
				metadataEmitted = true
				changeCount++
				require.True(targetRev.Equal(change.Revision))
				continue
			}

			if change.IsCheckpoint {
				if change.Revision.Equal(targetRev) || change.Revision.GreaterThan(targetRev) {
					require.True(metadataEmitted, "expected tx metadata before checkpoint")
					require.True(relTouchEmitted, "expected relationship touch before checkpoint")
					require.GreaterOrEqual(changeCount, 2, "expected at least 2 changes over the channel")
					return
				}
			}
		case <-changeWait.C:
			require.Fail("Timed out", "waited for checkpoint")
		}

		if relTouchEmitted && metadataEmitted && checkpointEmitted {
			return
		}
	}
}

// WatchObservesEveryReturnedRevisionTest verifies this contract: if
// WriteRelationships returns revision X, a watch consumer eventually observes
// revision X on the watch stream — even when the write produced no actual
// changes (e.g., a TOUCH that exactly matches an existing relationship).
func WatchObservesEveryReturnedRevisionTest(t *testing.T, tester DatastoreTester) {
	ds, err := tester.New(t, DefaultRevisionParameters(), 16)
	require.NoError(t, err)

	setupDatastore(t, ds)

	// First TOUCH creates the relationship.
	afterCreateRevision, err := common.WriteRelationships(t.Context(), ds, tuple.UpdateOperationTouch,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
	)
	require.NoError(t, err)

	// Start watching from after the create.
	changes, errchan := ds.Watch(t.Context(), afterCreateRevision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(t, errchan)

	// Second TOUCH of the same relationship is a no-op (no actual changes),
	secondRevision, err := common.WriteRelationships(t.Context(), ds, tuple.UpdateOperationTouch,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
	)
	require.NoError(t, err)

	verifyWatchReachesRevision(t, secondRevision, changes, errchan)
}

func verifyWatchReachesRevision(
	t *testing.T,
	targetRevision datastore.Revision,
	changes <-chan datastore.RevisionChanges,
	errchan <-chan error,
) {
	for {
		select {
		case change, ok := <-changes:
			require.True(t, ok, "watch stream closed before observing revision %s", targetRevision)
			if change.Revision != nil &&
				(change.Revision.Equal(targetRevision) || change.Revision.GreaterThan(targetRevision)) {
				return
			}
		case err := <-errchan:
			require.NoError(t, err, "unexpected watch error while waiting for revision %s", targetRevision)
		case <-time.After(waitForChangesTimeout):
			require.Fail(t, "Timed out waiting for watch to reach revision", "target: %s", targetRevision)
			return
		}
	}
}

// WatchEmitsCheckpointAfterWriteWithChangesTest verifies that when a consumer asks for checkpoints,
// every revision returned by WriteRelationships must eventually be observable as a checkpoint.
func WatchEmitsCheckpointAfterWriteWithChangesTest(t *testing.T, tester DatastoreTester) {
	ds, err := tester.New(t, DefaultRevisionParameters(), 16)
	require.NoError(t, err)

	setupDatastore(t, ds)

	startingRevisionResult, err := ds.HeadRevision(t.Context())
	require.NoError(t, err)
	startingRevision := startingRevisionResult.Revision

	changes, errchan := ds.Watch(t.Context(), startingRevision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(t, errchan)

	// A TOUCH that produces a real change.
	afterWriteRevision, err := common.WriteRelationships(t.Context(), ds, tuple.UpdateOperationTouch,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
	)
	require.NoError(t, err)

	verifyWatchEmitsCheckpoint(t, afterWriteRevision, changes, errchan)
}

func verifyWatchEmitsCheckpoint(
	t *testing.T,
	targetRevision datastore.Revision,
	changes <-chan datastore.RevisionChanges,
	errchan <-chan error,
) {
	for {
		select {
		case change, ok := <-changes:
			require.True(t, ok, "watch stream closed before observing checkpoint at revision %s", targetRevision)
			if change.IsCheckpoint && change.Revision != nil &&
				(change.Revision.Equal(targetRevision) || change.Revision.GreaterThan(targetRevision)) {
				return
			}
		case err := <-errchan:
			require.NoError(t, err, "unexpected watch error while waiting for checkpoint at revision %s", targetRevision)
		case <-time.After(waitForChangesTimeout):
			require.Fail(t, "Timed out waiting for checkpoint", "target: %s", targetRevision)
			return
		}
	}
}

func verifyCheckpointUpdate(
	require *require.Assertions,
	mode datalayer.SchemaMode,
	expectedRevision datastore.Revision,
	changes <-chan datastore.RevisionChanges,
) {
	var relChangeEmitted, schemaChangeEmitted bool
	changeWait := time.NewTimer(watchSchemaChangeTimeout)
	for {
		select {
		case change, ok := <-changes:
			require.True(ok)
			if len(change.ChangedDefinitions) > 0 {
				schemaChangeEmitted = true
			}
			if len(change.RelationshipChanges) > 0 {
				relChangeEmitted = true
			}
			if change.IsCheckpoint {
				if change.Revision.Equal(expectedRevision) || change.Revision.GreaterThan(expectedRevision) {
					require.True(relChangeEmitted || schemaChangeEmitted,
						"expected relationship/schema changes before checkpoint; %s", schemaModeFailureContext(mode))
					return
				}

				// we received a past revision checkpoint, ignore
			}
		case <-changeWait.C:
			require.Fail("Timed out", "waited %s for checkpoint; %s", watchSchemaChangeTimeout, schemaModeFailureContext(mode))
		}
	}
}

// WatchSchemaViaDataLayerTest verifies that schema changes emitted by the
// DataLayer's Watch are sourced from the schema storage selected by the
// DataLayer's schema mode: the legacy per-definition tables when reading from
// legacy, and the unified stored schema when reading from new.
//
// To tell the two sources apart, each transaction writes differently named
// definitions directly to each storage.
func WatchSchemaViaDataLayerTest(t *testing.T, tester DatastoreTester) {
	modes := []struct {
		name         string
		mode         datalayer.SchemaMode
		readsFromNew bool
	}{
		{"ReadLegacyWriteLegacy", datalayer.SchemaModeReadLegacyWriteLegacy, false},
		{"ReadLegacyWriteBoth", datalayer.SchemaModeReadLegacyWriteBoth, false},
		{"ReadNewWriteBoth", datalayer.SchemaModeReadNewWriteBoth, true},
		{"ReadNewWriteNew", datalayer.SchemaModeReadNewWriteNew, true},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(t, DefaultRevisionParameters(), 16)
			require.NoError(err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			dl := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(tc.mode))

			lowestRevision, _, err := dl.HeadRevision(ctx)
			require.NoError(err)

			changes, errchan := dl.Watch(ctx, lowestRevision, datastore.WatchJustSchema())
			require.Empty(errchan)

			writeStoredSchema := func(ctx context.Context, rwt datastore.ReadWriteTransaction, schemaText string) error {
				compiled, err := compiler.Compile(compiler.InputSchema{
					Source:       input.Source("schema"),
					SchemaString: schemaText,
				}, compiler.AllowUnprefixedObjectType())
				if err != nil {
					return err
				}

				_, _, err = datalayer.WriteSchemaViaStoredSchema(ctx, rwt, toSchemaDefinitions(compiled.OrderedDefinitions), schemaText)
				return err
			}

			// Addition
			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				if err := rwt.LegacyWriteNamespaces(ctx, &core.NamespaceDefinition{Name: "legacyonly"}); err != nil {
					return err
				}

				if err := rwt.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{{Name: "legacycaveat"}}); err != nil {
					return err
				}

				return writeStoredSchema(ctx, rwt, `caveat newcaveat(somevalue int) {
	somevalue > 1
}

definition newonly {}`)
			})
			require.NoError(err)

			expected := []string{"changed:legacyonly", "changed:legacycaveat"}
			if tc.readsFromNew {
				expected = []string{"changed:newonly", "changed:newcaveat"}
			}
			verifyMixedUpdates(require, tc.mode, [][]string{expected}, changes, errchan, false)

			// Changed and removed
			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				if err := rwt.LegacyDeleteNamespaces(ctx, []string{"legacyonly"}, datastore.DeleteNamespacesAndRelationships); err != nil {
					return err
				}

				if err := rwt.LegacyDeleteCaveats(ctx, []string{"legacycaveat"}); err != nil {
					return err
				}

				if err := rwt.LegacyWriteNamespaces(ctx, &core.NamespaceDefinition{Name: "legacyother"}); err != nil {
					return err
				}

				return writeStoredSchema(ctx, rwt, `definition newother {}`)
			})
			require.NoError(err)

			expected = []string{"changed:legacyother", "deleted-ns:legacyonly", "deleted-caveat:legacycaveat"}
			if tc.readsFromNew {
				expected = []string{"changed:newother", "deleted-ns:newonly", "deleted-caveat:newcaveat"}
			}
			verifyMixedUpdates(require, tc.mode, [][]string{expected}, changes, errchan, false)
		})
	}
}

// WatchSchemaEmissionStrategyViaDataLayerTest verifies that schema changes are emitted
// immediately, in every schema mode, when watching through the DataLayer with the
// EmitImmediatelyStrategy.
func WatchSchemaEmissionStrategyViaDataLayerTest(t *testing.T, tester DatastoreTester) {
	for _, tc := range watchSchemaModes {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(t, DefaultRevisionParameters(), 16)
			require.NoError(err)

			setupDatastore(t, ds)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			features, err := ds.Features(ctx)
			require.NoError(err)

			expectsWatchError := (features.WatchEmitsImmediately.Status != datastore.FeatureSupported)

			state := newSchemaState(t, ctx, ds, tc.mode)

			lowestRevisionResult, err := ds.HeadRevision(ctx)
			require.NoError(err)
			lowestRevision := lowestRevisionResult.Revision

			changes, errchan := state.dl.Watch(ctx, lowestRevision, datastore.WatchOptions{
				Content:            datastore.WatchCheckpoints | datastore.WatchSchema,
				CheckpointInterval: 100 * time.Millisecond,
				EmissionStrategy:   datastore.EmitImmediatelyStrategy,
			})
			if expectsWatchError {
				select {
				case err := <-errchan:
					require.ErrorContains(err, "emit immediately strategy is unsupported")
				case <-time.After(waitForChangesTimeout):
					require.Fail("Timed out", "waited for emit immediately strategy error")
				}
				return
			}
			require.Empty(errchan)

			targetRev, err := state.put(
				&core.NamespaceDefinition{Name: "somenewnamespace"},
				mustCaveatDefinition(t, "somenewcaveat", "a == b"),
			)
			require.NoError(err)

			// Changes are not accumulated, so the definitions may arrive in separate changes.
			foundChanges := mapz.NewSet[string]()
			changeWait := time.NewTimer(watchSchemaChangeTimeout)
			for {
				select {
				case change, ok := <-changes:
					require.True(ok)
					require.False(change.StoredSchemaChanged, "stored schema markers must not be emitted to callers")

					for _, changedDef := range change.ChangedDefinitions {
						require.True(targetRev.Equal(change.Revision))
						foundChanges.Insert("changed:" + changedDef.GetName())
					}
					require.Empty(change.DeletedNamespaces)
					require.Empty(change.DeletedCaveats)

					if change.IsCheckpoint && (change.Revision.Equal(targetRev) || change.Revision.GreaterThan(targetRev)) {
						found := foundChanges.AsSlice()
						sort.Strings(found)
						require.Equal([]string{"changed:somenewcaveat", "changed:somenewnamespace"}, found,
							"expected schema changes before checkpoint; %s", schemaModeFailureContext(tc.mode))
						return
					}
				case err := <-errchan:
					require.Fail("Unexpected watch error", "%v; %s", err, schemaModeFailureContext(tc.mode))
				case <-changeWait.C:
					require.Fail("Timed out", "waited %s for checkpoint; %s", watchSchemaChangeTimeout, schemaModeFailureContext(tc.mode))
				}
			}
		})
	}
}
