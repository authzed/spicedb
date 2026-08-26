package test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

// readYourWritesIterations is how many times ReadYourWritesTest repeats its
// write-then-read sequence within a single run.
const readYourWritesIterations = 50

// ReadYourWritesTest asserts that a write is visible at the revision the API's
// consistency rules select for that write's own ZedToken.
//
// It walks the same path as a permission check issued immediately after a
// write: the revision the write returns is encoded into a ZedToken, the
// consistency middleware turns that token back into a revision, and the data
// is read at that revision.
// A datastore that resolves or serves that revision incorrectly loses
// read-your-writes for every caller that round-trips a ZedToken.
//
// Both token-carrying consistency modes are covered:
//
//   - at_exact_snapshot resolves to exactly the token's revision, so the read
//     sees the world as of that write and nothing later.
//   - at_least_as_fresh resolves to the later of the token's revision and the
//     datastore's optimized revision, so the read never lands before the write.
//
// The sequence is repeated against one datastore, because the revision a write
// lands on depends on wall-clock timing, quantization boundaries and the
// datastore's own commit ordering: a single pass lands at one arbitrary offset
// into the quantization window and misses whatever the next offset exposes.
func ReadYourWritesTest(t *testing.T, tester DatastoreTester) {
	// Quantization decides which half of at_least_as_fresh does the work.
	// With no window the optimized revision has already caught up to the write
	// and wins the comparison; with a window far longer than the run it lags
	// every write, so the token's own revision is what keeps the write visible.
	// The window in between is neither: which side wins depends on where in the
	// window the iteration happens to land, which is what the loop varies.
	for _, quantization := range []time.Duration{0, 100 * time.Millisecond, 5 * time.Second} {
		t.Run(fmt.Sprintf("quantization%s", quantization), func(t *testing.T) {
			readYourWrites(t, tester, quantization)
		})
	}
}

func readYourWrites(t *testing.T, tester DatastoreTester, quantization time.Duration) {
	req := require.New(t)

	ds, err := tester.New(t, DefaultRevisionParameters().WithQuantization(quantization), 1)
	req.NoError(err)
	defer ds.Close()

	ctx := t.Context()
	setupDatastore(t, ds)

	dl := datalayer.NewDataLayer(ds)
	checker := testfixtures.RelationshipChecker{Require: req, DS: ds}

	// tokenFor encodes a revision the way WriteRelationships encodes the
	// revision it hands back as WrittenAt.
	tokenFor := func(rev datastore.Revision) *v1.ZedToken {
		token, err := zedtoken.NewFromRevision(ctx, rev, datalayer.NoSchemaHashInTransaction, dl)
		req.NoError(err)
		return token
	}

	// revisionFor resolves the revision the consistency middleware selects for
	// a CheckPermission carrying the given consistency block.
	revisionFor := func(c *v1.Consistency) datastore.Revision {
		reqCtx := consistency.ContextWithHandle(ctx)
		reqCtx = datalayer.ContextWithDataLayer(reqCtx, dl)

		err := consistency.AddRevisionToContext(reqCtx, &v1.CheckPermissionRequest{
			Consistency: c,
		}, dl, "", consistency.TreatMismatchingTokensAsError)
		req.NoError(err)

		rev, _, _, err := consistency.RevisionFromContext(reqCtx)
		req.NoError(err)
		return rev
	}

	atExactSnapshot := func(token *v1.ZedToken) datastore.Revision {
		return revisionFor(&v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{AtExactSnapshot: token},
		})
	}

	atLeastAsFresh := func(token *v1.ZedToken) datastore.Revision {
		return revisionFor(&v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{AtLeastAsFresh: token},
		})
	}

	// Each iteration uses its own relationships, so an assertion can never be
	// satisfied by a previous iteration's writes.
	for i := range readYourWritesIterations {
		// Write a relationship and keep the revision it was written at.
		first := makeTestRel(fmt.Sprintf("first-%d", i), "tom")
		firstRev, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, first)
		req.NoError(err)
		firstToken := tokenFor(firstRev)

		// at_exact_snapshot resolves to the write's own revision.
		firstExact := atExactSnapshot(firstToken)
		req.True(firstRev.Equal(firstExact),
			"iteration %d: at_exact_snapshot resolved to %v rather than the revision written at %v", i, firstExact, firstRev)
		checker.RelationshipExists(ctx, first, firstExact)

		// A permission check reads the schema as well as the relationships, so the
		// schema has to be readable at that same revision.
		storedSchema, err := ds.SnapshotReader(firstExact).ReadStoredSchema(ctx)
		req.NoError(err, "iteration %d: schema is not readable at the revision the write was made at", i)
		req.Contains(storedSchema.Get().GetV1().GetSchemaText(), testResourceNamespace)

		// at_least_as_fresh never resolves earlier than the token's revision, so
		// the write is visible there too.
		firstFresh := atLeastAsFresh(firstToken)
		req.False(firstFresh.LessThan(firstRev),
			"iteration %d: at_least_as_fresh resolved to %v, which is older than the write at %v", i, firstFresh, firstRev)
		checker.RelationshipExists(ctx, first, firstFresh)

		// A second write is visible at its own revision, alongside the first.
		second := makeTestRel(fmt.Sprintf("second-%d", i), "tom")
		secondRev, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, second)
		req.NoError(err)
		secondToken := tokenFor(secondRev)

		secondExact := atExactSnapshot(secondToken)
		checker.RelationshipExists(ctx, first, secondExact)
		checker.RelationshipExists(ctx, second, secondExact)

		secondFresh := atLeastAsFresh(secondToken)
		req.False(secondFresh.LessThan(secondRev),
			"iteration %d: at_least_as_fresh resolved to %v, which is older than the write at %v", i, secondFresh, secondRev)
		checker.RelationshipExists(ctx, first, secondFresh)
		checker.RelationshipExists(ctx, second, secondFresh)

		// The second write does not leak into the first write's snapshot.
		req.True(firstRev.Equal(atExactSnapshot(firstToken)),
			"iteration %d: at_exact_snapshot no longer resolves to the revision written at %v", i, firstRev)
		checker.RelationshipExists(ctx, first, atExactSnapshot(firstToken))
		checker.NoRelationshipExists(ctx, second, atExactSnapshot(firstToken))

		// A delete is read-your-writes in the same way: the relationship is gone at
		// the revision the delete was written at, and at any revision no older.
		deleteRev, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationDelete, first)
		req.NoError(err)
		deleteToken := tokenFor(deleteRev)

		deleteExact := atExactSnapshot(deleteToken)
		req.True(deleteRev.Equal(deleteExact),
			"iteration %d: at_exact_snapshot resolved to %v rather than the revision deleted at %v", i, deleteExact, deleteRev)
		checker.NoRelationshipExists(ctx, first, deleteExact)
		checker.RelationshipExists(ctx, second, deleteExact)

		deleteFresh := atLeastAsFresh(deleteToken)
		req.False(deleteFresh.LessThan(deleteRev),
			"iteration %d: at_least_as_fresh resolved to %v, which is older than the delete at %v", i, deleteFresh, deleteRev)
		checker.NoRelationshipExists(ctx, first, deleteFresh)
		checker.RelationshipExists(ctx, second, deleteFresh)

		// The delete does not reach back into the snapshot that preceded it.
		checker.RelationshipExists(ctx, first, atExactSnapshot(secondToken))
	}
}

// readYourConcurrentWritesWriters is how many writes
// ReadYourConcurrentWritesTest keeps in flight at once.
const readYourConcurrentWritesWriters = 16

// readYourConcurrentWritesRounds is how many times ReadYourConcurrentWritesTest
// repeats that concurrent round.
const readYourConcurrentWritesRounds = 8

// concurrentWrite is one writer's result from a round of
// ReadYourConcurrentWritesTest, recorded in the goroutine that made the write
// and asserted on afterwards by the test goroutine.
type concurrentWrite struct {
	rel tuple.Relationship
	rev datastore.Revision
	err error
}

// ReadYourConcurrentWritesTest asserts that a write is visible at the revision
// its own ZedToken names when it was committed alongside other writes.
//
// ReadYourWritesTest issues its writes one at a time, so each one is alone in
// whatever batch the datastore commits it in. A datastore that builds a
// revision out of a commit timestamp and then drops the tiebreaker ordering
// writes that share that timestamp still answers correctly there, because the
// tiebreaker is always zero. Keeping several writes in flight removes that
// condition: writes do share a commit timestamp, and the tiebreaker has to
// survive into the revision for the answers to stay right.
//
// The failure this catches is a lost read-your-writes. WriteRelationships
// returns a ZedToken whose revision sorts below the write it names, so a
// CheckPermission carrying that token cannot see the relationship the caller
// just wrote.
func ReadYourConcurrentWritesTest(t *testing.T, tester DatastoreTester) {
	req := require.New(t)

	// A quantization window longer than the run keeps the optimized revision
	// behind every write, so at_least_as_fresh resolves to the token's own
	// revision instead of to a later head that would hide the failure.
	ds, err := tester.New(t, DefaultRevisionParameters().WithQuantization(5*time.Second), 1)
	req.NoError(err)
	defer ds.Close()

	ctx := t.Context()
	setupDatastore(t, ds)

	dl := datalayer.NewDataLayer(ds)
	checker := testfixtures.RelationshipChecker{Require: req, DS: ds}

	// tokenFor encodes a revision the way WriteRelationships encodes the
	// revision it hands back as WrittenAt.
	tokenFor := func(rev datastore.Revision) *v1.ZedToken {
		token, err := zedtoken.NewFromRevision(ctx, rev, datalayer.NoSchemaHashInTransaction, dl)
		req.NoError(err)
		return token
	}

	// revisionFor resolves the revision the consistency middleware selects for
	// a CheckPermission carrying the given consistency block.
	revisionFor := func(c *v1.Consistency) datastore.Revision {
		reqCtx := consistency.ContextWithHandle(ctx)
		reqCtx = datalayer.ContextWithDataLayer(reqCtx, dl)

		err := consistency.AddRevisionToContext(reqCtx, &v1.CheckPermissionRequest{
			Consistency: c,
		}, dl, "", consistency.TreatMismatchingTokensAsError)
		req.NoError(err)

		rev, _, _, err := consistency.RevisionFromContext(reqCtx)
		req.NoError(err)
		return rev
	}

	atExactSnapshot := func(token *v1.ZedToken) datastore.Revision {
		return revisionFor(&v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{AtExactSnapshot: token},
		})
	}

	atLeastAsFresh := func(token *v1.ZedToken) datastore.Revision {
		return revisionFor(&v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{AtLeastAsFresh: token},
		})
	}

	// rounds[r][w] is the write writer w made in round r. Every writer uses its
	// own relationship, so an assertion can never be satisfied by another
	// writer's work.
	rounds := make([][]concurrentWrite, readYourConcurrentWritesRounds)

	for r := range readYourConcurrentWritesRounds {
		writes := make([]concurrentWrite, readYourConcurrentWritesWriters)

		// The writes have to be in flight together for the datastore to commit
		// them as a group, so every writer waits on the same starting gun.
		var start, done sync.WaitGroup
		start.Add(1)
		done.Add(readYourConcurrentWritesWriters)

		for w := range readYourConcurrentWritesWriters {
			go func() {
				defer done.Done()
				start.Wait()

				rel := makeTestRel(fmt.Sprintf("concurrent-%d-%d", r, w), "tom")
				rev, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, rel)

				// Only recorded here. require calls t.FailNow, which may run
				// on the test goroutine alone, so every assertion waits for
				// the rounds below.
				writes[w] = concurrentWrite{rel: rel, rev: rev, err: err}
			}()
		}

		start.Done()
		done.Wait()

		for w, write := range writes {
			req.NoError(write.err, "round %d writer %d: write failed", r, w)
		}
		rounds[r] = writes
	}

	for r, writes := range rounds {
		for w, write := range writes {
			token := tokenFor(write.rev)

			// The write is visible at exactly the revision it was made at.
			exact := atExactSnapshot(token)
			req.True(write.rev.Equal(exact),
				"round %d writer %d: at_exact_snapshot resolved to %v rather than the revision written at %v", r, w, exact, write.rev)
			checker.RelationshipExists(ctx, write.rel, exact)

			// And at every revision no older than that one.
			fresh := atLeastAsFresh(token)
			req.False(fresh.LessThan(write.rev),
				"round %d writer %d: at_least_as_fresh resolved to %v, which is older than the write at %v", r, w, fresh, write.rev)
			checker.RelationshipExists(ctx, write.rel, fresh)

			// A revision naming one write in a group must not expose writes
			// made after it. Widening every revision to cover its whole commit
			// batch would satisfy the two assertions above while leaking later
			// writes into earlier snapshots; this rules that out.
			if r+1 < len(rounds) {
				checker.NoRelationshipExists(ctx, rounds[r+1][w].rel, exact)
			}
		}
	}
}
