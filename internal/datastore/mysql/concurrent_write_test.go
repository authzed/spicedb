//go:build datastore

package mysql

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

// TestMySQLConcurrentWriteDeadlock reproduces https://github.com/authzed/spicedb/issues/3172:
// many concurrent WriteRelationships targeting the same resource object contend for the same
// locks under SERIALIZABLE and MySQL aborts some transactions with a deadlock (Error 1213).
// Before the retry loop backed off between attempts it retried with no delay, so the retries
// kept re-colliding and exhausted the retry budget, failing with "max retries exceeded". With
// jittered exponential backoff the contending writers spread out and all eventually succeed.
//
// A low MaxRetries is used so the no-backoff behavior on main reliably exhausts the budget,
// while the backoff fix keeps all writers within budget.
func TestMySQLConcurrentWriteDeadlock(t *testing.T) {
	b := testdatastore.RunMySQLForTestingWithOptions(t, testdatastore.MySQLTesterOptions{MigrateForNewDatastore: true}, "")
	t.Run("ConcurrentWriteDeadlock", createDatastoreTest(b, ConcurrentWriteDeadlockTest, append(defaultOptions, MaxRetries(3))...))
}

func ConcurrentWriteDeadlockTest(t *testing.T, ds datastore.Datastore) {
	req := require.New(t)
	ctx := t.Context()

	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(
			ctx,
			namespace.Namespace(
				"resource",
				namespace.MustRelation("reader", nil),
			),
			namespace.Namespace("user"),
		)
	})
	req.NoError(err)

	const concurrency = 50

	// Each goroutine uses the test's context directly (not a shared cancelable group
	// context) so that one writer giving up does not abort the others; we want to observe
	// every writer's final outcome.
	errs := make([]error, concurrency)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			rel := tuple.Touch(tuple.MustParse(fmt.Sprintf("resource:shared#reader@user:user-%d", i)))
			_, errs[i] = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{rel})
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		req.NoErrorf(err, "concurrent write %d to the shared resource should eventually succeed", i)
	}
}
