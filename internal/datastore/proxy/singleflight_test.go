package proxy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

type fakeRevisionDatastore struct {
	datastore.Datastore
	headRevision  func(ctx context.Context) (datastore.RevisionWithSchemaHash, error)
	checkRevision func(ctx context.Context) error
}

func (f *fakeRevisionDatastore) HeadRevision(ctx context.Context) (datastore.RevisionWithSchemaHash, error) {
	return f.headRevision(ctx)
}

func (f *fakeRevisionDatastore) CheckRevision(ctx context.Context, _ datastore.Revision) error {
	return f.checkRevision(ctx)
}

func newSingleflightProxyForTest(d datastore.Datastore) *singleflightProxy {
	p := NewSingleflightDatastoreProxy(d).(*singleflightProxy)
	p.sharedTimeout = 10 * time.Millisecond
	p.retryTimeout = 50 * time.Millisecond
	return p
}

// TestSingleflightHeadRevisionSharedTimeout checks that a hung shared call
// releases every waiting caller, each of which then succeeds on its direct retry.
func TestSingleflightHeadRevisionSharedTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		p := newSingleflightProxyForTest(&fakeRevisionDatastore{
			headRevision: func(ctx context.Context) (datastore.RevisionWithSchemaHash, error) {
				if calls.Add(1) == 1 {
					<-ctx.Done()
					return datastore.RevisionWithSchemaHash{}, ctx.Err()
				}
				return datastore.RevisionWithSchemaHash{Revision: one}, nil
			},
		})

		const callers = 5
		revs := make([]datastore.RevisionWithSchemaHash, callers)
		errs := make([]error, callers)
		var wg sync.WaitGroup
		for i := range callers {
			wg.Go(func() {
				revs[i], errs[i] = p.HeadRevision(t.Context())
			})
		}
		wg.Wait()

		for i := range callers {
			require.NoError(t, errs[i])
			require.True(t, one.Equal(revs[i].Revision))
		}
		require.Equal(t, int32(1+callers), calls.Load())
	})
}

// TestSingleflightHeadRevisionRetryTimeout checks that a caller without a
// deadline still returns when the direct retry also hangs.
func TestSingleflightHeadRevisionRetryTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newSingleflightProxyForTest(&fakeRevisionDatastore{
			headRevision: func(ctx context.Context) (datastore.RevisionWithSchemaHash, error) {
				<-ctx.Done()
				return datastore.RevisionWithSchemaHash{}, ctx.Err()
			},
		})

		_, err := p.HeadRevision(t.Context())
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

// TestSingleflightCheckRevisionRetriesOnlyTimeouts checks that CheckRevision is
// bounded the same way, and that datastore errors are returned without a retry.
func TestSingleflightCheckRevisionRetriesOnlyTimeouts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		fake := &fakeRevisionDatastore{
			checkRevision: func(ctx context.Context) error {
				if calls.Add(1) == 1 {
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			},
		}
		p := newSingleflightProxyForTest(fake)

		require.NoError(t, p.CheckRevision(t.Context(), one))
		require.Equal(t, int32(2), calls.Load())

		errStale := errors.New("stale revision")
		calls.Store(0)
		fake.checkRevision = func(context.Context) error {
			calls.Add(1)
			return errStale
		}
		require.ErrorIs(t, p.CheckRevision(t.Context(), two), errStale)
		require.Equal(t, int32(1), calls.Load())
	})
}
