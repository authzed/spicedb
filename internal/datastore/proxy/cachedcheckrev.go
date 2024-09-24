package proxy

import (
	"context"
	"sync/atomic"

	"github.com/authzed/spicedb/pkg/datastore"
)

// newCachedCheckRevision wraps a datastore with a cache that will avoid checking the revision
// if the last checked revision is at least as fresh as the one specified.
func newCachedCheckRevision(ds datastore.ReadOnlyDatastore) datastore.ReadOnlyDatastore {
	return &cachedCheckRevision{
		ReadOnlyDatastore: ds,
		lastCheckRevision: atomic.Pointer[datastore.Revision]{},
	}
}

type cachedCheckRevision struct {
	datastore.ReadOnlyDatastore
	lastCheckRevision atomic.Pointer[datastore.Revision]
}

func (c *cachedCheckRevision) CheckRevision(ctx context.Context, rev datastore.Revision) error {
	// Check if we've already seen a revision at least as fresh as that specified. If so, we can skip the check.
	lastChecked := c.lastCheckRevision.Load()
	if lastChecked != nil {
		lastCheckedRev := *lastChecked
		if lastCheckedRev.Equal(rev) || lastCheckedRev.GreaterThan(rev) {
			return nil
		}
	}

	err := c.ReadOnlyDatastore.CheckRevision(ctx, rev)
	if err != nil {
		return err
	}

	if lastChecked == nil || rev.LessThan(*lastChecked) {
		c.lastCheckRevision.CompareAndSwap(lastChecked, &rev)
	}

	return nil
}
