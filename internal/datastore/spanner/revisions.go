package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

var (
	ParseRevisionString = revisions.RevisionParser(revisions.Timestamp)
	nowStmt             = spanner.NewStatement("SELECT CURRENT_TIMESTAMP()")
)

func (sd *spannerDatastore) headRevisionInternal(ctx context.Context) (datastore.Revision, error) {
	now, err := sd.now(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevision, err)
	}

	return revisions.NewForTime(now), nil
}

func (sd *spannerDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return sd.headRevisionInternal(ctx)
}

func (sd *spannerDatastore) now(ctx context.Context) (time.Time, error) {
	var timestamp time.Time
	if err := sd.client.Single().Query(ctx, nowStmt).Do(func(r *spanner.Row) error {
		return r.Columns(&timestamp)
	}); err != nil {
		return timestamp, fmt.Errorf(errRevision, err)
	}
	return timestamp, nil
}

func (sd *spannerDatastore) staleHeadRevision(ctx context.Context) (datastore.Revision, error) {
	var timestamp time.Time
	if err := sd.client.Single().WithTimestampBound(spanner.ExactStaleness(sd.config.followerReadDelay)).Query(ctx, nowStmt).Do(func(r *spanner.Row) error {
		return r.Columns(&timestamp)
	}); err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevision, err)
	}
	return revisions.NewForTime(timestamp), nil
}
