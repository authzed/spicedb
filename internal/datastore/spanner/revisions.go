package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
)

func (sd spannerDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	now, err := sd.now(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevision, err)
	}

	return revisionFromTimestamp(now), nil
}

func (sd spannerDatastore) now(ctx context.Context) (time.Time, error) {
	ctx, span := tracer.Start(ctx, "now")
	defer span.End()

	iter := sd.client.Single().Query(ctx, spanner.NewStatement("SELECT CURRENT_TIMESTAMP()"))
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		return time.Time{}, err
	}

	var timestamp time.Time
	if err := row.Columns(&timestamp); err != nil {
		return time.Time{}, err
	}

	return timestamp, nil
}

func revisionFromTimestamp(t time.Time) datastore.Revision {
	return decimal.NewFromInt(t.UnixNano())
}

func timestampFromRevision(r datastore.Revision) time.Time {
	return time.Unix(0, r.IntPart())
}
