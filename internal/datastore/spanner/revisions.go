package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
)

func (sd spannerDatastore) headRevisionInternal(ctx context.Context) (revision.Decimal, error) {
	now, err := sd.now(ctx)
	if err != nil {
		return revision.NoRevision, fmt.Errorf(errRevision, err)
	}

	return revisionFromTimestamp(now), nil
}

func (sd spannerDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return sd.headRevisionInternal(ctx)
}

func (sd spannerDatastore) now(ctx context.Context) (time.Time, error) {
	ctx, span := tracer.Start(ctx, "now")
	defer span.End()

	var timestamp time.Time
	if err := sd.client.Single().Query(ctx, spanner.NewStatement("SELECT CURRENT_TIMESTAMP()")).Do(func(r *spanner.Row) error {
		return r.Columns(&timestamp)
	}); err != nil {
		return time.Time{}, err
	}

	return timestamp, nil
}

func revisionFromTimestamp(t time.Time) revision.Decimal {
	return revision.NewFromDecimal(decimal.NewFromInt(t.UnixNano()))
}

func timestampFromRevision(r revision.Decimal) time.Time {
	return time.Unix(0, r.IntPart())
}
