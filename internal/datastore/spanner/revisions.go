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
	nowWithHashStmt     = spanner.NewStatement(`
		SELECT
			CURRENT_TIMESTAMP(),
			COALESCE((SELECT schema_hash FROM schema_revision WHERE name = 'current' ORDER BY timestamp DESC LIMIT 1), b'')
	`)
)

func (sd *spannerDatastore) headRevisionInternal(ctx context.Context) (datastore.Revision, datastore.SchemaHash, error) {
	now, schemaHash, err := sd.nowWithHash(ctx)
	if err != nil {
		return datastore.NoRevision, "", fmt.Errorf(errRevision, err)
	}

	return revisions.NewForTime(now), schemaHash, nil
}

func (sd *spannerDatastore) HeadRevision(ctx context.Context) (datastore.Revision, datastore.SchemaHash, error) {
	return sd.headRevisionInternal(ctx)
}

func (sd *spannerDatastore) nowWithHash(ctx context.Context) (time.Time, datastore.SchemaHash, error) {
	var timestamp time.Time
	var schemaHash []byte
	if err := sd.client.Single().Query(ctx, nowWithHashStmt).Do(func(r *spanner.Row) error {
		return r.Columns(&timestamp, &schemaHash)
	}); err != nil {
		return timestamp, "", fmt.Errorf(errRevision, err)
	}
	return timestamp, datastore.SchemaHash(schemaHash), nil
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
