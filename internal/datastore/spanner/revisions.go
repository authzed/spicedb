package spanner

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

var (
	ParseRevisionString   = revisions.RevisionParser(revisions.Timestamp)
	nowWithSchemaHashStmt = spanner.NewStatement("SELECT CURRENT_TIMESTAMP(), COALESCE((SELECT schema_hash FROM schema_revision WHERE name = 'current' ORDER BY timestamp DESC LIMIT 1), b'')")
	nowStmt               = spanner.NewStatement("SELECT CURRENT_TIMESTAMP()")
)

func (sd *spannerDatastore) HeadRevision(ctx context.Context) (datastore.RevisionWithSchemaHash, error) {
	now, schemaHash, err := sd.nowWithHash(ctx)
	if err != nil {
		return datastore.RevisionWithSchemaHash{}, fmt.Errorf(errRevision, err)
	}

	return datastore.RevisionWithSchemaHash{Revision: revisions.NewForTime(now), SchemaHash: schemaHash}, nil
}

func (sd *spannerDatastore) nowWithHash(ctx context.Context) (time.Time, string, error) {
	var timestamp time.Time
	var schemaHash []byte
	if err := sd.client.Single().Query(ctx, nowWithSchemaHashStmt).Do(func(r *spanner.Row) error {
		return r.Columns(&timestamp, &schemaHash)
	}); err != nil {
		return time.Time{}, "", fmt.Errorf(errRevision, err)
	}
	return timestamp, string(schemaHash), nil
}

func (sd *spannerDatastore) staleHeadRevision(ctx context.Context) (datastore.Revision, string, error) {
	var timestamp time.Time
	var schemaHash []byte
	err := sd.client.Single().
		WithTimestampBound(spanner.ExactStaleness(sd.config.followerReadDelay)).
		Query(ctx, nowWithSchemaHashStmt).
		Do(func(r *spanner.Row) error {
			return r.Columns(&timestamp, &schemaHash)
		})
	if err == nil {
		return revisions.NewForTime(timestamp), string(schemaHash), nil
	}

	// The stale read timestamp can precede the creation of the schema_revision
	// table when the followerReadDelay is larger than the time elapsed since
	// migrations ran (commonly hit immediately after a fresh migration). At
	// that past time, no schema existed, so returning an empty hash is
	// semantically correct; fall back to a stale read of just the timestamp.
	if spanner.ErrCode(err) == codes.InvalidArgument && strings.Contains(err.Error(), "Table not found: schema_revision") {
		if err := sd.client.Single().
			WithTimestampBound(spanner.ExactStaleness(sd.config.followerReadDelay)).
			Query(ctx, nowStmt).
			Do(func(r *spanner.Row) error {
				return r.Columns(&timestamp)
			}); err != nil {
			return datastore.NoRevision, "", fmt.Errorf(errRevision, err)
		}
		return revisions.NewForTime(timestamp), "", nil
	}
	return datastore.NoRevision, "", fmt.Errorf(errRevision, err)
}
