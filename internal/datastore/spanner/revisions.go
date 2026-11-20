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
	schemaHashStmt        = spanner.NewStatement("SELECT COALESCE((SELECT schema_hash FROM schema_revision WHERE name = 'current' ORDER BY timestamp DESC LIMIT 1), b'')")
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

func (sd *spannerDatastore) nowOnly(ctx context.Context) (datastore.Revision, error) {
	var timestamp time.Time
	if err := sd.client.Single().
		WithTimestampBound(spanner.ExactStaleness(sd.config.followerReadDelay)).
		Query(ctx, nowStmt).
		Do(func(r *spanner.Row) error {
			return r.Columns(&timestamp)
		}); err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevision, err)
	}
	return revisions.NewForTime(timestamp), nil
}

// schemaHashAtRevision returns the stored schema hash as of rev. It returns an empty hash if
// no schema was stored at rev, including when rev precedes the creation of the
// schema_revision table (commonly hit immediately after a fresh migration).
func (sd *spannerDatastore) schemaHashAtRevision(ctx context.Context, rev revisions.WithTimestampRevision) (string, error) {
	var schemaHash []byte
	err := sd.client.Single().
		WithTimestampBound(spanner.ReadTimestamp(time.Unix(0, rev.TimestampNanoSec()))).
		Query(ctx, schemaHashStmt).
		Do(func(r *spanner.Row) error {
			return r.Columns(&schemaHash)
		})
	if err == nil {
		return string(schemaHash), nil
	}
	if spanner.ErrCode(err) == codes.InvalidArgument && strings.Contains(err.Error(), "Table not found: schema_revision") {
		return "", nil
	}
	return "", fmt.Errorf(errRevision, err)
}
