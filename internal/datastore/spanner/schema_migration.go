package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

func (sd *spannerDatastore) ReadSchemaMigrationBoundary(ctx context.Context) (datastore.SchemaMigrationBoundary, error) {
	row, err := sd.client.Single().ReadRow(ctx, "schema_revision", spanner.Key{common.SchemaMigrationMarkerName}, []string{"schema_hash", "timestamp"})
	if spanner.ErrCode(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var source []byte
	var timestamp time.Time
	if err := row.Columns(&source, &timestamp); err != nil {
		return nil, err
	}
	if string(source) == common.SchemaMigrationSourceUnified {
		return datastore.SchemaMigrationBoundaryFunc(func(datastore.Revision) bool { return true }), nil
	}
	if string(source) != common.SchemaMigrationSourceLegacy {
		return nil, fmt.Errorf("invalid schema migration source %q", source)
	}
	boundary := revisions.NewForTime(timestamp)
	return datastore.SchemaMigrationBoundaryFunc(func(rev datastore.Revision) bool { return !rev.LessThan(boundary) }), nil
}
