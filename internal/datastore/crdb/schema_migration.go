package crdb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

func (cds *crdbDatastore) ReadSchemaMigrationBoundary(ctx context.Context) (datastore.SchemaMigrationBoundary, error) {
	var source []byte
	err := cds.readPool.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error { return row.Scan(&source) },
		`SELECT hash FROM schema_revision WHERE name = $1`, common.SchemaMigrationMarkerName)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if string(source) == common.SchemaMigrationSourceUnified {
		return datastore.SchemaMigrationBoundaryFunc(func(datastore.Revision) bool { return true }), nil
	}
	encoded, ok := strings.CutPrefix(string(source), common.SchemaMigrationSourceLegacy+":")
	if !ok {
		return nil, fmt.Errorf("invalid schema migration source %q", source)
	}
	boundary, err := revisions.HLCRevisionFromString(encoded)
	if err != nil {
		return nil, err
	}
	return datastore.SchemaMigrationBoundaryFunc(func(rev datastore.Revision) bool { return !rev.LessThan(boundary) }), nil
}
