//go:build datastore

package spanner

import (
	"context"
	"fmt"
	"strings"
	"testing"

	admin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"

	"github.com/authzed/spicedb/pkg/datastore/test"
)

func init() {
	test.RegisterSchemaSnapshotter(Engine, snapshotSpannerSchema)
}

// snapshotSpannerSchema captures the schema of a Spanner database as the DDL
// statements the database itself reports, which is the whole schema: tables,
// columns, indexes and row deletion policies.
func snapshotSpannerSchema(ctx context.Context, _ testing.TB, uri string) (string, error) {
	adminClient, err := admin.NewDatabaseAdminClient(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create the spanner admin client: %w", err)
	}
	defer func() { _ = adminClient.Close() }()

	resp, err := adminClient.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{Database: uri})
	if err != nil {
		return "", fmt.Errorf("failed to read the spanner schema: %w", err)
	}

	// The statements come back in dependency order, which is stable for a given
	// schema, so they are joined as reported rather than sorted.
	return strings.Join(resp.GetStatements(), "\n"), nil
}
