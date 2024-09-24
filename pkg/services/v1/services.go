package v1

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	servicesv1 "github.com/authzed/spicedb/internal/services/v1"
	"github.com/authzed/spicedb/pkg/datastore"
)

// BulkExport implements the BulkExportRelationships API functionality. Given a datastore.Datastore, it will
// export stream via the sender all relationships matched by the incoming request.
// If no cursor is provided, it will fallback to the provided revision.
func BulkExport(ctx context.Context, ds datastore.ReadOnlyDatastore, batchSize uint64, req *v1.BulkExportRelationshipsRequest, fallbackRevision datastore.Revision, sender func(response *v1.BulkExportRelationshipsResponse) error) error {
	return servicesv1.BulkExport(ctx, ds, batchSize, req, fallbackRevision, sender)
}
