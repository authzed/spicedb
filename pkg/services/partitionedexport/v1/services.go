package v1

import (
	"context"

	servicesv1 "github.com/authzed/spicedb/internal/services/partitionedexport/v1"
	"github.com/authzed/spicedb/pkg/datastore"
	pev1 "github.com/authzed/spicedb/pkg/proto/partitionedexport/v1"
)

// PlanPartitionedExport plans a partitioned export by splitting the relationship table
// into non-overlapping key ranges for parallel streaming. The returned partitions
// and revision should be passed to StreamPartitionedExport.
func PlanPartitionedExport(ctx context.Context, ds datastore.Datastore, req *pev1.PlanPartitionedExportRequest) (*pev1.PlanPartitionedExportResponse, error) {
	srv := servicesv1.NewPartitionedExportServer(ds)
	return srv.PlanPartitionedExport(ctx, req)
}

// StreamPartitionedExport streams relationships for a single partition. The sender callback
// is invoked for each batch of relationships. Use PlanPartitionedExport first to obtain
// partition descriptors and a revision.
func StreamPartitionedExport(ctx context.Context, ds datastore.Datastore, req *pev1.StreamPartitionedExportRequest, sender func(response *pev1.StreamPartitionedExportResponse) error) error {
	return servicesv1.StreamPartitionedExportToSender(ctx, ds, req, sender)
}
