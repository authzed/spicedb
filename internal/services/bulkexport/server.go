package bulkexport

import (
	"google.golang.org/grpc"

	v1 "github.com/authzed/spicedb/internal/services/bulkexport/v1"
	"github.com/authzed/spicedb/pkg/datastore"
	bulkexportv1 "github.com/authzed/spicedb/pkg/proto/bulkexport/v1"
)

// RegisterGrpcServices registers the partitioned bulk export service with the specified server.
func RegisterGrpcServices(srv *grpc.Server, ds datastore.Datastore) {
	srv.RegisterService(&bulkexportv1.BulkExportService_ServiceDesc, v1.NewBulkExportServer(ds))
}
