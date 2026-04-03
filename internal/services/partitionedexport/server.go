package partitionedexport

import (
	"google.golang.org/grpc"

	v1 "github.com/authzed/spicedb/internal/services/partitionedexport/v1"
	"github.com/authzed/spicedb/pkg/datastore"
	pev1 "github.com/authzed/spicedb/pkg/proto/partitionedexport/v1"
)

// RegisterGrpcServices registers the partitioned export service with the specified server.
func RegisterGrpcServices(srv *grpc.Server, ds datastore.Datastore) {
	srv.RegisterService(&pev1.PartitionedExportService_ServiceDesc, v1.NewPartitionedExportServer(ds))
}
