package crdb

import (
	"context"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

func (cds *crdbDatastore) WriteNamespace(ctx context.Context, newConfig *pb.NamespaceDefinition) (uint64, error) {
	return 0, nil
}

func (cds *crdbDatastore) ReadNamespace(ctx context.Context, nsName string) (*pb.NamespaceDefinition, uint64, error) {
	return nil, 0, nil
}

func (cds *crdbDatastore) DeleteNamespace(ctx context.Context, nsName string) (uint64, error) {
	return 0, nil
}
