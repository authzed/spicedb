package crdb

import (
	"context"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

func (cds *crdbDatastore) WriteTuples(ctx context.Context, preconditions []*pb.RelationTuple, mutations []*pb.RelationTupleUpdate) (uint64, error) {
	return 0, nil
}
