package v1

import (
	"context"
	"encoding/base64"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	pev1 "github.com/authzed/spicedb/pkg/proto/partitionedexport/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const defaultBatchSize = 1000

// NewPartitionedExportServer creates a new PartitionedExportService server.
func NewPartitionedExportServer(ds datastore.Datastore) pev1.PartitionedExportServiceServer {
	return &partitionedExportServer{ds: ds}
}

type partitionedExportServer struct {
	pev1.UnimplementedPartitionedExportServiceServer
	ds datastore.Datastore
}

func (s *partitionedExportServer) PlanPartitionedExport(ctx context.Context, req *pev1.PlanPartitionedExportRequest) (*pev1.PlanPartitionedExportResponse, error) {
	desiredCount := req.DesiredPartitions
	if desiredCount == 0 {
		desiredCount = 1
	}

	revision, err := s.ds.OptimizedRevision(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get revision: %v", err)
	}

	partitioner := datastore.UnwrapAs[datastore.BulkExportPartitioner](s.ds)
	if partitioner == nil {
		return &pev1.PlanPartitionedExportResponse{
			Revision: revision.String(),
			Partitions: []*pev1.ExportPartition{
				{Index: 0},
			},
		}, nil
	}

	ranges, err := partitioner.PlanPartitions(ctx, revision, desiredCount)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to plan partitions: %v", err)
	}

	return &pev1.PlanPartitionedExportResponse{
		Revision:   revision.String(),
		Partitions: partitionRangesToProto(ranges),
	}, nil
}

func (s *partitionedExportServer) StreamPartitionedExport(req *pev1.StreamPartitionedExportRequest, stream grpc.ServerStreamingServer[pev1.StreamPartitionedExportResponse]) error {
	return StreamPartitionedExportToSender(stream.Context(), s.ds, req, stream.Send)
}

// StreamPartitionedExportToSender implements the StreamPartitionedExport logic. Given a datastore,
// it will stream via the sender all relationships matched by the partition in the request.
// If no cursor is provided, it will start from the partition's lower bound.
func StreamPartitionedExportToSender(ctx context.Context, ds datastore.Datastore, req *pev1.StreamPartitionedExportRequest, sender func(response *pev1.StreamPartitionedExportResponse) error) error {
	if req.Revision == "" {
		return status.Error(codes.InvalidArgument, "revision is required")
	}

	revision, err := ds.RevisionFromString(req.Revision)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid revision: %v", err)
	}

	if err := ds.CheckRevision(ctx, revision); err != nil {
		return status.Errorf(codes.FailedPrecondition, "revision expired: %v", err)
	}

	reader := ds.SnapshotReader(revision)

	batchSize := uint64(req.BatchSize)
	if batchSize == 0 {
		batchSize = defaultBatchSize
	}

	opts := []dsoptions.QueryOptionsOption{
		dsoptions.WithSort(dsoptions.ByResource),
		dsoptions.WithQueryShape(queryshape.Varying),
	}

	if req.Cursor != nil && *req.Cursor != "" {
		cur, err := decodeCursor(*req.Cursor)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid cursor: %v", err)
		}
		opts = append(opts, dsoptions.WithAfter(cur))
	} else if req.Partition != nil && req.Partition.LowerBound != nil {
		cur := partitionBoundToCursor(req.Partition.LowerBound)
		opts = append(opts, dsoptions.WithAfter(cur))
	}

	if req.Partition != nil && req.Partition.UpperBound != nil {
		cur := partitionBoundToCursor(req.Partition.UpperBound)
		opts = append(opts, dsoptions.WithBeforeOrEqual(cur))
	}

	iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{}, opts...)
	if err != nil {
		return status.Errorf(codes.Internal, "query failed: %v", err)
	}

	batch := make([]*pev1.ExportedRelationship, 0, batchSize)
	var last tuple.Relationship

	for rel, err := range iter {
		if err != nil {
			return status.Errorf(codes.Internal, "iteration error: %v", err)
		}

		batch = append(batch, &pev1.ExportedRelationship{
			ResourceType:    rel.Resource.ObjectType,
			ResourceId:      rel.Resource.ObjectID,
			Relation:        rel.Resource.Relation,
			SubjectType:     rel.Subject.ObjectType,
			SubjectId:       rel.Subject.ObjectID,
			SubjectRelation: rel.Subject.Relation,
			CaveatName:      caveatName(rel),
			CaveatContext:   caveatContext(rel),
		})
		last = rel

		if uint64(len(batch)) == batchSize {
			if err := sendBatch(sender, batch, last); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := sendBatch(sender, batch, last); err != nil {
			return err
		}
	}

	return nil
}

func sendBatch(sender func(response *pev1.StreamPartitionedExportResponse) error, batch []*pev1.ExportedRelationship, last tuple.Relationship) error {
	return sender(&pev1.StreamPartitionedExportResponse{
		AfterResultCursor: encodeCursor(last),
		Relationships:     batch,
	})
}

func encodeCursor(rel tuple.Relationship) string {
	return base64.StdEncoding.EncodeToString([]byte(tuple.MustString(rel)))
}

func decodeCursor(encoded string) (dsoptions.Cursor, error) {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	rel, err := tuple.Parse(string(data))
	if err != nil {
		return nil, fmt.Errorf("invalid cursor content: %w", err)
	}

	return dsoptions.ToCursor(rel), nil
}

func partitionRangesToProto(ranges []datastore.PartitionRange) []*pev1.ExportPartition {
	partitions := make([]*pev1.ExportPartition, len(ranges))
	for i, r := range ranges {
		partitions[i] = &pev1.ExportPartition{
			Index: uint32(i),
		}
		if r.LowerBound != nil {
			partitions[i].LowerBound = cursorToPartitionBound(r.LowerBound)
		}
		if r.UpperBound != nil {
			partitions[i].UpperBound = cursorToPartitionBound(r.UpperBound)
		}
	}
	return partitions
}

func cursorToPartitionBound(c dsoptions.Cursor) *pev1.PartitionBound {
	rel := dsoptions.ToRelationship(c)
	if rel == nil {
		return nil
	}
	return &pev1.PartitionBound{
		Namespace:        rel.Resource.ObjectType,
		ObjectId:         rel.Resource.ObjectID,
		Relation:         rel.Resource.Relation,
		UsersetNamespace: rel.Subject.ObjectType,
		UsersetObjectId:  rel.Subject.ObjectID,
		UsersetRelation:  rel.Subject.Relation,
	}
}

func partitionBoundToCursor(b *pev1.PartitionBound) dsoptions.Cursor {
	return dsoptions.ToCursor(tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ObjectAndRelation{
				ObjectType: b.Namespace,
				ObjectID:   b.ObjectId,
				Relation:   b.Relation,
			},
			Subject: tuple.ObjectAndRelation{
				ObjectType: b.UsersetNamespace,
				ObjectID:   b.UsersetObjectId,
				Relation:   b.UsersetRelation,
			},
		},
	})
}

func caveatName(rel tuple.Relationship) *string {
	if rel.OptionalCaveat != nil && rel.OptionalCaveat.CaveatName != "" {
		return &rel.OptionalCaveat.CaveatName
	}
	return nil
}

func caveatContext(rel tuple.Relationship) []byte {
	if rel.OptionalCaveat != nil && rel.OptionalCaveat.Context != nil {
		data, err := rel.OptionalCaveat.Context.MarshalJSON()
		if err != nil {
			return nil
		}
		return data
	}
	return nil
}
