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
	bulkexportv1 "github.com/authzed/spicedb/pkg/proto/bulkexport/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const defaultBatchSize = 1000

// NewBulkExportServer creates a new BulkExportService server.
func NewBulkExportServer(ds datastore.Datastore) bulkexportv1.BulkExportServiceServer {
	return &bulkExportServer{ds: ds}
}

type bulkExportServer struct {
	bulkexportv1.UnimplementedBulkExportServiceServer
	ds datastore.Datastore
}

func (s *bulkExportServer) PlanBulkExport(ctx context.Context, req *bulkexportv1.PlanBulkExportRequest) (*bulkexportv1.PlanBulkExportResponse, error) {
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
		// Datastore doesn't support partitioning; return single partition.
		return &bulkexportv1.PlanBulkExportResponse{
			Revision: revision.String(),
			Partitions: []*bulkexportv1.ExportPartition{
				{Index: 0},
			},
		}, nil
	}

	ranges, err := partitioner.PlanPartitions(ctx, revision, desiredCount)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to plan partitions: %v", err)
	}

	return &bulkexportv1.PlanBulkExportResponse{
		Revision:   revision.String(),
		Partitions: partitionRangesToProto(ranges),
	}, nil
}

func (s *bulkExportServer) StreamBulkExport(req *bulkexportv1.StreamBulkExportRequest, stream grpc.ServerStreamingServer[bulkexportv1.StreamBulkExportResponse]) error {
	ctx := stream.Context()

	if req.Revision == "" {
		return status.Error(codes.InvalidArgument, "revision is required")
	}

	revision, err := s.ds.RevisionFromString(req.Revision)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid revision: %v", err)
	}

	if err := s.ds.CheckRevision(ctx, revision); err != nil {
		return status.Errorf(codes.FailedPrecondition, "revision expired: %v", err)
	}

	reader := s.ds.SnapshotReader(revision)

	batchSize := uint64(req.BatchSize)
	if batchSize == 0 {
		batchSize = defaultBatchSize
	}

	// Build query options.
	opts := []dsoptions.QueryOptionsOption{
		dsoptions.WithSort(dsoptions.ByResource),
		dsoptions.WithQueryShape(queryshape.Varying),
	}

	// Determine lower bound: cursor (resume) takes priority over partition lower bound.
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

	// Upper bound from partition.
	if req.Partition != nil && req.Partition.UpperBound != nil {
		cur := partitionBoundToCursor(req.Partition.UpperBound)
		opts = append(opts, dsoptions.WithBefore(cur))
	}

	iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{}, opts...)
	if err != nil {
		return status.Errorf(codes.Internal, "query failed: %v", err)
	}

	batch := make([]*bulkexportv1.ExportedRelationship, 0, batchSize)
	var last tuple.Relationship

	for rel, err := range iter {
		if err != nil {
			return status.Errorf(codes.Internal, "iteration error: %v", err)
		}

		batch = append(batch, &bulkexportv1.ExportedRelationship{
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
			if err := sendExportBatch(stream, batch, last); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	// Send remaining partial batch.
	if len(batch) > 0 {
		if err := sendExportBatch(stream, batch, last); err != nil {
			return err
		}
	}

	return nil
}

func sendExportBatch(stream grpc.ServerStreamingServer[bulkexportv1.StreamBulkExportResponse], batch []*bulkexportv1.ExportedRelationship, last tuple.Relationship) error {
	return stream.Send(&bulkexportv1.StreamBulkExportResponse{
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

func partitionRangesToProto(ranges []datastore.PartitionRange) []*bulkexportv1.ExportPartition {
	partitions := make([]*bulkexportv1.ExportPartition, len(ranges))
	for i, r := range ranges {
		partitions[i] = &bulkexportv1.ExportPartition{
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

func cursorToPartitionBound(c dsoptions.Cursor) *bulkexportv1.PartitionBound {
	rel := dsoptions.ToRelationship(c)
	if rel == nil {
		return nil
	}
	return &bulkexportv1.PartitionBound{
		Namespace:        rel.Resource.ObjectType,
		ObjectId:         rel.Resource.ObjectID,
		Relation:         rel.Resource.Relation,
		UsersetNamespace: rel.Subject.ObjectType,
		UsersetObjectId:  rel.Subject.ObjectID,
		UsersetRelation:  rel.Subject.Relation,
	}
}

func partitionBoundToCursor(b *bulkexportv1.PartitionBound) dsoptions.Cursor {
	rel := tuple.Relationship{
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
	}
	return dsoptions.ToCursor(rel)
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
