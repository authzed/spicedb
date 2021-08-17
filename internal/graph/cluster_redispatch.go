package graph

import (
	"context"
	"fmt"
	"strconv"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/authzed/spicedb/pkg/zookie"
)

const (
	errCheckRedispatch  = "error remotely dispatching check request: %w"
	errExpandRedispatch = "error remotely dispatching expand request: %w"
	errLookupRedispatch = "error remotely dispatching lookup request: %w"
)

type clusterClient interface {
	Check(ctx context.Context, req *v0.CheckRequest, opts ...grpc.CallOption) (*v0.CheckResponse, error)
	Expand(ctx context.Context, req *v0.ExpandRequest, opts ...grpc.CallOption) (*v0.ExpandResponse, error)
	Lookup(ctx context.Context, req *v0.LookupRequest, opts ...grpc.CallOption) (*v0.LookupResponse, error)
}

// NewClusterDispatcher creates a dispatcher implementation that uses the provided client
// to dispatch requests to peer nodes in the cluster.
func NewClusterDispatcher(client clusterClient, depthRemainingHeader, forcedRevisionHeader string) Dispatcher {
	return &clusterDispatcher{client, depthRemainingHeader, forcedRevisionHeader}
}

type clusterDispatcher struct {
	clusterClient        clusterClient
	depthRemainingHeader string
	forcedRevisionHeader string
}

func (cr *clusterDispatcher) Check(ctx context.Context, req CheckRequest) CheckResult {
	ctx = cr.addDepthRemaining(ctx, req.DepthRemaining)
	ctx = cr.addForcedRevision(ctx, req.AtRevision)

	resp, err := cr.clusterClient.Check(ctx, &v0.CheckRequest{
		TestUserset: req.Start,
		User:        &v0.User{UserOneof: &v0.User_Userset{Userset: req.Goal}},
		AtRevision:  zookie.NewFromRevision(req.AtRevision),
	})
	if err != nil {
		return CheckResult{
			Err: fmt.Errorf(errCheckRedispatch, err),
		}
	}

	return CheckResult{
		IsMember: resp.IsMember,
	}
}

func (cr *clusterDispatcher) Expand(ctx context.Context, req ExpandRequest) ExpandResult {
	ctx = cr.addDepthRemaining(ctx, req.DepthRemaining)
	ctx = cr.addForcedRevision(ctx, req.AtRevision)

	resp, err := cr.clusterClient.Expand(ctx, &v0.ExpandRequest{
		Userset:    req.Start,
		AtRevision: zookie.NewFromRevision(req.AtRevision),
	})
	if err != nil {
		return ExpandResult{
			Err: fmt.Errorf(errExpandRedispatch, err),
		}
	}

	return ExpandResult{
		Tree: resp.TreeNode,
	}
}

func (cr *clusterDispatcher) Lookup(ctx context.Context, req LookupRequest) LookupResult {
	ctx = cr.addDepthRemaining(ctx, req.DepthRemaining)
	ctx = cr.addForcedRevision(ctx, req.AtRevision)

	resp, err := cr.clusterClient.Lookup(ctx, &v0.LookupRequest{
		ObjectRelation: req.StartRelation,
		User:           req.TargetONR,
		Limit:          uint32(req.Limit),
		AtRevision:     zookie.NewFromRevision(req.AtRevision),
	})
	if err != nil {
		return LookupResult{
			Err: fmt.Errorf(errLookupRedispatch, err),
		}
	}

	resolvedONRs := make([]*v0.ObjectAndRelation, 0, len(resp.ResolvedObjectIds))
	for _, objectID := range resp.ResolvedObjectIds {
		resolvedONRs = append(resolvedONRs, &v0.ObjectAndRelation{
			Namespace: req.StartRelation.Namespace,
			ObjectId:  objectID,
			Relation:  req.StartRelation.Relation,
		})
	}

	return LookupResult{
		ResolvedObjects: resolvedONRs,
	}
}

func (cr *clusterDispatcher) addDepthRemaining(ctx context.Context, depthRemaining uint16) context.Context {
	depthRemainingStr := strconv.Itoa(int(depthRemaining))
	return metadata.AppendToOutgoingContext(ctx, cr.depthRemainingHeader, depthRemainingStr)
}

func (cr *clusterDispatcher) addForcedRevision(ctx context.Context, revision decimal.Decimal) context.Context {
	revisionStr := revision.String()
	return metadata.AppendToOutgoingContext(ctx, cr.forcedRevisionHeader, revisionStr)
}

// Always verify that we implement the interface
var _ Dispatcher = &clusterDispatcher{}
