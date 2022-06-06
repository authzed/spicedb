package consistency

import (
	"context"
	"errors"
	"fmt"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/services/serviceerrors"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

type hasConsistency interface {
	GetConsistency() *v1.Consistency
}

type ctxKeyType struct{}

var revisionKey ctxKeyType = struct{}{}

var errInvalidZedToken = errors.New("invalid revision requested")

type revisionHandle struct {
	revision datastore.Revision
}

// ContextWithHandle adds a placeholder to a context that will later be
// filled by the revision
func ContextWithHandle(ctx context.Context) context.Context {
	return context.WithValue(ctx, revisionKey, &revisionHandle{})
}

// RevisionFromContext reads the selected revision out of a context.Context and returns nil if it
// does not exist.
func RevisionFromContext(ctx context.Context) *decimal.Decimal {
	if c := ctx.Value(revisionKey); c != nil {
		handle := c.(*revisionHandle)
		return &handle.revision
	}
	return nil
}

// MustRevisionFromContext reads the selected revision out of a context.Context, computes a zedtoken
// from it, and panics if it has not been set on the context.
func MustRevisionFromContext(ctx context.Context) (decimal.Decimal, *v1.ZedToken) {
	rev := RevisionFromContext(ctx)
	if rev == nil {
		panic("consistency middleware did not inject revision")
	}

	return *rev, zedtoken.NewFromRevision(*rev)
}

// AddRevisionToContext adds a revision to the given context, based on the consistency block found
// in the given request (if applicable).
func AddRevisionToContext(ctx context.Context, req interface{}, ds datastore.Datastore) error {
	switch req := req.(type) {
	case hasConsistency:
		return addRevisionToContextFromConsistency(ctx, req, ds)
	default:
		return addHeadRevision(ctx, ds)
	}
}

// addHeadRevision sets the value of the revision in the context to the current head revision in the datastore
func addHeadRevision(ctx context.Context, ds datastore.Datastore) error {
	handle := ctx.Value(revisionKey)
	if handle == nil {
		return nil
	}

	revision, err := ds.HeadRevision(ctx)
	if err != nil {
		return rewriteDatastoreError(ctx, err)
	}
	handle.(*revisionHandle).revision = revision
	return nil
}

// addRevisionToContextFromConsistency adds a revision to the given context, based on the consistency block found
// in the given request (if applicable).
func addRevisionToContextFromConsistency(ctx context.Context, req hasConsistency, ds datastore.Datastore) error {
	handle := ctx.Value(revisionKey)
	if handle == nil {
		return nil
	}

	var revision decimal.Decimal
	consistency := req.GetConsistency()

	switch {
	case consistency == nil || consistency.GetMinimizeLatency():
		// Minimize Latency: Use the datastore's current revision, whatever it may be.
		databaseRev, err := ds.OptimizedRevision(ctx)
		if err != nil {
			return rewriteDatastoreError(ctx, err)
		}
		revision = databaseRev

	case consistency.GetFullyConsistent():
		// Fully Consistent: Use the datastore's synchronized revision.
		databaseRev, err := ds.HeadRevision(ctx)
		if err != nil {
			return rewriteDatastoreError(ctx, err)
		}
		revision = databaseRev

	case consistency.GetAtLeastAsFresh() != nil:
		// At least as fresh as: Pick one of the datastore's revision and that specified, which
		// ever is later.
		picked, err := pickBestRevision(ctx, consistency.GetAtLeastAsFresh(), ds)
		if err != nil {
			return rewriteDatastoreError(ctx, err)
		}
		revision = picked

	case consistency.GetAtExactSnapshot() != nil:
		// Exact snapshot: Use the revision as encoded in the zed token.
		requestedRev, err := zedtoken.DecodeRevision(consistency.GetAtExactSnapshot())
		if err != nil {
			return errInvalidZedToken
		}

		err = ds.CheckRevision(ctx, requestedRev)
		if err != nil {
			return rewriteDatastoreError(ctx, err)
		}

		revision = requestedRev

	default:
		return fmt.Errorf("missing handling of consistency case in %v", consistency)
	}

	handle.(*revisionHandle).revision = revision
	return nil
}

var bypassServiceWhitelist = map[string]struct{}{
	"/grpc.reflection.v1alpha.ServerReflection/": {},
	"/grpc.health.v1.Health/":                    {},
}

// UnaryServerInterceptor returns a new unary server interceptor that performs per-request exchange of
// the specified consistency configuration for the revision at which to perform the request.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		for bypass := range bypassServiceWhitelist {
			if strings.HasPrefix(info.FullMethod, bypass) {
				return handler(ctx, req)
			}
		}
		ds := datastoremw.MustFromContext(ctx)
		newCtx := ContextWithHandle(ctx)
		if err := AddRevisionToContext(newCtx, req, ds); err != nil {
			return nil, err
		}

		return handler(newCtx, req)
	}
}

// StreamServerInterceptor returns a new stream server interceptor that performs per-request exchange of
// the specified consistency configuration for the revision at which to perform the request.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		for bypass := range bypassServiceWhitelist {
			if strings.HasPrefix(info.FullMethod, bypass) {
				return handler(srv, stream)
			}
		}
		wrapper := &recvWrapper{stream, ContextWithHandle(stream.Context())}
		return handler(srv, wrapper)
	}
}

type recvWrapper struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *recvWrapper) Context() context.Context {
	return s.ctx
}

func (s *recvWrapper) RecvMsg(m interface{}) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	ds := datastoremw.MustFromContext(s.ctx)

	if err := AddRevisionToContext(s.ctx, m, ds); err != nil {
		return err
	}

	return nil
}

func pickBestRevision(ctx context.Context, requested *v1.ZedToken, ds datastore.Datastore) (decimal.Decimal, error) {
	// Calculate a revision as we see fit
	databaseRev, err := ds.OptimizedRevision(ctx)
	if err != nil {
		return decimal.Zero, err
	}

	if requested != nil {
		requestedRev, err := zedtoken.DecodeRevision(requested)
		if err != nil {
			return decimal.Zero, errInvalidZedToken
		}

		if requestedRev.GreaterThan(databaseRev) {
			return requestedRev, nil
		}
		return databaseRev, nil
	}

	return databaseRev, nil
}

func rewriteDatastoreError(ctx context.Context, err error) error {
	switch {
	case errors.As(err, &datastore.ErrInvalidRevision{}):
		return status.Errorf(codes.OutOfRange, "invalid revision: %s", err)

	case errors.As(err, &datastore.ErrReadOnly{}):
		return serviceerrors.ErrServiceReadOnly

	default:
		log.Ctx(ctx).Err(err)
		return err
	}
}
