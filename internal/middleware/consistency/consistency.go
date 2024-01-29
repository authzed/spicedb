package consistency

import (
	"context"
	"errors"
	"fmt"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

var ConsistentyCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "middleware",
	Name:      "consistency_assigned_total",
	Help:      "Count of the consistencies used per request",
}, []string{"method", "source"})

type hasConsistency interface{ GetConsistency() *v1.Consistency }

type hasOptionalCursor interface{ GetOptionalCursor() *v1.Cursor }

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

// RevisionFromContext reads the selected revision out of a context.Context, computes a zedtoken
// from it, and returns an error if it has not been set on the context.
func RevisionFromContext(ctx context.Context) (datastore.Revision, *v1.ZedToken, error) {
	if c := ctx.Value(revisionKey); c != nil {
		handle := c.(*revisionHandle)
		rev := handle.revision
		if rev != nil {
			ds := datastoremw.FromContext(ctx)
			if ds == nil {
				return nil, nil, spiceerrors.MustBugf("consistency middleware did not inject datastore")
			}

			zedToken, err := zedtoken.NewFromRevision(ctx, rev, ds)
			if err != nil {
				return nil, nil, err
			}

			return rev, zedToken, nil
		}
	}

	return nil, nil, fmt.Errorf("consistency middleware did not inject revision")
}

type MismatchingTokenOption int

const (
	TreatMismatchingTokensAsFullConsistency MismatchingTokenOption = iota

	TreatMismatchingTokensAsMinLatency

	TreatMismatchingTokensAsError
)

// AddRevisionToContext adds a revision to the given context, based on the consistency block found
// in the given request (if applicable).
func AddRevisionToContext(ctx context.Context, req interface{}, ds datastore.Datastore, option MismatchingTokenOption) error {
	switch req := req.(type) {
	case hasConsistency:
		return addRevisionToContextFromConsistency(ctx, req, ds, option)
	default:
		return nil
	}
}

// addRevisionToContextFromConsistency adds a revision to the given context, based on the consistency block found
// in the given request (if applicable).
func addRevisionToContextFromConsistency(ctx context.Context, req hasConsistency, ds datastore.Datastore, option MismatchingTokenOption) error {
	handle := ctx.Value(revisionKey)
	if handle == nil {
		return nil
	}

	var revision datastore.Revision
	consistency := req.GetConsistency()

	withOptionalCursor, hasOptionalCursor := req.(hasOptionalCursor)

	switch {
	case hasOptionalCursor && withOptionalCursor.GetOptionalCursor() != nil:
		// Always use the revision encoded in the cursor.
		ConsistentyCounter.WithLabelValues("snapshot", "cursor").Inc()

		requestedRev, _, err := cursor.DecodeToDispatchRevision(ctx, withOptionalCursor.GetOptionalCursor(), ds)
		if err != nil {
			return rewriteDatastoreError(ctx, err)
		}

		err = ds.CheckRevision(ctx, requestedRev)
		if err != nil {
			return rewriteDatastoreError(ctx, err)
		}

		revision = requestedRev

	case consistency == nil || consistency.GetMinimizeLatency():
		// Minimize Latency: Use the datastore's current revision, whatever it may be.
		source := "request"
		if consistency == nil {
			source = "server"
		}
		ConsistentyCounter.WithLabelValues("minlatency", source).Inc()

		databaseRev, err := ds.OptimizedRevision(ctx)
		if err != nil {
			return rewriteDatastoreError(ctx, err)
		}
		revision = databaseRev

	case consistency.GetFullyConsistent():
		// Fully Consistent: Use the datastore's synchronized revision.
		ConsistentyCounter.WithLabelValues("full", "request").Inc()

		databaseRev, err := ds.HeadRevision(ctx)
		if err != nil {
			return rewriteDatastoreError(ctx, err)
		}
		revision = databaseRev

	case consistency.GetAtLeastAsFresh() != nil:
		// At least as fresh as: Pick one of the datastore's revision and that specified, which
		// ever is later.
		picked, pickedRequest, err := pickBestRevision(ctx, consistency.GetAtLeastAsFresh(), ds, option)
		if err != nil {
			return rewriteDatastoreError(ctx, err)
		}

		source := "server"
		if pickedRequest {
			source = "request"
		}
		ConsistentyCounter.WithLabelValues("atleast", source).Inc()

		revision = picked

	case consistency.GetAtExactSnapshot() != nil:
		// Exact snapshot: Use the revision as encoded in the zed token.
		ConsistentyCounter.WithLabelValues("snapshot", "request").Inc()

		requestedRev, status, err := zedtoken.DecodeRevision(consistency.GetAtExactSnapshot(), ds)
		if err != nil {
			return errInvalidZedToken
		}

		if status == zedtoken.StatusMismatchedDatastoreID {
			log.Error().Str("zedtoken", consistency.GetAtExactSnapshot().Token).Msg("ZedToken specified references an older datastore but at-exact-snapshot was requested")
			return fmt.Errorf("ZedToken specified references an older datastore but at-exact-snapshot was requested")
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
	"/grpc.reflection.v1.ServerReflection/":      {},
	"/grpc.health.v1.Health/":                    {},
}

// UnaryServerInterceptor returns a new unary server interceptor that performs per-request exchange of
// the specified consistency configuration for the revision at which to perform the request.
func UnaryServerInterceptor(option MismatchingTokenOption) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		for bypass := range bypassServiceWhitelist {
			if strings.HasPrefix(info.FullMethod, bypass) {
				return handler(ctx, req)
			}
		}
		ds := datastoremw.MustFromContext(ctx)
		newCtx := ContextWithHandle(ctx)
		if err := AddRevisionToContext(newCtx, req, ds, option); err != nil {
			return nil, err
		}

		return handler(newCtx, req)
	}
}

// StreamServerInterceptor returns a new stream server interceptor that performs per-request exchange of
// the specified consistency configuration for the revision at which to perform the request.
func StreamServerInterceptor(option MismatchingTokenOption) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		for bypass := range bypassServiceWhitelist {
			if strings.HasPrefix(info.FullMethod, bypass) {
				return handler(srv, stream)
			}
		}
		wrapper := &recvWrapper{stream, ContextWithHandle(stream.Context()), option}
		return handler(srv, wrapper)
	}
}

type recvWrapper struct {
	grpc.ServerStream
	ctx    context.Context
	option MismatchingTokenOption
}

func (s *recvWrapper) Context() context.Context { return s.ctx }

func (s *recvWrapper) RecvMsg(m interface{}) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	ds := datastoremw.MustFromContext(s.ctx)

	return AddRevisionToContext(s.ctx, m, ds, s.option)
}

// pickBestRevision compares the provided ZedToken with the optimized revision of the datastore, and returns the most
// recent one. The boolean return value will be true if the provided ZedToken is the most recent, false otherwise.
func pickBestRevision(ctx context.Context, requested *v1.ZedToken, ds datastore.Datastore, option MismatchingTokenOption) (datastore.Revision, bool, error) {
	// Calculate a revision as we see fit
	databaseRev, err := ds.OptimizedRevision(ctx)
	if err != nil {
		return datastore.NoRevision, false, err
	}

	if requested != nil {
		requestedRev, status, err := zedtoken.DecodeRevision(requested, ds)
		if err != nil {
			return datastore.NoRevision, false, errInvalidZedToken
		}

		if status == zedtoken.StatusMismatchedDatastoreID {
			switch option {
			case TreatMismatchingTokensAsFullConsistency:
				log.Warn().Str("zedtoken", requested.Token).Msg("ZedToken specified references an older datastore and SpiceDB is configured to treat this as a full consistency request")
				headRev, err := ds.HeadRevision(ctx)
				if err != nil {
					return datastore.NoRevision, false, err
				}

				return headRev, false, nil

			case TreatMismatchingTokensAsMinLatency:
				log.Warn().Str("zedtoken", requested.Token).Msg("ZedToken specified references an older datastore and SpiceDB is configured to treat this as a min latency request")
				return databaseRev, false, nil

			case TreatMismatchingTokensAsError:
				log.Error().Str("zedtoken", requested.Token).Msg("ZedToken specified references an older datastore and SpiceDB is configured to raise an error in this scenario")
				return datastore.NoRevision, false, fmt.Errorf("ZedToken specified references an older datastore and SpiceDB is configured to raise an error in this scenario")

			default:
				return datastore.NoRevision, false, spiceerrors.MustBugf("unknown mismatching token option: %v", option)
			}
		}

		if databaseRev.GreaterThan(requestedRev) {
			return databaseRev, false, nil
		}

		return requestedRev, true, nil
	}

	return databaseRev, false, nil
}

func rewriteDatastoreError(ctx context.Context, err error) error {
	// Check if the error can be directly used.
	if _, ok := status.FromError(err); ok {
		return err
	}

	switch {
	case errors.As(err, &datastore.ErrInvalidRevision{}):
		return status.Errorf(codes.OutOfRange, "invalid revision: %s", err)

	case errors.As(err, &datastore.ErrReadOnly{}):
		return shared.ErrServiceReadOnly

	default:
		log.Ctx(ctx).Err(err).Msg("unexpected consistency middleware error")
		return err
	}
}
