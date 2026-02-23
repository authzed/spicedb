package consistency

import (
	"context"
	"errors"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

var ConsistencyCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "middleware",
	Name:      "consistency_assigned_total",
	Help:      "Count of the consistencies used per request",
}, []string{"method", "source", "service"})

// MismatchingTokenOption is the option specifying the behavior of the consistency middleware
// when a ZedToken provided references a different datastore instance than the current
// datastore instance.
type MismatchingTokenOption int

const (
	// TreatMismatchingTokensAsFullConsistency specifies that the middleware should treat
	// a ZedToken that references a different datastore instance as a request for full
	// consistency.
	TreatMismatchingTokensAsFullConsistency MismatchingTokenOption = iota

	// TreatMismatchingTokensAsMinLatency specifies that the middleware should treat
	// a ZedToken that references a different datastore instance as a request for min
	// latency.
	TreatMismatchingTokensAsMinLatency

	// TreatMismatchingTokensAsError specifies that the middleware should raise an error
	// when a ZedToken that references a different datastore instance is provided.
	TreatMismatchingTokensAsError
)

type hasConsistency interface{ GetConsistency() *v1.Consistency }

type hasOptionalCursor interface{ GetOptionalCursor() *v1.Cursor }

type ctxKeyType struct{}

var revisionKey ctxKeyType = struct{}{}

var errInvalidZedToken = status.Error(codes.InvalidArgument, "invalid revision requested")

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
			dl := datalayer.FromContext(ctx)
			if dl == nil {
				return nil, nil, spiceerrors.MustBugf("consistency middleware did not inject datastore")
			}

			zedToken, err := zedtoken.NewFromRevision(ctx, rev, dl)
			if err != nil {
				return nil, nil, err
			}

			return rev, zedToken, nil
		}
	}

	return nil, nil, status.Error(codes.Internal, "consistency middleware did not inject revision")
}

// AddRevisionToContext adds a revision to the given context, based on the consistency block found
// in the given request (if applicable).
func AddRevisionToContext(ctx context.Context, req any, dl datalayer.DataLayer, serviceLabel string, option MismatchingTokenOption) error {
	switch req := req.(type) {
	case hasConsistency:
		return addRevisionToContextFromConsistency(ctx, req, dl, serviceLabel, option)
	default:
		return nil
	}
}

// addRevisionToContextFromConsistency adds a revision to the given context, based on the consistency block found
// in the given request (if applicable).
func addRevisionToContextFromConsistency(ctx context.Context, req hasConsistency, dl datalayer.DataLayer, serviceLabel string, option MismatchingTokenOption) error {
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
		if serviceLabel != "" {
			ConsistencyCounter.WithLabelValues("snapshot", "cursor", serviceLabel).Inc()
		}

		requestedRev, _, err := cursor.DecodeToDispatchRevision(ctx, withOptionalCursor.GetOptionalCursor(), dl)
		if err != nil {
			return rewriteDatastoreError(err)
		}

		err = dl.CheckRevision(ctx, requestedRev)
		if err != nil {
			return rewriteDatastoreError(err)
		}

		revision = requestedRev

	case consistency == nil || consistency.GetMinimizeLatency():
		// Minimize Latency: Use the datastore's current revision, whatever it may be.
		source := "request"
		if consistency == nil {
			source = "server"
		}

		if serviceLabel != "" {
			ConsistencyCounter.WithLabelValues("minlatency", source, serviceLabel).Inc()
		}

		databaseRev, err := dl.OptimizedRevision(ctx)
		if err != nil {
			return rewriteDatastoreError(err)
		}
		revision = databaseRev

	case consistency.GetFullyConsistent():
		// Fully Consistent: Use the datastore's synchronized revision.
		if serviceLabel != "" {
			ConsistencyCounter.WithLabelValues("full", "request", serviceLabel).Inc()
		}

		databaseRev, err := dl.HeadRevision(ctx)
		if err != nil {
			return rewriteDatastoreError(err)
		}
		revision = databaseRev

	case consistency.GetAtLeastAsFresh() != nil:
		// At least as fresh as: Pick one of the datastore's revision and that specified, which
		// ever is later.
		picked, pickedRequest, err := pickBestRevision(ctx, consistency.GetAtLeastAsFresh(), dl, option)
		if err != nil {
			return rewriteDatastoreError(err)
		}

		source := "server"
		if pickedRequest {
			source = "request"
		}

		if serviceLabel != "" {
			ConsistencyCounter.WithLabelValues("atleast", source, serviceLabel).Inc()
		}

		revision = picked

	case consistency.GetAtExactSnapshot() != nil:
		// Exact snapshot: Use the revision as encoded in the zed token.
		if serviceLabel != "" {
			ConsistencyCounter.WithLabelValues("snapshot", "request", serviceLabel).Inc()
		}

		requestedRev, status, err := zedtoken.DecodeRevision(consistency.GetAtExactSnapshot(), dl)
		if err != nil {
			return errInvalidZedToken
		}

		if status == zedtoken.StatusMismatchedDatastoreID {
			return errors.New("ZedToken specified references a different datastore instance but at-exact-snapshot was requested")
		}

		err = dl.CheckRevision(ctx, requestedRev)
		if err != nil {
			return rewriteDatastoreError(err)
		}

		revision = requestedRev

	default:
		return status.Errorf(codes.Internal, "missing handling of consistency case in %v", consistency)
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
func UnaryServerInterceptor(serviceLabel string, option MismatchingTokenOption) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		for bypass := range bypassServiceWhitelist {
			if strings.HasPrefix(info.FullMethod, bypass) {
				return handler(ctx, req)
			}
		}
		dl := datalayer.MustFromContext(ctx)
		newCtx := ContextWithHandle(ctx)
		if err := AddRevisionToContext(newCtx, req, dl, serviceLabel, option); err != nil {
			return nil, err
		}

		return handler(newCtx, req)
	}
}

// StreamServerInterceptor returns a new stream server interceptor that performs per-request exchange of
// the specified consistency configuration for the revision at which to perform the request.
func StreamServerInterceptor(serviceLabel string, option MismatchingTokenOption) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		for bypass := range bypassServiceWhitelist {
			if strings.HasPrefix(info.FullMethod, bypass) {
				return handler(srv, stream)
			}
		}
		wrapper := &recvWrapper{stream, ContextWithHandle(stream.Context()), serviceLabel, option, AddRevisionToContext}
		return handler(srv, wrapper)
	}
}

type recvWrapper struct {
	grpc.ServerStream
	ctx          context.Context
	serviceLabel string
	option       MismatchingTokenOption
	handler      func(context.Context, any, datalayer.DataLayer, string, MismatchingTokenOption) error
}

func (s *recvWrapper) Context() context.Context { return s.ctx }

func (s *recvWrapper) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	dl := datalayer.MustFromContext(s.ctx)
	return s.handler(s.ctx, m, dl, s.serviceLabel, s.option)
}

// pickBestRevision compares the provided ZedToken with the optimized revision of the datastore, and returns the most
// recent one. The boolean return value will be true if the provided ZedToken is the most recent, false otherwise.
func pickBestRevision(ctx context.Context, requested *v1.ZedToken, dl datalayer.DataLayer, option MismatchingTokenOption) (datastore.Revision, bool, error) {
	// Calculate a revision as we see fit
	databaseRev, err := dl.OptimizedRevision(ctx)
	if err != nil {
		return datastore.NoRevision, false, err
	}

	if requested != nil {
		requestedRev, status, err := zedtoken.DecodeRevision(requested, dl)
		if err != nil {
			return datastore.NoRevision, false, errInvalidZedToken
		}

		if status == zedtoken.StatusMismatchedDatastoreID {
			switch option {
			case TreatMismatchingTokensAsFullConsistency:
				log.Warn().Str("zedtoken", requested.Token).Msg("ZedToken specified references a different datastore instance and SpiceDB is configured to treat this as a full consistency request")
				headRev, err := dl.HeadRevision(ctx)
				if err != nil {
					return datastore.NoRevision, false, err
				}

				return headRev, false, nil

			case TreatMismatchingTokensAsMinLatency:
				log.Warn().Str("zedtoken", requested.Token).Msg("ZedToken specified references a different datastore instance and SpiceDB is configured to treat this as a min latency request")
				return databaseRev, false, nil

			case TreatMismatchingTokensAsError:
				log.Warn().Str("zedtoken", requested.Token).Msg("ZedToken specified references a different datastore instance and SpiceDB is configured to raise an error in this scenario")
				return datastore.NoRevision, false, errors.New("ZedToken specified references a different datastore instance and SpiceDB is configured to raise an error in this scenario")

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

func rewriteDatastoreError(err error) error {
	// Check if the error can be directly used.
	if _, ok := status.FromError(err); ok {
		return err
	}

	switch {
	case errors.As(err, &datastore.InvalidRevisionError{}):
		return status.Errorf(codes.OutOfRange, "invalid revision: %s", err)

	case errors.As(err, &datastore.ReadOnlyError{}):
		return shared.ErrServiceReadOnly

	case errors.Is(err, context.Canceled):
		return status.Errorf(codes.Canceled, "%s", err)

	default:
		return status.Errorf(codes.Internal, "unexpected consistency middleware error: %s", err.Error())
	}
}
