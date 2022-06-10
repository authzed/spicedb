package keys

import (
	"context"

	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// Handler is an interface defining how keys are computed for dispatching and caching.
type Handler interface {
	// ComputeCheckKey computes the key for a Check operation.
	ComputeCheckKey(ctx context.Context, req *v1.DispatchCheckRequest) (string, error)
}

// DirectKeyHandler is a key handler that uses the relation name itself as the key.
type DirectKeyHandler struct{}

func (d *DirectKeyHandler) ComputeCheckKey(ctx context.Context, req *v1.DispatchCheckRequest) (string, error) {
	return dispatch.CheckRequestToKey(req), nil
}

// CanonicalKeyHandler is a key handler which makes use of the canonical key for relations for
// dispatching.
type CanonicalKeyHandler struct{}

func (c *CanonicalKeyHandler) ComputeCheckKey(ctx context.Context, req *v1.DispatchCheckRequest) (string, error) {
	// NOTE: We do not use the canonicalized cache key when checking within the same namespace, as
	// we may get different results if the subject being checked matches the resource exactly, e.g.
	// a check for `somenamespace:someobject#somerel@somenamespace:someobject#somerel`.
	if req.ResourceAndRelation.Namespace != req.Subject.Namespace {
		// Load the relation to get its computed cache key, if any.
		revision, err := decimal.NewFromString(req.Metadata.AtRevision)
		if err != nil {
			return "", err
		}

		ds := datastoremw.MustFromContext(ctx).SnapshotReader(revision)

		_, relation, err := namespace.ReadNamespaceAndRelation(
			ctx,
			req.ResourceAndRelation.Namespace,
			req.ResourceAndRelation.Relation,
			ds,
		)
		if err != nil {
			return "", err
		}

		// TODO(jschorr): Remove this conditional once we have a verified migration ordering system that ensures a backfill migration has
		// run after the namespace annotation code has been fully deployed by users.
		if relation.CanonicalCacheKey != "" {
			return dispatch.CheckRequestToKeyWithCanonical(req, relation.CanonicalCacheKey), nil
		}
	}

	return dispatch.CheckRequestToKey(req), nil
}
