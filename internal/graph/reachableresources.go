package graph

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/scylladb/go-set/strset"
	"golang.org/x/sync/errgroup"
)

// NewConcurrentReachableResources creates an instance of ConcurrentReachableResources.
func NewConcurrentReachableResources(d dispatch.ReachableResources, nsm namespace.Manager) *ConcurrentReachableResources {
	return &ConcurrentReachableResources{d: d, nsm: nsm}
}

// ConcurrentReachableResources exposes a method to perform ReachableResources requests, and
// delegates subproblems to the provided dispatch.ReachableResources instance.
type ConcurrentReachableResources struct {
	d   dispatch.ReachableResources
	nsm namespace.Manager
}

// ValidatedReachableResourcesRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedReachableResourcesRequest struct {
	*v1.DispatchReachableResourcesRequest
	Revision decimal.Decimal
}

func (crr *ConcurrentReachableResources) ReachableResources(
	req ValidatedReachableResourcesRequest,
	stream v1.DispatchService_DispatchReachableResourcesServer,
) error {
	ctx := stream.Context()

	// If the resource type matches the subject type, yield directly.
	if req.Subject.Namespace == req.ObjectRelation.Namespace &&
		req.Subject.Relation == req.ObjectRelation.Relation {
		stream.Send(&v1.DispatchReachableResourcesResponse{
			Resource: &v1.ReachableResource{
				Resource:     req.Subject,
				ResultStatus: v1.ReachableResource_HAS_PERMISSION,
			},
		})
	}

	// Load the type system and reachability graph to find the entrypoints for the reachability.
	_, typeSystem, err := crr.nsm.ReadNamespaceAndTypes(ctx, req.ObjectRelation.Namespace, req.Revision)
	if err != nil {
		return err
	}

	rg := namespace.ReachabilityGraphFor(typeSystem.AsValidated())
	entrypoints, err := rg.OptimizedEntrypointsForSubjectToResource(ctx, &core.RelationReference{
		Namespace: req.Subject.Namespace,
		Relation:  req.Subject.Relation,
	}, req.ObjectRelation)
	if err != nil {
		return err
	}

	cancelCtx, checkCancel := context.WithCancel(ctx)
	defer checkCancel()

	g, subCtx := errgroup.WithContext(cancelCtx)

	// For each entrypoint, load the necessary data and re-dispatch if a subproblem was found.
	for _, entrypoint := range entrypoints {
		switch entrypoint.EntrypointKind() {

		case core.ReachabilityEntrypoint_RELATION_ENTRYPOINT:
			err := crr.lookupRelationEntrypoint(ctx, entrypoint, g, subCtx, req, stream)
			if err != nil {
				return err
			}

		case core.ReachabilityEntrypoint_COMPUTED_USERSET_ENTRYPOINT:
			containingRelation := entrypoint.ContainingRelationOrPermission()
			rewrittenSubjectTpl := &core.ObjectAndRelation{
				Namespace: containingRelation.Namespace,
				ObjectId:  req.Subject.ObjectId,
				Relation:  containingRelation.Relation,
			}

			// Otherwise, redispatch.
			g.Go(crr.redispatch(
				subCtx,
				entrypoint,
				stream,
				&v1.DispatchReachableResourcesRequest{
					ObjectRelation: req.ObjectRelation,
					Subject:        rewrittenSubjectTpl,
					Metadata: &v1.ResolverMeta{
						AtRevision:     req.Revision.String(),
						DepthRemaining: req.Metadata.DepthRemaining - 1,
					},
				},
			))

		case core.ReachabilityEntrypoint_TUPLESET_TO_USERSET_ENTRYPOINT:
			containingRelation := entrypoint.ContainingRelationOrPermission()

			// TODO(jschorr): Should we put this information into the entrypoint itself, to avoid
			// a lookup of the namespace?
			nsDef, ttuTypeSystem, err := crr.nsm.ReadNamespaceAndTypes(ctx, containingRelation.Namespace, req.Revision)
			if err != nil {
				return err
			}

			ttu := entrypoint.TupleToUserset(nsDef)
			if ttu == nil {
				return fmt.Errorf("found nil ttu for TTU entrypoint")
			}

			// Search for the resolved subject in the tupleset of the TTU. Note that we need to do so
			// for both `...` as well as the subject's defined relation, as either is applicable in
			// the tupleset (the relation is ignored when following the arrow).
			relations := strset.New(tuple.Ellipsis, req.Subject.Relation)

			for _, subjectRelation := range relations.List() {
				isAllowed, err := ttuTypeSystem.IsAllowedDirectRelation(ttu.Tupleset.Relation, req.Subject.Namespace, subjectRelation)
				if err != nil {
					return err
				}

				if isAllowed != namespace.DirectRelationValid {
					continue
				}

				ds := datastoremw.MustFromContext(ctx)
				it, err := ds.ReverseQueryTuples(
					ctx,
					tuple.UsersetToSubjectFilter(&core.ObjectAndRelation{
						Namespace: req.Subject.Namespace,
						ObjectId:  req.Subject.ObjectId,
						Relation:  subjectRelation,
					}),
					req.Revision,
					options.WithResRelation(&options.ResourceRelation{
						Namespace: containingRelation.Namespace,
						Relation:  ttu.Tupleset.Relation,
					}),
				)
				if err != nil {
					return err
				}
				defer it.Close()

				for tpl := it.Next(); tpl != nil; tpl = it.Next() {
					if it.Err() != nil {
						return it.Err()
					}

					rewrittenObjectTpl := &core.ObjectAndRelation{
						Namespace: containingRelation.Namespace,
						ObjectId:  tpl.ObjectAndRelation.ObjectId,
						Relation:  containingRelation.Relation,
					}

					g.Go(crr.redispatch(
						subCtx,
						entrypoint,
						stream,
						&v1.DispatchReachableResourcesRequest{
							ObjectRelation: req.ObjectRelation,
							Subject:        rewrittenObjectTpl,
							Metadata: &v1.ResolverMeta{
								AtRevision:     req.Revision.String(),
								DepthRemaining: req.Metadata.DepthRemaining - 1,
							},
						},
					))
				}
			}

		default:
			panic(fmt.Sprintf("Unknown kind of entrypoint: %v", entrypoint.EntrypointKind()))
		}
	}

	return g.Wait()
}

func (crr *ConcurrentReachableResources) lookupRelationEntrypoint(ctx context.Context,
	entrypoint namespace.ReachabilityEntrypoint,
	g *errgroup.Group,
	subCtx context.Context,
	req ValidatedReachableResourcesRequest,
	stream v1.DispatchService_DispatchReachableResourcesServer,
) error {
	relationReference := entrypoint.DirectRelation()
	_, relTypeSystem, err := crr.nsm.ReadNamespaceAndTypes(ctx, relationReference.Namespace, req.Revision)
	if err != nil {
		return err
	}

	isDirectAllowed, err := relTypeSystem.IsAllowedDirectRelation(relationReference.Relation, req.Subject.Namespace, req.Subject.Relation)
	if err != nil {
		return err
	}

	isWildcardAllowed, err := relTypeSystem.IsAllowedPublicNamespace(relationReference.Relation, req.Subject.Namespace)
	if err != nil {
		return err
	}

	// TODO(jschorr): Combine these into a single query once the datastore supports a direct or wildcard
	// query option (which should also be used for check).
	ds := datastoremw.MustFromContext(ctx)

	collectResults := func(objectId string) error {
		it, err := ds.ReverseQueryTuples(
			subCtx,
			tuple.UsersetToSubjectFilter(&core.ObjectAndRelation{
				Namespace: req.Subject.Namespace,
				ObjectId:  objectId,
				Relation:  req.Subject.Relation,
			}),
			req.Revision,
			options.WithResRelation(&options.ResourceRelation{
				Namespace: relationReference.Namespace,
				Relation:  relationReference.Relation,
			}),
		)
		if err != nil {
			return err
		}
		defer it.Close()

		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			if it.Err() != nil {
				return it.Err()
			}

			// Redispatch to continue looking for results.
			g.Go(crr.redispatch(
				subCtx,
				entrypoint,
				stream,
				&v1.DispatchReachableResourcesRequest{
					ObjectRelation: req.ObjectRelation,
					Subject:        tpl.ObjectAndRelation,
					Metadata: &v1.ResolverMeta{
						AtRevision:     req.Revision.String(),
						DepthRemaining: req.Metadata.DepthRemaining - 1,
					},
				},
			))
		}
		return nil
	}

	if isDirectAllowed == namespace.DirectRelationValid {
		g.Go(func() error {
			return collectResults(req.Subject.ObjectId)
		})
	}

	if isWildcardAllowed == namespace.PublicSubjectAllowed {
		g.Go(func() error {
			return collectResults(tuple.PublicWildcard)
		})
	}

	return nil
}

func (crr *ConcurrentReachableResources) redispatch(
	ctx context.Context,
	entrypoint namespace.ReachabilityEntrypoint,
	parentStream v1.DispatchService_DispatchReachableResourcesServer,
	req *v1.DispatchReachableResourcesRequest,
) func() error {
	return func() error {
		stream := &dispatch.WrappedDispatchStream[*v1.DispatchReachableResourcesResponse]{
			DispatchStream: parentStream,
			Ctx:            ctx,
			Processor: func(result *v1.DispatchReachableResourcesResponse) (*v1.DispatchReachableResourcesResponse, error) {
				if entrypoint.IsDirectResult() {
					return result, nil
				}

				// Otherwise, mark all streamed items as requiring checks.
				result.Resource.ResultStatus = v1.ReachableResource_REQUIRES_CHECK
				return result, nil
			},
		}

		return crr.d.DispatchReachableResources(req, stream)
	}
}
