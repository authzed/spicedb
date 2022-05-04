package graph

import (
	"context"
	"errors"
	"fmt"

	v1_proto "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const MaxConcurrentSlowLookupChecks = 10

// NewConcurrentLookup creates and instance of ConcurrentLookup.
func NewConcurrentLookup(d dispatch.Lookup, c dispatch.Check) *ConcurrentLookup {
	return &ConcurrentLookup{d: d, c: c}
}

// ConcurrentLookup exposes a method to perform Lookup requests, and delegates subproblems to the
// provided dispatch.Lookup instance.
type ConcurrentLookup struct {
	d dispatch.Lookup
	c dispatch.Check
}

// ValidatedLookupRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedLookupRequest struct {
	*v1.DispatchLookupRequest
	Revision decimal.Decimal
}

// Calculate the maximum int value to allow us to effectively set no limit on certain recursive
// lookup calls.
const (
	noLimit = ^uint32(0)
)

// Lookup performs a lookup request with the provided request and context.
func (cl *ConcurrentLookup) Lookup(ctx context.Context, req ValidatedLookupRequest) (*v1.DispatchLookupResponse, error) {
	funcToResolve := cl.lookupInternal(ctx, req)
	if req.Subject.ObjectId == tuple.PublicWildcard {
		funcToResolve = returnResult(lookupResultError(req, NewErrInvalidArgument(errors.New("cannot perform lookup on wildcard")), emptyMetadata))
	}

	resolved := lookupOne(ctx, req, funcToResolve)

	// Remove the resolved relation reference from the excluded direct list to mark that it was completely resolved.
	var lookupExcludedDirect []*core.RelationReference
	if resolved.Resp.Metadata.LookupExcludedDirect != nil {
		for _, rr := range resolved.Resp.Metadata.LookupExcludedDirect {
			if rr.Namespace == req.ObjectRelation.Namespace && rr.Relation == req.ObjectRelation.Relation {
				continue
			}

			lookupExcludedDirect = append(lookupExcludedDirect, rr)
		}
	}

	resolved.Resp.Metadata.LookupExcludedDirect = lookupExcludedDirect
	resolved.Resp.Metadata = addCallToResponseMetadata(resolved.Resp.Metadata)
	return resolved.Resp, resolved.Err
}

// LookupViaChecks performs a slow-path lookup request with the provided request and context.
// It performs a lookup by finding all instances of the requested object and then
// issuing a check for each one.
func (cl *ConcurrentLookup) LookupViaChecks(ctx context.Context, req ValidatedLookupRequest) (*v1.DispatchLookupResponse, error) {
	if req.Subject.ObjectId == tuple.PublicWildcard {
		resp := lookupResultError(req, NewErrInvalidArgument(errors.New("cannot perform lookup on wildcard")), emptyMetadata)
		return resp.Resp, resp.Err
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)
	it, err := ds.QueryRelationships(ctx,
		&v1_proto.RelationshipFilter{
			ResourceType: req.ObjectRelation.Namespace,
		},
	)
	if err != nil {
		resp := lookupResultError(req, err, emptyMetadata)
		return resp.Resp, resp.Err
	}
	defer it.Close()

	objects := tuple.NewONRSet()
	for tpl := it.Next(); tpl != nil; tpl = it.Next() {
		objects.Add(&core.ObjectAndRelation{
			Namespace: tpl.ObjectAndRelation.Namespace,
			ObjectId:  tpl.ObjectAndRelation.ObjectId,
			Relation:  req.ObjectRelation.Relation,
		})
	}

	cancelCtx, checkCancel := context.WithCancel(ctx)
	defer checkCancel()

	g, checkCtx := errgroup.WithContext(cancelCtx)

	type onrWithMeta struct {
		onr  *core.ObjectAndRelation
		meta *v1.ResponseMeta
	}
	allowedONRs := make(chan onrWithMeta)

	dispatchCount := uint32(1) // 1 for the initial read
	cachedDispatchCount := uint32(0)
	depthRequired := uint32(0)
	allowed := tuple.NewONRSet()
	g.Go(func() error {
		for {
			o, ok := <-allowedONRs
			if !ok {
				break
			}
			allowed.Add(o.onr)
			dispatchCount += o.meta.DispatchCount
			cachedDispatchCount += o.meta.CachedDispatchCount
			if o.meta.DepthRequired > depthRequired {
				depthRequired = o.meta.DepthRequired
			}
		}
		return nil
	})

	g.Go(func() error {
		sem := semaphore.NewWeighted(MaxConcurrentSlowLookupChecks)
		for _, o := range objects.AsSlice() {
			if err := sem.Acquire(ctx, 1); err != nil {
				return err
			}
			o := o
			g.Go(func() error {
				defer sem.Release(1)
				res, err := cl.c.DispatchCheck(checkCtx, &v1.DispatchCheckRequest{
					Metadata: &v1.ResolverMeta{
						AtRevision:     req.Revision.String(),
						DepthRemaining: req.Metadata.DepthRemaining,
					},
					ObjectAndRelation: o,
					Subject:           req.Subject,
				})
				if err != nil {
					return err
				}
				if res.Membership == v1.DispatchCheckResponse_MEMBER {
					allowedONRs <- onrWithMeta{
						onr:  o,
						meta: res.Metadata,
					}
				}
				return nil
			})
		}
		if err := sem.Acquire(ctx, MaxConcurrentSlowLookupChecks); err != nil {
			return err
		}
		close(allowedONRs)
		return it.Err()
	})

	if err := g.Wait(); err != nil {
		resp := lookupResultError(req, err, emptyMetadata)
		return resp.Resp, resp.Err
	}

	res := lookupResult(req, allowed.AsSlice(), &v1.ResponseMeta{
		DispatchCount:       dispatchCount,
		CachedDispatchCount: cachedDispatchCount,
		DepthRequired:       depthRequired,
	})
	return res.Resp, res.Err
}

func (cl *ConcurrentLookup) lookupInternal(ctx context.Context, req ValidatedLookupRequest) ReduceableLookupFunc {
	log.Ctx(ctx).Trace().Object("lookup", req).Send()

	objSet := tuple.NewONRSet()

	// If we've found the target ONR, add it to the set of resolved objects. Note that we still need
	// to continue processing, as this may also be an intermediate step in resolution.
	if req.ObjectRelation.Namespace == req.Subject.Namespace && req.ObjectRelation.Relation == req.Subject.Relation {
		objSet.Add(req.Subject)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)
	nsdef, typeSystem, err := namespace.ReadNamespaceAndTypes(ctx, req.ObjectRelation.Namespace, ds)
	if err != nil {
		return returnResult(lookupResultError(req, err, emptyMetadata))
	}

	// Ensure the relation exists.
	relation, ok := findRelation(nsdef, req.ObjectRelation.Relation)
	if !ok {
		return returnResult(lookupResultError(
			req,
			NewRelationNotFoundErr(req.ObjectRelation.Namespace, req.ObjectRelation.Relation),
			emptyMetadata,
		))
	}

	rewrite := relation.UsersetRewrite
	var request ReduceableLookupFunc
	if rewrite != nil {
		request = cl.processRewrite(ctx, req, nsdef, typeSystem, rewrite)
	} else {
		request = cl.lookupDirect(ctx, req, typeSystem)
	}

	// Perform the structural lookup.
	result := lookupAny(ctx, req, req.Limit, []ReduceableLookupFunc{request})
	if result.Err != nil {
		return returnResult(lookupResultError(req, result.Err, result.Resp.Metadata))
	}

	objSet.Update(result.Resp.ResolvedOnrs)

	// Recursively perform lookup on any of the ONRs found that do not match the target ONR.
	// This ensures that we resolve the full transitive closure of all objects.
	toCheck := objSet
	responseMetadata := result.Resp.Metadata
	for toCheck.Length() != 0 && objSet.Length() < req.Limit {
		var requests []ReduceableLookupFunc
		for _, obj := range toCheck.AsSlice() {
			// If we've already found the target ONR, no further resolution is necessary.
			if onrEqualOrWildcard(obj, req.Subject) {
				continue
			}

			requests = append(requests, cl.dispatch(ValidatedLookupRequest{
				&v1.DispatchLookupRequest{
					Subject:        obj,
					ObjectRelation: req.ObjectRelation,
					Limit:          req.Limit - objSet.Length(),
					Metadata:       decrementDepth(req.Metadata),
					DirectStack:    req.DirectStack,
					TtuStack:       req.TtuStack,
				},
				req.Revision,
			}))
		}

		result := lookupAny(ctx, req, req.Limit, requests)
		responseMetadata = combineResponseMetadata(responseMetadata, result.Resp.Metadata)

		if result.Err != nil {
			return returnResult(lookupResultError(req, result.Err, responseMetadata))
		}

		toCheck = tuple.NewONRSet()

		for _, obj := range result.Resp.ResolvedOnrs {
			// Only check recursively for new objects.
			if objSet.Add(obj) {
				toCheck.Add(obj)
			}
		}
	}

	return returnResult(lookupResult(req, limitedSlice(objSet.AsSlice(), req.Limit), responseMetadata))
}

func (cl *ConcurrentLookup) lookupDirect(ctx context.Context, req ValidatedLookupRequest, typeSystem *namespace.TypeSystem) ReduceableLookupFunc {
	requests := []ReduceableLookupFunc{}

	// Ensure type informatione exists on the relation.
	if !typeSystem.HasTypeInformation(req.ObjectRelation.Relation) {
		return returnResult(lookupResultError(
			req,
			NewRelationMissingTypeInfoErr(req.ObjectRelation.Namespace, req.ObjectRelation.Relation),
			emptyMetadata,
		))
	}

	// Dispatch a check for the target ONR directly, if it is allowed on the start relation.
	isDirectAllowed, err := typeSystem.IsAllowedDirectRelation(req.ObjectRelation.Relation, req.Subject.Namespace, req.Subject.Relation)
	if err != nil {
		return returnResult(lookupResultError(req, err, emptyMetadata))
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)

	if isDirectAllowed == namespace.DirectRelationValid {
		requests = append(requests, func(ctx context.Context, resultChan chan<- LookupResult) {
			objects := tuple.NewONRSet()
			it, err := ds.ReverseQueryRelationships(
				ctx,
				tuple.UsersetToSubjectFilter(req.Subject),
				options.WithResRelation(&options.ResourceRelation{
					Namespace: req.ObjectRelation.Namespace,
					Relation:  req.ObjectRelation.Relation,
				}),
			)
			if err != nil {
				resultChan <- lookupResultError(req, err, emptyMetadata)
				return
			}
			defer it.Close()

			for tpl := it.Next(); tpl != nil; tpl = it.Next() {
				objects.Add(tpl.ObjectAndRelation)
				if objects.Length() >= req.Limit {
					break
				}
			}

			if it.Err() != nil {
				resultChan <- lookupResultError(req, it.Err(), emptyMetadata)
				return
			}

			resultChan <- lookupResult(req, objects.AsSlice(), emptyMetadata)
		})
	}

	// Dispatch a check for the subject wildcard, if allowed.
	isWildcardAllowed, err := typeSystem.IsAllowedPublicNamespace(req.ObjectRelation.Relation, req.Subject.Namespace)
	if err != nil {
		return returnResult(lookupResultError(req, err, emptyMetadata))
	}

	if isWildcardAllowed == namespace.PublicSubjectAllowed {
		requests = append(requests, func(ctx context.Context, resultChan chan<- LookupResult) {
			objects := tuple.NewONRSet()
			it, err := ds.ReverseQueryRelationships(
				ctx,
				tuple.UsersetToSubjectFilter(&core.ObjectAndRelation{
					Namespace: req.Subject.Namespace,
					ObjectId:  tuple.PublicWildcard,
					Relation:  req.Subject.Relation,
				}),
				options.WithResRelation(&options.ResourceRelation{
					Namespace: req.ObjectRelation.Namespace,
					Relation:  req.ObjectRelation.Relation,
				}),
			)
			if err != nil {
				resultChan <- lookupResultError(req, err, emptyMetadata)
				return
			}
			defer it.Close()

			for tpl := it.Next(); tpl != nil; tpl = it.Next() {
				objects.Add(tpl.ObjectAndRelation)
				if objects.Length() >= req.Limit {
					break
				}
			}

			if it.Err() != nil {
				resultChan <- lookupResultError(req, it.Err(), emptyMetadata)
				return
			}

			resultChan <- lookupResult(req, objects.AsSlice(), emptyMetadata)
		})
	}

	// Dispatch to any allowed subject relation types that don't match the target ONR, collect
	// the found object IDs, and then search for those.
	allowedDirect, err := typeSystem.AllowedSubjectRelations(req.ObjectRelation.Relation)
	if err != nil {
		return returnResult(lookupResultError(req, err, emptyMetadata))
	}

	directStack := make([]*core.RelationReference, 0, len(req.DirectStack)+1)
	directStack = append(directStack, req.DirectStack...)
	directStack = append(directStack, &core.RelationReference{
		Namespace: req.ObjectRelation.Namespace,
		Relation:  req.ObjectRelation.Relation,
	})

	var excludedDirect []*core.RelationReference
	for _, allowedDirectType := range allowedDirect {
		if allowedDirectType.GetRelation() == Ellipsis {
			continue
		}

		if allowedDirectType.Namespace == req.ObjectRelation.Namespace &&
			allowedDirectType.GetRelation() == req.ObjectRelation.Relation {
			continue
		}

		// Prevent recursive inferred lookups, which can cause an infinite loop.
		rr := &core.RelationReference{
			Namespace: allowedDirectType.Namespace,
			Relation:  allowedDirectType.GetRelation(),
		}
		if checkStackContains(directStack, rr) {
			excludedDirect = append(excludedDirect, rr)
			continue
		}

		// Bind to the current allowed direct type
		allowedDirectType := allowedDirectType

		requests = append(requests, func(ctx context.Context, resultChan chan<- LookupResult) {
			// Dispatch on the inferred relation.
			inferredRequest := cl.dispatch(ValidatedLookupRequest{
				&v1.DispatchLookupRequest{
					Subject: req.Subject,
					ObjectRelation: &core.RelationReference{
						Namespace: allowedDirectType.Namespace,
						Relation:  allowedDirectType.GetRelation(),
					},
					Limit:       noLimit, // Since this is an inferred lookup, we can't limit.
					Metadata:    decrementDepth(req.Metadata),
					DirectStack: directStack,
					TtuStack:    req.TtuStack,
				},
				req.Revision,
			})

			result := lookupAny(ctx, req, noLimit, []ReduceableLookupFunc{inferredRequest})
			if result.Err != nil {
				resultChan <- result
				return
			}

			// For each inferred object found, check for the target ONR.
			objects := tuple.NewONRSet()
			if len(result.Resp.ResolvedOnrs) > 0 {
				limit := uint64(req.Limit)
				it, err := ds.QueryRelationships(
					ctx,
					&v1_proto.RelationshipFilter{
						ResourceType:     req.ObjectRelation.Namespace,
						OptionalRelation: req.ObjectRelation.Relation,
					},
					options.SetUsersets(result.Resp.ResolvedOnrs),
					options.WithLimit(&limit),
				)
				if err != nil {
					resultChan <- lookupResultError(req, err, emptyMetadata)
					return
				}
				defer it.Close()

				for tpl := it.Next(); tpl != nil; tpl = it.Next() {
					if it.Err() != nil {
						resultChan <- lookupResultError(req, it.Err(), emptyMetadata)
						return
					}

					objects.Add(tpl.ObjectAndRelation)
					if objects.Length() >= req.Limit {
						break
					}
				}
			}

			resultChan <- lookupResult(req, objects.AsSlice(), result.Resp.Metadata)
		})
	}

	return func(ctx context.Context, resultChan chan<- LookupResult) {
		resultChan <- lookupAnyWithExcludedDirect(ctx, req, req.Limit, requests, excludedDirect)
	}
}

func (cl *ConcurrentLookup) processRewrite(ctx context.Context, req ValidatedLookupRequest, nsdef *core.NamespaceDefinition, typeSystem *namespace.TypeSystem, usr *core.UsersetRewrite) ReduceableLookupFunc {
	switch rw := usr.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		return cl.processSetOperation(ctx, req, nsdef, typeSystem, rw.Union, lookupAny)
	case *core.UsersetRewrite_Intersection:
		return cl.processSetOperation(ctx, req, nsdef, typeSystem, rw.Intersection, lookupAll)
	case *core.UsersetRewrite_Exclusion:
		return cl.processSetOperation(ctx, req, nsdef, typeSystem, rw.Exclusion, lookupExclude)
	default:
		return returnResult(lookupResultError(req, fmt.Errorf("unknown userset rewrite kind under `%s#%s`", req.ObjectRelation.Namespace, req.ObjectRelation.Relation), emptyMetadata))
	}
}

func (cl *ConcurrentLookup) processSetOperation(ctx context.Context, req ValidatedLookupRequest, nsdef *core.NamespaceDefinition, typeSystem *namespace.TypeSystem, so *core.SetOperation, reducer LookupReducer) ReduceableLookupFunc {
	var requests []ReduceableLookupFunc

	for _, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *core.SetOperation_Child_XThis:
			// TODO(jschorr): Turn into an error once v0 API has been removed.
			log.Ctx(ctx).Warn().Stringer("operation", so).Msg("Use of _this is deprecated and will soon be an error! Please switch to using schema!")
			requests = append(requests, cl.lookupDirect(ctx, req, typeSystem))
		case *core.SetOperation_Child_ComputedUserset:
			requests = append(requests, cl.lookupComputed(ctx, req, child.ComputedUserset))
		case *core.SetOperation_Child_UsersetRewrite:
			requests = append(requests, cl.processRewrite(ctx, req, nsdef, typeSystem, child.UsersetRewrite))
		case *core.SetOperation_Child_TupleToUserset:
			requests = append(requests, cl.processTupleToUserset(ctx, req, nsdef, typeSystem, child.TupleToUserset))
		case *core.SetOperation_Child_XNil:
			requests = append(requests, emptyLookup(ctx, req))
		default:
			return returnResult(lookupResultError(req, fmt.Errorf("unknown set operation child `%T` in check", child), emptyMetadata))
		}
	}
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		log.Ctx(ctx).Trace().Object("setOperation", req).Stringer("operation", so).Send()
		resultChan <- reducer(ctx, req, req.Limit, requests)
	}
}

func emptyLookup(ctx context.Context, req ValidatedLookupRequest) ReduceableLookupFunc {
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		resultChan <- lookupResult(req, []*core.ObjectAndRelation{}, emptyMetadata)
	}
}

func findRelation(nsdef *core.NamespaceDefinition, relationName string) (*core.Relation, bool) {
	for _, relation := range nsdef.Relation {
		if relation.Name == relationName {
			return relation, true
		}
	}

	return nil, false
}

func (cl *ConcurrentLookup) processTupleToUserset(ctx context.Context, req ValidatedLookupRequest, nsdef *core.NamespaceDefinition, typeSystem *namespace.TypeSystem, ttu *core.TupleToUserset) ReduceableLookupFunc {
	// Ensure that we don't process TTUs recursively, as that can cause an infinite loop.
	nr := &core.RelationReference{
		Namespace: req.ObjectRelation.Namespace,
		Relation:  req.ObjectRelation.Relation,
	}
	if checkStackContains(req.TtuStack, nr) {
		result := lookupResult(req, []*core.ObjectAndRelation{}, emptyMetadata)
		result.Resp.Metadata.LookupExcludedTtu = append(result.Resp.Metadata.LookupExcludedTtu, nr)
		return returnResult(result)
	}

	tuplesetDirectRelations, err := typeSystem.AllowedSubjectRelations(ttu.Tupleset.Relation)
	if err != nil {
		return returnResult(lookupResultError(req, err, emptyMetadata))
	}

	// Dispatch to all the accessible namespaces for the computed userset.
	requests := []ReduceableLookupFunc{}
	namespaces := map[string]bool{}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)

	for _, directRelation := range tuplesetDirectRelations {
		_, ok := namespaces[directRelation.Namespace]
		if ok {
			continue
		}

		_, directRelTypeSystem, err := namespace.ReadNamespaceAndTypes(ctx, directRelation.Namespace, ds)
		if err != nil {
			return returnResult(lookupResultError(req, err, emptyMetadata))
		}

		if !directRelTypeSystem.HasRelation(ttu.ComputedUserset.Relation) {
			continue
		}

		namespaces[directRelation.Namespace] = true

		// Bind the current direct relation.
		directRelation := directRelation

		requests = append(requests, func(ctx context.Context, resultChan chan<- LookupResult) {
			// Dispatch a request to perform the computed userset lookup.
			computedUsersetRequest := cl.dispatch(ValidatedLookupRequest{
				&v1.DispatchLookupRequest{
					Subject: req.Subject,
					ObjectRelation: &core.RelationReference{
						Namespace: directRelation.Namespace,
						Relation:  ttu.ComputedUserset.Relation,
					},
					Limit:       noLimit, // Since this is a step in the lookup.
					Metadata:    decrementDepth(req.Metadata),
					DirectStack: req.DirectStack,
					TtuStack:    append(req.TtuStack, nr),
				},
				req.Revision,
			})

			result := lookupAny(ctx, req, noLimit, []ReduceableLookupFunc{computedUsersetRequest})
			if result.Err != nil || len(result.Resp.ResolvedOnrs) == 0 {
				resultChan <- result
				return
			}

			// For each computed userset object, collect the usersets and then perform a tupleset lookup.
			usersets := []*core.ObjectAndRelation{}
			for _, resolvedObj := range result.Resp.ResolvedOnrs {
				// Determine the relation(s) to use or the tupleset. This is determined based on the allowed direct relations and
				// we always check for both the actual relation resolved, as well as `...`
				allowedRelations := []string{}
				allowedDirect, err := typeSystem.IsAllowedDirectRelation(ttu.Tupleset.Relation, resolvedObj.Namespace, resolvedObj.Relation)
				if err != nil {
					resultChan <- lookupResultError(req, err, result.Resp.Metadata)
					return
				}

				if allowedDirect == namespace.DirectRelationValid {
					allowedRelations = append(allowedRelations, resolvedObj.Relation)
				}

				if resolvedObj.Relation != Ellipsis {
					allowedEllipsis, err := typeSystem.IsAllowedDirectRelation(ttu.Tupleset.Relation, resolvedObj.Namespace, Ellipsis)
					if err != nil {
						resultChan <- lookupResultError(req, err, result.Resp.Metadata)
					}

					if allowedEllipsis == namespace.DirectRelationValid {
						allowedRelations = append(allowedRelations, Ellipsis)
					}
				}

				for _, allowedRelation := range allowedRelations {
					userset := &core.ObjectAndRelation{
						Namespace: resolvedObj.Namespace,
						ObjectId:  resolvedObj.ObjectId,
						Relation:  allowedRelation,
					}
					usersets = append(usersets, userset)
				}
			}

			// Perform the tupleset lookup.
			objects := tuple.NewONRSet()
			if len(usersets) > 0 {
				ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)
				limit := uint64(req.Limit)
				it, err := ds.QueryRelationships(
					ctx,
					&v1_proto.RelationshipFilter{
						ResourceType:     req.ObjectRelation.Namespace,
						OptionalRelation: ttu.Tupleset.Relation,
					},
					options.SetUsersets(usersets),
					options.WithLimit(&limit),
				)
				if err != nil {
					resultChan <- lookupResultError(req, err, result.Resp.Metadata)
					return
				}
				defer it.Close()

				for tpl := it.Next(); tpl != nil; tpl = it.Next() {
					if it.Err() != nil {
						resultChan <- lookupResultError(req, it.Err(), result.Resp.Metadata)
						return
					}

					if tpl.ObjectAndRelation.Namespace != req.ObjectRelation.Namespace {
						resultChan <- lookupResultError(req, fmt.Errorf("got unexpected namespace"), result.Resp.Metadata)
						return
					}

					objects.Add(&core.ObjectAndRelation{
						Namespace: req.ObjectRelation.Namespace,
						ObjectId:  tpl.ObjectAndRelation.ObjectId,
						Relation:  req.ObjectRelation.Relation,
					})

					if objects.Length() >= req.Limit {
						break
					}
				}
			}

			resultChan <- lookupResult(req, objects.AsSlice(), result.Resp.Metadata)
		})
	}

	return func(ctx context.Context, resultChan chan<- LookupResult) {
		result := lookupAny(ctx, req, req.Limit, requests)

		// Remove the TTU from the excluded list to mark it as fully resolved now.
		var lookupExcludedTtu []*core.RelationReference
		if result.Resp.Metadata.LookupExcludedTtu != nil {
			for _, rr := range result.Resp.Metadata.LookupExcludedTtu {
				if rr.Namespace == req.ObjectRelation.Namespace && rr.Relation == req.ObjectRelation.Relation {
					continue
				}

				lookupExcludedTtu = append(lookupExcludedTtu, rr)
			}
		}

		result.Resp.Metadata.LookupExcludedTtu = lookupExcludedTtu
		resultChan <- result
	}
}

func (cl *ConcurrentLookup) lookupComputed(ctx context.Context, req ValidatedLookupRequest, cu *core.ComputedUserset) ReduceableLookupFunc {
	result := lookupOne(ctx, req, cl.dispatch(ValidatedLookupRequest{
		&v1.DispatchLookupRequest{
			Subject: req.Subject,
			ObjectRelation: &core.RelationReference{
				Namespace: req.ObjectRelation.Namespace,
				Relation:  cu.Relation,
			},
			Limit:    req.Limit,
			Metadata: decrementDepth(req.Metadata),
			DirectStack: append(req.DirectStack, &core.RelationReference{
				Namespace: req.ObjectRelation.Namespace,
				Relation:  req.ObjectRelation.Relation,
			}),
			TtuStack: req.TtuStack,
		},
		req.Revision,
	}))

	if result.Err != nil {
		return returnResult(lookupResultError(req, result.Err, result.Resp.Metadata))
	}

	// Rewrite the found ONRs to be this relation.
	rewrittenResolved := make([]*core.ObjectAndRelation, 0, len(result.Resp.ResolvedOnrs))
	for _, resolved := range result.Resp.ResolvedOnrs {
		if resolved.Namespace != req.ObjectRelation.Namespace {
			return returnResult(lookupResultError(req,
				fmt.Errorf(
					"invalid namespace: %s vs %s",
					tuple.StringONR(resolved),
					req.ObjectRelation.Namespace,
				),
				result.Resp.Metadata,
			))
		}

		rewrittenResolved = append(rewrittenResolved,
			&core.ObjectAndRelation{
				Namespace: resolved.Namespace,
				Relation:  req.ObjectRelation.Relation,
				ObjectId:  resolved.ObjectId,
			})
	}

	return returnResult(lookupResult(req, rewrittenResolved, result.Resp.Metadata))
}

func (cl *ConcurrentLookup) dispatch(req ValidatedLookupRequest) ReduceableLookupFunc {
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		log.Ctx(ctx).Trace().Object("dispatchLookup", req).Send()
		result, err := cl.d.DispatchLookup(ctx, req.DispatchLookupRequest)
		resultChan <- LookupResult{result, err}
	}
}

func lookupOne(ctx context.Context, parentReq ValidatedLookupRequest, request ReduceableLookupFunc) LookupResult {
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	resultChan := make(chan LookupResult, 1)
	go request(childCtx, resultChan)

	select {
	case result := <-resultChan:
		return result
	case <-ctx.Done():
		return lookupResultError(parentReq, NewRequestCanceledErr(), emptyMetadata)
	}
}

func lookupAnyWithExcludedDirect(ctx context.Context, parentReq ValidatedLookupRequest, limit uint32, requests []ReduceableLookupFunc, excludedDirect []*core.RelationReference) LookupResult {
	result := lookupAny(ctx, parentReq, limit, requests)
	result.Resp.Metadata.LookupExcludedDirect = excludedDirect
	return result
}

func lookupAny(ctx context.Context, parentReq ValidatedLookupRequest, limit uint32, requests []ReduceableLookupFunc) LookupResult {
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	resultChans := make([]chan LookupResult, 0, len(requests))
	for _, req := range requests {
		resultChan := make(chan LookupResult, 1)
		resultChans = append(resultChans, resultChan)
		go req(childCtx, resultChan)
	}

	objects := tuple.NewONRSet()

	responseMetadata := emptyMetadata
	for _, resultChan := range resultChans {
		select {
		case result := <-resultChan:
			responseMetadata = combineResponseMetadata(responseMetadata, result.Resp.Metadata)
			if result.Err != nil {
				return lookupResultError(parentReq, result.Err, responseMetadata)
			}

			objects.Update(result.Resp.ResolvedOnrs)

			if objects.Length() >= limit {
				return lookupResult(parentReq, limitedSlice(objects.AsSlice(), limit), responseMetadata)
			}
		case <-ctx.Done():
			return lookupResultError(parentReq, NewRequestCanceledErr(), responseMetadata)
		}
	}

	return lookupResult(parentReq, limitedSlice(objects.AsSlice(), limit), responseMetadata)
}

func lookupAll(ctx context.Context, parentReq ValidatedLookupRequest, limit uint32, requests []ReduceableLookupFunc) LookupResult {
	if len(requests) == 0 {
		return lookupResult(parentReq, []*core.ObjectAndRelation{}, emptyMetadata)
	}

	resultChan := make(chan LookupResult, len(requests))
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	for _, req := range requests {
		go req(childCtx, resultChan)
	}

	objSet := tuple.NewONRSet()
	responseMetadata := emptyMetadata

	for i := 0; i < len(requests); i++ {
		select {
		case result := <-resultChan:
			responseMetadata = combineResponseMetadata(responseMetadata, result.Resp.Metadata)

			if result.Err != nil {
				return lookupResultError(parentReq, result.Err, responseMetadata)
			}

			subSet := tuple.NewONRSet()
			subSet.Update(result.Resp.ResolvedOnrs)

			if i == 0 {
				objSet = subSet
			} else {
				objSet = objSet.Intersect(subSet)
			}

			if objSet.Length() == 0 {
				return lookupResult(parentReq, []*core.ObjectAndRelation{}, responseMetadata)
			}
		case <-ctx.Done():
			return lookupResultError(parentReq, NewRequestCanceledErr(), responseMetadata)
		}
	}

	return lookupResult(parentReq, objSet.AsSlice(), responseMetadata)
}

func lookupExclude(ctx context.Context, parentReq ValidatedLookupRequest, limit uint32, requests []ReduceableLookupFunc) LookupResult {
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	baseChan := make(chan LookupResult, 1)
	othersChan := make(chan LookupResult, len(requests)-1)

	go requests[0](childCtx, baseChan)
	for _, req := range requests[1:] {
		go req(childCtx, othersChan)
	}

	objSet := tuple.NewONRSet()
	excSet := tuple.NewONRSet()

	responseMetadata := emptyMetadata

	for i := 0; i < len(requests); i++ {
		select {
		case base := <-baseChan:
			responseMetadata = combineResponseMetadata(responseMetadata, base.Resp.Metadata)

			if base.Err != nil {
				return lookupResultError(parentReq, base.Err, responseMetadata)
			}
			objSet.Update(base.Resp.ResolvedOnrs)

		case sub := <-othersChan:
			responseMetadata = combineResponseMetadata(responseMetadata, sub.Resp.Metadata)

			if sub.Err != nil {
				return lookupResultError(parentReq, sub.Err, responseMetadata)
			}

			excSet.Update(sub.Resp.ResolvedOnrs)
		case <-ctx.Done():
			return lookupResultError(parentReq, NewRequestCanceledErr(), responseMetadata)
		}
	}

	return lookupResult(parentReq, limitedSlice(objSet.Subtract(excSet).AsSlice(), limit), responseMetadata)
}

func returnResult(result LookupResult) ReduceableLookupFunc {
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		resultChan <- result
	}
}

func limitedSlice(slice []*core.ObjectAndRelation, limit uint32) []*core.ObjectAndRelation {
	if len(slice) > int(limit) {
		return slice[0:limit]
	}

	return slice
}

func lookupResult(req ValidatedLookupRequest, resolvedONRs []*core.ObjectAndRelation, subProblemMetadata *v1.ResponseMeta) LookupResult {
	return LookupResult{
		&v1.DispatchLookupResponse{
			Metadata:     ensureMetadata(subProblemMetadata),
			ResolvedOnrs: resolvedONRs,
		},
		nil,
	}
}

func lookupResultError(req ValidatedLookupRequest, err error, subProblemMetadata *v1.ResponseMeta) LookupResult {
	return LookupResult{
		&v1.DispatchLookupResponse{
			Metadata: ensureMetadata(subProblemMetadata),
		},
		err,
	}
}

func checkStackContains(stack []*core.RelationReference, target *core.RelationReference) bool {
	for _, v := range stack {
		if v.Namespace == target.Namespace && v.Relation == target.Relation {
			return true
		}
	}
	return false
}
