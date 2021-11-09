package graph

import (
	"context"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// NewConcurrentLookup creates and instance of ConcurrentLookup.
func NewConcurrentLookup(d dispatch.Lookup, ds datastore.GraphDatastore, nsm namespace.Manager) *ConcurrentLookup {
	return &ConcurrentLookup{d: d, ds: ds, nsm: nsm}
}

// ConcurrentLookup exposes a method to perform Lookup requests, and delegates subproblems to the
// provided dispatch.Lookup instance.
type ConcurrentLookup struct {
	d   dispatch.Lookup
	ds  datastore.GraphDatastore
	nsm namespace.Manager
}

// Calculate the maximum int value to allow us to effectively set no limit on certain recursive
// lookup calls.
const (
	noLimit = ^uint32(0)
)

// Lookup performs a lookup request with the provided request and context.
func (cl *ConcurrentLookup) Lookup(ctx context.Context, req *v1.DispatchLookupRequest) (*v1.DispatchLookupResponse, error) {
	funcToResolve := cl.lookupInternal(ctx, req)
	resolved := lookupOne(ctx, req, funcToResolve)

	// Remove the resolved relation reference from the excluded direct list to mark that it was completely resolved.
	var lookupExcludedDirect []*v0.RelationReference
	if resolved.Resp.Metadata.LookupExcludedDirect != nil {
		for _, rr := range resolved.Resp.Metadata.LookupExcludedDirect {
			if rr.Namespace == req.ObjectRelation.Namespace && rr.Relation == req.ObjectRelation.Relation {
				continue
			}

			lookupExcludedDirect = append(lookupExcludedDirect, rr)
		}
	}

	resolved.Resp.Metadata.LookupExcludedDirect = lookupExcludedDirect
	return resolved.Resp, resolved.Err
}

var emptyMetadata = &v1.ResponseMeta{}

func (cl *ConcurrentLookup) lookupInternal(ctx context.Context, req *v1.DispatchLookupRequest) ReduceableLookupFunc {
	log.Ctx(ctx).Trace().Object("lookup", req).Send()

	objSet := tuple.NewONRSet()

	// If we've found the target ONR, add it to the set of resolved objects. Note that we still need
	// to continue processing, as this may also be an intermediate step in resolution.
	if req.ObjectRelation.Namespace == req.Subject.Namespace && req.ObjectRelation.Relation == req.Subject.Relation {
		objSet.Add(req.Subject)
	}

	nsdef, typeSystem, _, err := cl.nsm.ReadNamespaceAndTypes(ctx, req.ObjectRelation.Namespace)
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
			if obj.Namespace == req.Subject.Namespace &&
				obj.Relation == req.Subject.Relation &&
				obj.ObjectId == req.Subject.ObjectId {
				continue
			}

			requests = append(requests, cl.dispatch(&v1.DispatchLookupRequest{
				Subject:        obj,
				ObjectRelation: req.ObjectRelation,
				Limit:          req.Limit - objSet.Length(),
				Metadata:       decrementDepth(req.Metadata),
				DirectStack:    req.DirectStack,
				TtuStack:       req.TtuStack,
			}))
		}

		result := lookupAny(ctx, req, req.Limit, requests)
		responseMetadata = updateRespMetadata(responseMetadata, result.Resp)

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

func (cl *ConcurrentLookup) lookupDirect(ctx context.Context, req *v1.DispatchLookupRequest, typeSystem *namespace.NamespaceTypeSystem) ReduceableLookupFunc {
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

	requestRevision, err := decimal.NewFromString(req.Metadata.AtRevision)
	if err != nil {
		return returnResult(lookupResultError(req, err, emptyMetadata))
	}

	if isDirectAllowed == namespace.DirectRelationValid {
		requests = append(requests, func(ctx context.Context, resultChan chan<- LookupResult) {
			objects := tuple.NewONRSet()
			it, err := cl.ds.ReverseQueryTuplesFromSubject(req.Subject, requestRevision).
				WithObjectRelation(req.ObjectRelation.Namespace, req.ObjectRelation.Relation).
				Execute(ctx)
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

	// Dispatch to any allowed direct relation types that don't match the target ONR, collect
	// the found object IDs, and then search for those.
	allowedDirect, err := typeSystem.AllowedDirectRelations(req.ObjectRelation.Relation)
	if err != nil {
		return returnResult(lookupResultError(req, err, emptyMetadata))
	}

	directStack := append(req.DirectStack, &v0.RelationReference{
		Namespace: req.ObjectRelation.Namespace,
		Relation:  req.ObjectRelation.Relation,
	})

	var excludedDirect []*v0.RelationReference
	for _, allowedDirectType := range allowedDirect {
		if allowedDirectType.Relation == Ellipsis {
			continue
		}

		if allowedDirectType.Namespace == req.ObjectRelation.Namespace &&
			allowedDirectType.Relation == req.ObjectRelation.Relation {
			continue
		}

		// Prevent recursive inferred lookups, which can cause an infinite loop.
		rr := &v0.RelationReference{
			Namespace: allowedDirectType.Namespace,
			Relation:  allowedDirectType.Relation,
		}
		if checkStackContains(directStack, rr) {
			excludedDirect = append(excludedDirect, rr)
			continue
		}

		// Bind to the current allowed direct type
		allowedDirectType := allowedDirectType

		requests = append(requests, func(ctx context.Context, resultChan chan<- LookupResult) {
			// Dispatch on the inferred relation.
			inferredRequest := cl.dispatch(&v1.DispatchLookupRequest{
				Subject: req.Subject,
				ObjectRelation: &v0.RelationReference{
					Namespace: allowedDirectType.Namespace,
					Relation:  allowedDirectType.Relation,
				},
				Limit:       noLimit, // Since this is an inferred lookup, we can't limit.
				Metadata:    decrementDepth(req.Metadata),
				DirectStack: directStack,
				TtuStack:    req.TtuStack,
			})

			result := lookupAny(ctx, req, noLimit, []ReduceableLookupFunc{inferredRequest})
			if result.Err != nil {
				resultChan <- result
				return
			}

			// For each inferred object found, check for the target ONR.
			objects := tuple.NewONRSet()
			if len(result.Resp.ResolvedOnrs) > 0 {
				it, err := cl.ds.QueryTuples(datastore.TupleQueryResourceFilter{
					ResourceType:             req.ObjectRelation.Namespace,
					OptionalResourceRelation: req.ObjectRelation.Relation,
				}, requestRevision).WithUsersets(result.Resp.ResolvedOnrs).Limit(uint64(req.Limit)).Execute(ctx)
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

func (cl *ConcurrentLookup) processRewrite(ctx context.Context, req *v1.DispatchLookupRequest, nsdef *v0.NamespaceDefinition, typeSystem *namespace.NamespaceTypeSystem, usr *v0.UsersetRewrite) ReduceableLookupFunc {
	switch rw := usr.RewriteOperation.(type) {
	case *v0.UsersetRewrite_Union:
		return cl.processSetOperation(ctx, req, nsdef, typeSystem, rw.Union, lookupAny)
	case *v0.UsersetRewrite_Intersection:
		return cl.processSetOperation(ctx, req, nsdef, typeSystem, rw.Intersection, lookupAll)
	case *v0.UsersetRewrite_Exclusion:
		return cl.processSetOperation(ctx, req, nsdef, typeSystem, rw.Exclusion, lookupExclude)
	default:
		return returnResult(lookupResultError(req, fmt.Errorf("unknown userset rewrite kind under `%s#%s`", req.ObjectRelation.Namespace, req.ObjectRelation.Relation), emptyMetadata))
	}
}

func (cl *ConcurrentLookup) processSetOperation(ctx context.Context, req *v1.DispatchLookupRequest, nsdef *v0.NamespaceDefinition, typeSystem *namespace.NamespaceTypeSystem, so *v0.SetOperation, reducer LookupReducer) ReduceableLookupFunc {
	var requests []ReduceableLookupFunc

	for _, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *v0.SetOperation_Child_XThis:
			requests = append(requests, cl.lookupDirect(ctx, req, typeSystem))
		case *v0.SetOperation_Child_ComputedUserset:
			requests = append(requests, cl.lookupComputed(ctx, req, child.ComputedUserset))
		case *v0.SetOperation_Child_UsersetRewrite:
			requests = append(requests, cl.processRewrite(ctx, req, nsdef, typeSystem, child.UsersetRewrite))
		case *v0.SetOperation_Child_TupleToUserset:
			requests = append(requests, cl.processTupleToUserset(ctx, req, nsdef, typeSystem, child.TupleToUserset))
		default:
			return returnResult(lookupResultError(req, fmt.Errorf("unknown set operation child"), emptyMetadata))
		}
	}
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		log.Ctx(ctx).Trace().Object("set operation", req).Stringer("operation", so).Send()
		resultChan <- reducer(ctx, req, req.Limit, requests)
	}
}

func findRelation(nsdef *v0.NamespaceDefinition, relationName string) (*v0.Relation, bool) {
	for _, relation := range nsdef.Relation {
		if relation.Name == relationName {
			return relation, true
		}
	}

	return nil, false
}

func (cl *ConcurrentLookup) processTupleToUserset(ctx context.Context, req *v1.DispatchLookupRequest, nsdef *v0.NamespaceDefinition, typeSystem *namespace.NamespaceTypeSystem, ttu *v0.TupleToUserset) ReduceableLookupFunc {
	// Ensure that we don't process TTUs recursively, as that can cause an infinite loop.
	nr := &v0.RelationReference{
		Namespace: req.ObjectRelation.Namespace,
		Relation:  req.ObjectRelation.Relation,
	}
	if checkStackContains(req.TtuStack, nr) {
		result := lookupResult(req, []*v0.ObjectAndRelation{}, emptyMetadata)
		result.Resp.Metadata.LookupExcludedTtu = append(result.Resp.Metadata.LookupExcludedTtu, nr)
		return returnResult(result)
	}

	tuplesetDirectRelations, err := typeSystem.AllowedDirectRelations(ttu.Tupleset.Relation)
	if err != nil {
		return returnResult(lookupResultError(req, err, emptyMetadata))
	}

	requestRevision, err := decimal.NewFromString(req.Metadata.AtRevision)
	if err != nil {
		return returnResult(lookupResultError(req, err, emptyMetadata))
	}

	// Dispatch to all the accessible namespaces for the computed userset.
	requests := []ReduceableLookupFunc{}
	namespaces := map[string]bool{}

	for _, directRelation := range tuplesetDirectRelations {
		_, ok := namespaces[directRelation.Namespace]
		if ok {
			continue
		}

		_, directRelTypeSystem, _, err := cl.nsm.ReadNamespaceAndTypes(ctx, directRelation.Namespace)
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
			computedUsersetRequest := cl.dispatch(&v1.DispatchLookupRequest{
				Subject: req.Subject,
				ObjectRelation: &v0.RelationReference{
					Namespace: directRelation.Namespace,
					Relation:  ttu.ComputedUserset.Relation,
				},
				Limit:       noLimit, // Since this is a step in the lookup.
				Metadata:    decrementDepth(req.Metadata),
				DirectStack: req.DirectStack,
				TtuStack:    append(req.TtuStack, nr),
			})

			result := lookupAny(ctx, req, noLimit, []ReduceableLookupFunc{computedUsersetRequest})
			if result.Err != nil || len(result.Resp.ResolvedOnrs) == 0 {
				resultChan <- result
				return
			}

			// For each computed userset object, collect the usersets and then perform a tupleset lookup.
			usersets := []*v0.ObjectAndRelation{}
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
					userset := &v0.ObjectAndRelation{
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
				it, err := cl.ds.QueryTuples(datastore.TupleQueryResourceFilter{
					ResourceType:             req.ObjectRelation.Namespace,
					OptionalResourceRelation: ttu.Tupleset.Relation,
				}, requestRevision).WithUsersets(usersets).Limit(uint64(req.Limit)).Execute(ctx)
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

					objects.Add(&v0.ObjectAndRelation{
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
		var lookupExcludedTtu []*v0.RelationReference
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

func (cl *ConcurrentLookup) lookupComputed(ctx context.Context, req *v1.DispatchLookupRequest, cu *v0.ComputedUserset) ReduceableLookupFunc {
	result := lookupOne(ctx, req, cl.dispatch(&v1.DispatchLookupRequest{
		Subject: req.Subject,
		ObjectRelation: &v0.RelationReference{
			Namespace: req.ObjectRelation.Namespace,
			Relation:  cu.Relation,
		},
		Limit:    req.Limit,
		Metadata: decrementDepth(req.Metadata),
		DirectStack: append(req.DirectStack, &v0.RelationReference{
			Namespace: req.ObjectRelation.Namespace,
			Relation:  req.ObjectRelation.Relation,
		}),
		TtuStack: req.TtuStack,
	}))

	if result.Err != nil {
		return returnResult(lookupResultError(req, result.Err, result.Resp.Metadata))
	}

	// Rewrite the found ONRs to be this relation.
	rewrittenResolved := make([]*v0.ObjectAndRelation, 0, len(result.Resp.ResolvedOnrs))
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
			&v0.ObjectAndRelation{
				Namespace: resolved.Namespace,
				Relation:  req.ObjectRelation.Relation,
				ObjectId:  resolved.ObjectId,
			})
	}

	return returnResult(lookupResult(req, rewrittenResolved, result.Resp.Metadata))
}

func (cl *ConcurrentLookup) dispatch(req *v1.DispatchLookupRequest) ReduceableLookupFunc {
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		log.Ctx(ctx).Trace().Object("dispatch lookup", req).Send()
		result, err := cl.d.DispatchLookup(ctx, req)
		resultChan <- LookupResult{result, err}
	}
}

func lookupOne(ctx context.Context, parentReq *v1.DispatchLookupRequest, request ReduceableLookupFunc) LookupResult {
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

func lookupAnyWithExcludedDirect(ctx context.Context, parentReq *v1.DispatchLookupRequest, limit uint32, requests []ReduceableLookupFunc, excludedDirect []*v0.RelationReference) LookupResult {
	result := lookupAny(ctx, parentReq, limit, requests)
	result.Resp.Metadata.LookupExcludedDirect = excludedDirect
	return result
}

func lookupAny(ctx context.Context, parentReq *v1.DispatchLookupRequest, limit uint32, requests []ReduceableLookupFunc) LookupResult {
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
			responseMetadata = updateRespMetadata(responseMetadata, result.Resp)
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

func lookupAll(ctx context.Context, parentReq *v1.DispatchLookupRequest, limit uint32, requests []ReduceableLookupFunc) LookupResult {
	if len(requests) == 0 {
		return lookupResult(parentReq, []*v0.ObjectAndRelation{}, emptyMetadata)
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
			responseMetadata = updateRespMetadata(responseMetadata, result.Resp)

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
				return lookupResult(parentReq, []*v0.ObjectAndRelation{}, responseMetadata)
			}
		case <-ctx.Done():
			return lookupResultError(parentReq, NewRequestCanceledErr(), responseMetadata)
		}
	}

	return lookupResult(parentReq, objSet.AsSlice(), responseMetadata)
}

func lookupExclude(ctx context.Context, parentReq *v1.DispatchLookupRequest, limit uint32, requests []ReduceableLookupFunc) LookupResult {
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
			responseMetadata = updateRespMetadata(responseMetadata, base.Resp)

			if base.Err != nil {
				return lookupResultError(parentReq, base.Err, responseMetadata)
			}
			objSet.Update(base.Resp.ResolvedOnrs)

		case sub := <-othersChan:
			responseMetadata = updateRespMetadata(responseMetadata, sub.Resp)

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

func updateRespMetadata(existing *v1.ResponseMeta, response *v1.DispatchLookupResponse) *v1.ResponseMeta {
	lookupExcludedDirect := existing.LookupExcludedDirect
	if response.Metadata.LookupExcludedDirect != nil {
		lookupExcludedDirect = append(lookupExcludedDirect, response.Metadata.LookupExcludedDirect...)
	}

	lookupExcludedTtu := existing.LookupExcludedTtu
	if response.Metadata.LookupExcludedTtu != nil {
		lookupExcludedTtu = append(lookupExcludedTtu, response.Metadata.LookupExcludedTtu...)
	}

	return &v1.ResponseMeta{
		DispatchCount:        existing.DispatchCount + response.Metadata.DispatchCount,
		DepthRequired:        max(existing.DepthRequired, response.Metadata.DepthRequired),
		LookupExcludedDirect: lookupExcludedDirect,
		LookupExcludedTtu:    lookupExcludedTtu,
	}
}

func returnResult(result LookupResult) ReduceableLookupFunc {
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		resultChan <- result
	}
}

func limitedSlice(slice []*v0.ObjectAndRelation, limit uint32) []*v0.ObjectAndRelation {
	if len(slice) > int(limit) {
		return slice[0:limit]
	}

	return slice
}

func lookupResult(req *v1.DispatchLookupRequest, resolvedONRs []*v0.ObjectAndRelation, metadata *v1.ResponseMeta) LookupResult {
	if metadata == nil {
		metadata = emptyMetadata
	}

	return LookupResult{
		&v1.DispatchLookupResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount:        metadata.DispatchCount,
				DepthRequired:        metadata.DepthRequired + 1, // +1 for the current call,
				LookupExcludedDirect: metadata.LookupExcludedDirect,
				LookupExcludedTtu:    metadata.LookupExcludedTtu,
			},
			ResolvedOnrs: resolvedONRs,
		},
		nil,
	}
}

func lookupResultError(req *v1.DispatchLookupRequest, err error, metadata *v1.ResponseMeta) LookupResult {
	if metadata == nil {
		metadata = emptyMetadata
	}

	return LookupResult{
		&v1.DispatchLookupResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: metadata.DispatchCount,
				DepthRequired: metadata.DepthRequired + 1, // +1 for the current call
			},
		},
		err,
	}
}

func checkStackContains(stack []*v0.RelationReference, target *v0.RelationReference) bool {
	for _, v := range stack {
		if v.Namespace == target.Namespace && v.Relation == target.Relation {
			return true
		}
	}
	return false
}
