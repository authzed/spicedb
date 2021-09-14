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
	v1_api "github.com/authzed/spicedb/internal/proto/authzed/api/v1"
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
	resolved := lookupOne(ctx, funcToResolve)
	return resolved.Resp, resolved.Err
}

func (cl *ConcurrentLookup) lookupInternal(ctx context.Context, req *v1.DispatchLookupRequest) ReduceableLookupFunc {
	log.Trace().Object("lookup", req).Send()

	objSet := tuple.NewONRSet()

	// If we've found the target ONR, add it to the set of resolved objects. Note that we still need
	// to continue processing, as this may also be an intermediate step in resolution.
	if req.ObjectRelation.Namespace == req.Subject.Namespace && req.ObjectRelation.Relation == req.Subject.Relation {
		objSet.Add(req.Subject)
	}

	nsdef, typeSystem, _, err := cl.nsm.ReadNamespaceAndTypes(ctx, req.ObjectRelation.Namespace)
	if err != nil {
		return returnResult(lookupResultError(err, 0))
	}

	relation, ok := findRelation(nsdef, req.ObjectRelation.Relation)
	if !ok {
		return returnResult(lookupResultError(
			fmt.Errorf(
				"relation `%s` not found under namespace `%s`",
				req.ObjectRelation.Relation,
				req.ObjectRelation.Namespace,
			),
			0,
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
	result := lookupAny(ctx, req.Limit, []ReduceableLookupFunc{request})
	if result.Err != nil {
		return returnResult(lookupResultError(result.Err, result.Resp.Metadata.DispatchCount))
	}

	objSet.Update(result.Resp.ResolvedOnrs)

	// Recursively perform lookup on any of the ONRs found that do not match the target ONR.
	// This ensures that we resolve the full transitive closure of all objects.
	totalRequestCount := result.Resp.Metadata.DispatchCount
	toCheck := objSet
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

		result := lookupAny(ctx, req.Limit, requests)
		totalRequestCount += result.Resp.Metadata.DispatchCount
		if result.Err != nil {
			return returnResult(lookupResultError(result.Err, totalRequestCount))
		}

		toCheck = tuple.NewONRSet()

		for _, obj := range result.Resp.ResolvedOnrs {
			// Only check recursively for new objects.
			if objSet.Add(obj) {
				toCheck.Add(obj)
			}
		}
	}

	return returnResult(lookupResult(limitedSlice(objSet.AsSlice(), req.Limit), totalRequestCount))
}

func (cl *ConcurrentLookup) lookupDirect(ctx context.Context, req *v1.DispatchLookupRequest, typeSystem *namespace.NamespaceTypeSystem) ReduceableLookupFunc {
	requests := []ReduceableLookupFunc{}

	// Dispatch a check for the target ONR directly, if it is allowed on the start relation.
	isDirectAllowed, err := typeSystem.IsAllowedDirectRelation(req.ObjectRelation.Relation, req.Subject.Namespace, req.Subject.Relation)
	if err != nil {
		return returnResult(lookupResultError(err, 0))
	}

	requestRevision, err := decimal.NewFromString(req.Metadata.AtRevision)
	if err != nil {
		return returnResult(lookupResultError(err, 0))
	}

	if isDirectAllowed == namespace.DirectRelationValid {
		requests = append(requests, func(ctx context.Context, resultChan chan<- LookupResult) {
			objects := tuple.NewONRSet()
			it, err := cl.ds.ReverseQueryTuplesFromSubject(req.Subject, requestRevision).
				WithObjectRelation(req.ObjectRelation.Namespace, req.ObjectRelation.Relation).
				Execute(ctx)
			if err != nil {
				resultChan <- lookupResultError(err, 1)
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
				resultChan <- lookupResultError(it.Err(), 1)
				return
			}

			resultChan <- lookupResult(objects.AsSlice(), 1)
		})
	}

	// Dispatch to any allowed direct relation types that don't match the target ONR, collect
	// the found object IDs, and then search for those.
	allowedDirect, err := typeSystem.AllowedDirectRelations(req.ObjectRelation.Relation)
	if err != nil {
		return returnResult(lookupResultError(err, 0))
	}

	directStack := append(req.DirectStack, &v0.ObjectAndRelation{
		Namespace: req.ObjectRelation.Namespace,
		Relation:  req.ObjectRelation.Relation,
		ObjectId:  "",
	})

	for _, allowedDirectType := range allowedDirect {
		if allowedDirectType.Relation == Ellipsis {
			continue
		}

		if allowedDirectType.Namespace == req.ObjectRelation.Namespace &&
			allowedDirectType.Relation == req.ObjectRelation.Relation {
			continue
		}

		// Prevent recursive inferred lookups, which can cause an infinite loop.
		onr := &v0.ObjectAndRelation{
			Namespace: allowedDirectType.Namespace,
			Relation:  allowedDirectType.Relation,
			ObjectId:  "",
		}
		if checkStackContains(directStack, onr) {
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

			result := lookupAny(ctx, noLimit, []ReduceableLookupFunc{inferredRequest})
			if result.Err != nil {
				resultChan <- result
				return
			}

			// For each inferred object found, check for the target ONR.
			objects := tuple.NewONRSet()
			if len(result.Resp.ResolvedOnrs) > 0 {
				it, err := cl.ds.QueryTuples(&v1_api.ObjectFilter{
					ObjectType:       req.ObjectRelation.Namespace,
					OptionalRelation: req.ObjectRelation.Relation,
				}, requestRevision).
					WithUsersets(result.Resp.ResolvedOnrs).
					Limit(uint64(req.Limit)).
					Execute(ctx)
				if err != nil {
					resultChan <- lookupResultError(err, 1)
					return
				}
				defer it.Close()

				for tpl := it.Next(); tpl != nil; tpl = it.Next() {
					if it.Err() != nil {
						resultChan <- lookupResultError(it.Err(), 1)
						return
					}

					objects.Add(tpl.ObjectAndRelation)
					if objects.Length() >= req.Limit {
						break
					}
				}
			}

			resultChan <- lookupResult(objects.AsSlice(), 1)
		})
	}

	return func(ctx context.Context, resultChan chan<- LookupResult) {
		resultChan <- lookupAny(ctx, req.Limit, requests)
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
		return returnResult(lookupResultError(fmt.Errorf("unknown userset rewrite kind under `%s#%s`", req.ObjectRelation.Namespace, req.ObjectRelation.Relation), 0))
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
			return returnResult(lookupResultError(fmt.Errorf("unknown set operation child"), 0))
		}
	}
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		log.Trace().Object("set operation", req).Stringer("operation", so).Send()
		resultChan <- reducer(ctx, req.Limit, requests)
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
	onr := &v0.ObjectAndRelation{
		Namespace: req.ObjectRelation.Namespace,
		Relation:  req.ObjectRelation.Relation,
		ObjectId:  "",
	}
	if checkStackContains(req.TtuStack, onr) {
		return returnResult(lookupResult([]*v0.ObjectAndRelation{}, 0))
	}

	tuplesetDirectRelations, err := typeSystem.AllowedDirectRelations(ttu.Tupleset.Relation)
	if err != nil {
		return returnResult(lookupResultError(err, 0))
	}

	requestRevision, err := decimal.NewFromString(req.Metadata.AtRevision)
	if err != nil {
		return returnResult(lookupResultError(err, 0))
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
			return returnResult(lookupResultError(err, 0))
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
				TtuStack:    append(req.TtuStack, onr),
			})

			result := lookupAny(ctx, noLimit, []ReduceableLookupFunc{computedUsersetRequest})
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
					resultChan <- lookupResultError(err, 0)
					return
				}

				if allowedDirect == namespace.DirectRelationValid {
					allowedRelations = append(allowedRelations, resolvedObj.Relation)
				}

				if resolvedObj.Relation != Ellipsis {
					allowedEllipsis, err := typeSystem.IsAllowedDirectRelation(ttu.Tupleset.Relation, resolvedObj.Namespace, Ellipsis)
					if err != nil {
						resultChan <- lookupResultError(err, 0)
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
				it, err := cl.ds.QueryTuples(&v1_api.ObjectFilter{
					ObjectType:       req.ObjectRelation.Namespace,
					OptionalRelation: ttu.Tupleset.Relation,
				}, requestRevision).
					WithUsersets(usersets).
					Limit(uint64(req.Limit)).
					Execute(ctx)
				if err != nil {
					resultChan <- lookupResultError(err, 1)
					return
				}
				defer it.Close()

				for tpl := it.Next(); tpl != nil; tpl = it.Next() {
					if it.Err() != nil {
						resultChan <- lookupResultError(it.Err(), 1)
						return
					}

					if tpl.ObjectAndRelation.Namespace != req.ObjectRelation.Namespace {
						resultChan <- lookupResultError(fmt.Errorf("got unexpected namespace"), 1)
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

			resultChan <- lookupResult(objects.AsSlice(), 1)
		})
	}

	return func(ctx context.Context, resultChan chan<- LookupResult) {
		resultChan <- lookupAny(ctx, req.Limit, requests)
	}
}

func (cl *ConcurrentLookup) lookupComputed(ctx context.Context, req *v1.DispatchLookupRequest, cu *v0.ComputedUserset) ReduceableLookupFunc {
	result := lookupOne(ctx, cl.dispatch(&v1.DispatchLookupRequest{
		Subject: req.Subject,
		ObjectRelation: &v0.RelationReference{
			Namespace: req.ObjectRelation.Namespace,
			Relation:  cu.Relation,
		},
		Limit:    req.Limit,
		Metadata: decrementDepth(req.Metadata),
		DirectStack: append(req.DirectStack, &v0.ObjectAndRelation{
			Namespace: req.ObjectRelation.Namespace,
			Relation:  req.ObjectRelation.Relation,
			ObjectId:  "",
		}),
		TtuStack: req.TtuStack,
	}))

	if result.Err != nil {
		return returnResult(lookupResultError(result.Err, result.Resp.Metadata.DispatchCount))
	}

	// Rewrite the found ONRs to be this relation.
	rewrittenResolved := make([]*v0.ObjectAndRelation, 0, len(result.Resp.ResolvedOnrs))
	for _, resolved := range result.Resp.ResolvedOnrs {
		if resolved.Namespace != req.ObjectRelation.Namespace {
			return returnResult(lookupResultError(
				fmt.Errorf(
					"invalid namespace: %s vs %s",
					tuple.StringONR(resolved),
					req.ObjectRelation.Namespace,
				),
				result.Resp.Metadata.DispatchCount,
			))
		}

		rewrittenResolved = append(rewrittenResolved,
			&v0.ObjectAndRelation{
				Namespace: resolved.Namespace,
				Relation:  req.ObjectRelation.Relation,
				ObjectId:  resolved.ObjectId,
			})
	}

	return returnResult(lookupResult(rewrittenResolved, result.Resp.Metadata.DispatchCount))
}

func (cl *ConcurrentLookup) dispatch(req *v1.DispatchLookupRequest) ReduceableLookupFunc {
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		log.Trace().Object("dispatch lookup", req).Send()
		result, err := cl.d.DispatchLookup(ctx, req)
		resultChan <- LookupResult{result, err}
	}
}

func lookupOne(ctx context.Context, request ReduceableLookupFunc) LookupResult {
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	resultChan := make(chan LookupResult)
	go request(childCtx, resultChan)

	select {
	case result := <-resultChan:
		return result
	case <-ctx.Done():
		return lookupResultError(NewRequestCanceledErr(), 0)
	}
}

func lookupAny(ctx context.Context, limit uint32, requests []ReduceableLookupFunc) LookupResult {
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	resultChans := make([]chan LookupResult, 0, len(requests))
	for _, req := range requests {
		resultChan := make(chan LookupResult)
		resultChans = append(resultChans, resultChan)
		go req(childCtx, resultChan)
	}

	objects := tuple.NewONRSet()
	var totalRequestCount uint32
	for _, resultChan := range resultChans {
		select {
		case result := <-resultChan:
			totalRequestCount += result.Resp.Metadata.DispatchCount
			if result.Err != nil {
				return lookupResultError(result.Err, totalRequestCount)
			}

			objects.Update(result.Resp.ResolvedOnrs)

			if objects.Length() >= limit {
				return lookupResult(limitedSlice(objects.AsSlice(), limit), totalRequestCount)
			}
		case <-ctx.Done():
			return lookupResultError(NewRequestCanceledErr(), totalRequestCount)
		}
	}

	return lookupResult(limitedSlice(objects.AsSlice(), limit), totalRequestCount)
}

func lookupAll(ctx context.Context, limit uint32, requests []ReduceableLookupFunc) LookupResult {
	if len(requests) == 0 {
		return lookupResult([]*v0.ObjectAndRelation{}, 0)
	}

	resultChan := make(chan LookupResult, len(requests))
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	for _, req := range requests {
		go req(childCtx, resultChan)
	}

	objSet := tuple.NewONRSet()
	var totalRequestCount uint32
	for i := 0; i < len(requests); i++ {
		select {
		case result := <-resultChan:
			totalRequestCount += result.Resp.Metadata.DispatchCount
			if result.Err != nil {
				return lookupResultError(result.Err, totalRequestCount)
			}

			subSet := tuple.NewONRSet()
			subSet.Update(result.Resp.ResolvedOnrs)

			if i == 0 {
				objSet = subSet
			} else {
				objSet = objSet.Intersect(subSet)
			}

			if objSet.Length() == 0 {
				return lookupResult([]*v0.ObjectAndRelation{}, totalRequestCount)
			}
		case <-ctx.Done():
			return lookupResultError(NewRequestCanceledErr(), totalRequestCount)
		}
	}

	return lookupResult(objSet.AsSlice(), totalRequestCount)
}

func lookupExclude(ctx context.Context, limit uint32, requests []ReduceableLookupFunc) LookupResult {
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
	var totalRequestCount uint32
	for i := 0; i < len(requests); i++ {
		select {
		case base := <-baseChan:
			totalRequestCount += base.Resp.Metadata.DispatchCount
			if base.Err != nil {
				return lookupResultError(base.Err, totalRequestCount)
			}
			objSet.Update(base.Resp.ResolvedOnrs)

		case sub := <-othersChan:
			totalRequestCount += sub.Resp.Metadata.DispatchCount
			if sub.Err != nil {
				return lookupResultError(sub.Err, totalRequestCount)
			}

			excSet.Update(sub.Resp.ResolvedOnrs)
		case <-ctx.Done():
			return lookupResultError(NewRequestCanceledErr(), totalRequestCount)
		}
	}

	return lookupResult(limitedSlice(objSet.Subtract(excSet).AsSlice(), limit), totalRequestCount)
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

func lookupResult(resolvedONRs []*v0.ObjectAndRelation, numRequests uint32) LookupResult {
	return LookupResult{
		&v1.DispatchLookupResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: numRequests,
			},
			ResolvedOnrs: resolvedONRs,
		},
		nil,
	}
}

func lookupResultError(err error, numRequests uint32) LookupResult {
	return LookupResult{
		&v1.DispatchLookupResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: numRequests,
			},
		},
		err,
	}
}

func checkStackContains(stack []*v0.ObjectAndRelation, target *v0.ObjectAndRelation) bool {
	for _, v := range stack {
		if v.Namespace == target.Namespace && v.ObjectId == target.ObjectId && v.Relation == target.Relation {
			return true
		}
	}
	return false
}
