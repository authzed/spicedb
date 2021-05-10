package graph

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/rs/zerolog/log"
)

func newConcurrentLookup(d Dispatcher, ds datastore.GraphDatastore, rg *namespace.ReachabilityGraph) lookupHandler {
	return &concurrentLookup{d: d, ds: ds, rg: rg}
}

type concurrentLookup struct {
	d  Dispatcher
	ds datastore.GraphDatastore
	rg *namespace.ReachabilityGraph
}

func (cl *concurrentLookup) lookup(ctx context.Context, req LookupRequest) ReduceableLookupFunc {
	log.Trace().Object("lookup", req).Send()

	// Check if we've hit the target relation. If so, nothing more to do.
	if req.Start.Namespace == req.TargetRelation.Namespace && req.Start.Relation == req.TargetRelation.Relation {
		return Resolved(ResolvedObject{req.Start, req.ReductionNodeID})
	}

	var requests []ReduceableLookupFunc
	entrypoints := cl.rg.Entrypoints(req.Start.Namespace, req.Start.Relation)
	for _, entrypoint := range entrypoints {
		switch entrypoint.Kind() {
		case namespace.SubjectEntrypoint:
			// Created From: direct references to a relation type in type information
			// A subject entrypoint means we need to start at the current start userset,
			// find all relations with the userset on the right hand side that match the
			// expected target relation(s), and walk from there.
			namespaceName, relationName := entrypoint.SubjectTargetRelation()
			requests = append(requests, cl.buildDispatchedLookup(
				ctx,
				req,
				namespaceName,
				relationName,
				req.Start,
				entrypoint.ReductionNodeID(),
			))

		case namespace.AliasedRelationEntrypoint:
			// Created From: computed_userset, tupleset_to_userset (tupleset branch)
			// An aliased relation entrypoint means that the current userset should be
			// adjusted to have the relation specified as the aliasing relation, and a
			// walk should occur from there.
			namespaceName, relationName := entrypoint.AliasingRelation()
			if namespaceName != req.Start.Namespace {
				panic("Got invalid namespace for aliased entrypoint")
			}

			adjustedONR := &pb.ObjectAndRelation{
				Namespace: req.Start.Namespace,
				ObjectId:  req.Start.ObjectId,
				Relation:  relationName,
			}

			// If the entrypoint is a reduction node, then simply return the adjusted ONR for
			// reduction, since we cannot walk past it.
			reductionNodeID := entrypoint.ReductionNodeID()
			if !req.PostReductionRequest && reductionNodeID != "" {
				requests = append(requests, Resolved(ResolvedObject{adjustedONR, reductionNodeID}))
				continue
			}

			requests = append(requests, cl.dispatch(LookupRequest{
				Start:          adjustedONR,
				TargetRelation: req.TargetRelation,
				Limit:          req.Limit,
				AtRevision:     req.AtRevision,
				DepthRemaining: req.DepthRemaining - 1,
			}))

		case namespace.WalkedRelationEntrypoint:
			// Created From: tupleset_to_userset (computed_userset branch)
			// A walked relation indicates that a single relation found needs to be "walked"
			// to another relation via a simple rewrite.
			namespaceName, relationName, allowedRelationTypes := entrypoint.WalkRelationAndTypes()
			for _, allowedType := range allowedRelationTypes {
				if allowedType.Namespace != req.Start.Namespace {
					continue
				}

				// We allow walk outs from both `...` and from the same relation.
				if allowedType.Relation == "..." || allowedType.Relation == req.Start.Relation {
					requests = append(requests, cl.buildDispatchedLookup(
						ctx,
						req,
						namespaceName,
						relationName,
						&pb.ObjectAndRelation{
							Namespace: req.Start.Namespace,
							ObjectId:  req.Start.ObjectId,
							Relation:  allowedType.Relation,
						},
						entrypoint.ReductionNodeID(),
					))
				}
			}
		}
	}

	return func(ctx context.Context, resultChan chan<- LookupResult) {
		if req.IsRootRequest {
			resultChan <- cl.lookupAndReduceAll(ctx, req, requests)
		} else {
			resultChan <- LookupAll(ctx, requests)
		}
	}
}

func (cl *concurrentLookup) buildDispatchedLookup(
	ctx context.Context,
	req LookupRequest,
	targetNamespaceName string,
	targetRelationName string,
	startONR *pb.ObjectAndRelation,
	reductionNodeID namespace.NodeID) ReduceableLookupFunc {
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		// Lookup all tuples with the request's start tuple on the right hand side, and
		// the entrypoint relation on the left hand side.
		it, err := cl.ds.ReverseQueryTuples(targetNamespaceName, targetRelationName, startONR, req.AtRevision).
			Execute(ctx)
		if err != nil {
			resultChan <- LookupResult{Err: err}
			return
		}
		defer it.Close()

		var directResults []ResolvedObject
		var nextRequests []ReduceableLookupFunc

		// For each tuple found, check if we've found the expected relation. If so, add
		// to the direct results. Otherwise, walk outward from the left hand side.
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			onr := tpl.ObjectAndRelation
			if reductionNodeID != "" || (onr.Namespace == req.TargetRelation.Namespace && onr.Relation == req.TargetRelation.Relation) {
				directResults = append(directResults, ResolvedObject{onr, reductionNodeID})
				continue
			}

			nextRequests = append(nextRequests, cl.dispatch(LookupRequest{
				Start:          onr,
				TargetRelation: req.TargetRelation,
				Limit:          req.Limit - uint64(len(directResults)),
				AtRevision:     req.AtRevision,
				DepthRemaining: req.DepthRemaining - 1,
			}))
		}
		if it.Err() != nil {
			resultChan <- LookupResult{Err: err}
			return
		}

		if len(nextRequests) > 0 {
			resultChan <- LookupAll(ctx, nextRequests, directResults...)
		} else {
			resultChan <- LookupResult{ResolvedObjects: directResults}
		}
	}
}

func (cl *concurrentLookup) dispatch(req LookupRequest) ReduceableLookupFunc {
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		log.Trace().Object("dispatch lookup", req).Send()
		result := cl.d.Lookup(ctx, req)
		resultChan <- result
	}
}

// lookupAndReduceAll returns a result with all of the children reduced.
func (cl *concurrentLookup) lookupAndReduceAll(ctx context.Context, req LookupRequest, requests []ReduceableLookupFunc, directResults ...ResolvedObject) LookupResult {
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	resultChans := make([]chan LookupResult, 0, len(requests))
	for _, req := range requests {
		resultChan := make(chan LookupResult)
		resultChans = append(resultChans, resultChan)
		go req(childCtx, resultChan)
	}

	reducer := cl.rg.NewReducer()
	objects := newSetFromSlice([]ResolvedObject{})

	for _, resultChan := range resultChans {
		select {
		case result := <-resultChan:
			if result.Err != nil {
				return LookupResult{Err: result.Err}
			}

			// For each object resolved, check if it is under a reduction node.
			// If so, reduction needs to be run and the lookup continued from the
			// reduced relation. If not, then this object is a final result.
			for _, obj := range result.ResolvedObjects {
				if obj.ReductionNodeID != "" {
					reducer.Add(obj.ReductionNodeID, obj.ONR)
				} else {
					objects.add(obj)
				}
			}

		case <-ctx.Done():
			return LookupResult{Err: ErrRequestCanceled}
		}
	}

	// If there is nothing to reduce, then return the final results.
	if reducer.Empty() {
		return LookupResult{
			ResolvedObjects: objects.AsSlice(),
		}
	}

	// Otherwise, perform reduction on the reducable results, then kick off Lookup
	// from that point forward.
	reduced := reducer.Run()
	directResults = append(directResults, objects.AsSlice()...)

	var newRequests []ReduceableLookupFunc
	for _, reducedONR := range reduced {
		newRequests = append(newRequests, cl.dispatch(LookupRequest{
			Start:                reducedONR,
			PostReductionRequest: true,
			TargetRelation:       req.TargetRelation,
			Limit:                req.Limit - uint64(len(directResults)),
			AtRevision:           req.AtRevision,
			DepthRemaining:       req.DepthRemaining - 1,
		}))
	}

	return cl.lookupAndReduceAll(ctx, req, newRequests, directResults...)
}

// LookupAll returns a result with all of the children.
func LookupAll(ctx context.Context, requests []ReduceableLookupFunc, directResults ...ResolvedObject) LookupResult {
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	resultChans := make([]chan LookupResult, 0, len(requests))
	for _, req := range requests {
		resultChan := make(chan LookupResult)
		resultChans = append(resultChans, resultChan)
		go req(childCtx, resultChan)
	}

	objects := newSetFromSlice(directResults)
	for _, resultChan := range resultChans {
		select {
		case result := <-resultChan:
			if result.Err != nil {
				return LookupResult{Err: result.Err}
			}
			objects.update(result.ResolvedObjects)
		case <-ctx.Done():
			return LookupResult{Err: ErrRequestCanceled}
		}
	}

	return LookupResult{
		ResolvedObjects: objects.AsSlice(),
	}
}

// LookupOne waits for exactly one response
func LookupOne(ctx context.Context, request ReduceableLookupFunc) LookupResult {
	resultChan := make(chan LookupResult, 1)
	go request(ctx, resultChan)

	select {
	case result := <-resultChan:
		if result.Err != nil {
			return LookupResult{Err: result.Err}
		}
		return result
	case <-ctx.Done():
		return LookupResult{Err: ErrRequestCanceled}
	}
}

func Resolved(resolved ResolvedObject) ReduceableLookupFunc {
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		resultChan <- LookupResult{ResolvedObjects: []ResolvedObject{resolved}}
	}
}

type ResolvedObjectSet struct {
	entries map[string]ResolvedObject
}

func newSetFromSlice(ros []ResolvedObject) *ResolvedObjectSet {
	set := &ResolvedObjectSet{
		entries: map[string]ResolvedObject{},
	}
	set.update(ros)
	return set
}

func (s *ResolvedObjectSet) add(value ResolvedObject) {
	s.entries[fmt.Sprintf("%s-%s", tuple.StringONR(value.ONR), value.ReductionNodeID)] = value
}

func (s *ResolvedObjectSet) update(ros []ResolvedObject) {
	for _, value := range ros {
		s.add(value)
	}
}

func (s *ResolvedObjectSet) AsSlice() []ResolvedObject {
	slice := []ResolvedObject{}
	for _, value := range s.entries {
		slice = append(slice, value)
	}
	return slice
}
