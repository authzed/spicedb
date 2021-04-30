package graph

import (
	"context"
	"errors"

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
	// Check if we've hit the target relation. If so, nothing more to do.
	if req.Start.Namespace == req.TargetRelation.Namespace && req.Start.Relation == req.TargetRelation.Relation {
		return FoundResult(req.Start)
	}

	log.Trace().Object("lookup", req).Send()

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
					))
				}
			}
		}
	}

	return func(ctx context.Context, resultChan chan<- LookupResult) {
		resultChan <- LookupAll(ctx, requests)
	}
}

func (cl *concurrentLookup) buildDispatchedLookup(ctx context.Context, req LookupRequest, targetNamespaceName string, targetRelationName string, startONR *pb.ObjectAndRelation) ReduceableLookupFunc {
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

		var directResults []*pb.ObjectAndRelation
		var nextRequests []ReduceableLookupFunc

		// For each tuple found, check if we've found the expected relation. If so, add
		// to the direct results. Otherwise, walk outward from the left hand side.
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			onr := tpl.ObjectAndRelation
			if onr.Namespace == req.TargetRelation.Namespace && onr.Relation == req.TargetRelation.Relation {
				directResults = append(directResults, onr)
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
			resultChan <- LookupResult{FoundObjects: directResults}
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

// LookupAll returns a result with all of the children.
func LookupAll(ctx context.Context, requests []ReduceableLookupFunc, directResults ...*pb.ObjectAndRelation) LookupResult {
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
			objects.update(result.FoundObjects)
		case <-ctx.Done():
			return LookupResult{Err: ErrRequestCanceled}
		}
	}

	return LookupResult{
		FoundObjects: objects.AsSlice(),
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

func FoundResult(onr *pb.ObjectAndRelation) ReduceableLookupFunc {
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		resultChan <- LookupResult{FoundObjects: []*pb.ObjectAndRelation{onr}}
	}
}

var errAlwaysFailLookup = errors.New("always fail")

func alwaysFailLookup(ctx context.Context, resultChan chan<- LookupResult) {
	resultChan <- LookupResult{Err: errAlwaysFailExpand}
}

type ONRSet struct {
	entries map[string]*pb.ObjectAndRelation
}

func newSetFromSlice(onrs []*pb.ObjectAndRelation) *ONRSet {
	set := &ONRSet{
		entries: map[string]*pb.ObjectAndRelation{},
	}
	set.update(onrs)
	return set
}

func (s *ONRSet) update(onrs []*pb.ObjectAndRelation) {
	for _, value := range onrs {
		s.entries[tuple.StringONR(value)] = value
	}
}

func (s *ONRSet) AsSlice() []*pb.ObjectAndRelation {
	slice := []*pb.ObjectAndRelation{}
	for _, value := range s.entries {
		slice = append(slice, value)
	}
	return slice
}
