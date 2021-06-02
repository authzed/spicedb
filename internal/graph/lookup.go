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

func newConcurrentLookup(d Dispatcher, ds datastore.GraphDatastore, nsm namespace.Manager) lookupHandler {
	return &concurrentLookup{d: d, ds: ds, nsm: nsm}
}

type concurrentLookup struct {
	d   Dispatcher
	ds  datastore.GraphDatastore
	nsm namespace.Manager
}

// Calculate the maximum int value to allow us to effectively set no limit on certain recursive
// lookup calls.
const maxUint = ^uint(0)
const noLimit = int(maxUint >> 1)

func (cl *concurrentLookup) lookup(ctx context.Context, req LookupRequest) ReduceableLookupFunc {
	log.Trace().Object("lookup", req).Send()

	objSet := newSet()

	// If we've found the target ONR, add it to the set of resolved objects. Note that we still need
	// to continue processing, as this may also be an intermediate step in resolution.
	if req.StartRelation.Namespace == req.TargetONR.Namespace && req.StartRelation.Relation == req.TargetONR.Relation {
		objSet.add(ResolvedObject{req.TargetONR})
		req.DebugTracer.Child("(self)")
	}

	nsdef, typeSystem, _, err := cl.nsm.ReadNamespaceAndTypes(ctx, req.StartRelation.Namespace)
	if err != nil {
		return ResolveError(err)
	}

	relation, ok := findRelation(nsdef, req.StartRelation.Relation)
	if !ok {
		return ResolveError(fmt.Errorf("relation `%s` not found under namespace `%s`", req.StartRelation.Relation, req.StartRelation.Namespace))
	}

	rewrite := relation.UsersetRewrite
	var request ReduceableLookupFunc
	if rewrite != nil {
		request = cl.processRewrite(ctx, req, req.DebugTracer, nsdef, typeSystem, rewrite)
	} else {
		request = cl.lookupDirect(ctx, req, req.DebugTracer, typeSystem)
	}

	// Perform the structural lookup.
	result := LookupAny(ctx, req.Limit, []ReduceableLookupFunc{request})
	if result.Err != nil {
		return ResolveError(err)
	}

	objSet.update(result.ResolvedObjects)

	// Recursively perform lookup on any of the ONRs found that do not match the target ONR.
	// This ensures that we resolve the full transitive closure of all objects.
	recursiveTracer := req.DebugTracer.Child("Recursive")
	toCheck := objSet
	for {
		if toCheck.length() == 0 || objSet.length() >= req.Limit {
			break
		}

		loopTracer := recursiveTracer.Child("Loop")
		outgoingTracer := loopTracer.Child("Outgoing")

		var requests []ReduceableLookupFunc
		for _, obj := range toCheck.asSlice() {
			// If we've already found the target ONR, no further resolution is necessary.
			if obj.ONR.Namespace == req.TargetONR.Namespace &&
				obj.ONR.Relation == req.TargetONR.Relation &&
				obj.ONR.ObjectId == req.TargetONR.ObjectId {
				continue
			}

			requests = append(requests, cl.dispatch(LookupRequest{
				TargetONR:      obj.ONR,
				StartRelation:  req.StartRelation,
				Limit:          req.Limit - objSet.length(),
				AtRevision:     req.AtRevision,
				DepthRemaining: req.DepthRemaining - 1,
				DirectStack:    req.DirectStack,
				TTUStack:       req.TTUStack,
				DebugTracer:    outgoingTracer.Childf("%s", tuple.StringONR(obj.ONR)),
			}))
		}

		if len(requests) == 0 {
			break
		}

		result := LookupAny(ctx, req.Limit, requests)
		if result.Err != nil {
			return ResolveError(err)
		}

		toCheck = newSet()

		resultsTracer := loopTracer.Child("Results")
		for _, obj := range result.ResolvedObjects {
			resultsTracer.ChildONR(obj.ONR)

			// Only check recursively for new objects.
			if objSet.add(obj) {
				toCheck.add(obj)
			}
		}
	}

	return ResolvedObjects(limitedSlice(objSet.asSlice(), req.Limit))
}

func (cl *concurrentLookup) lookupDirect(ctx context.Context, req LookupRequest, tracer DebugTracer, typeSystem *namespace.NamespaceTypeSystem) ReduceableLookupFunc {
	// Check for the target ONR directly.
	objects := newSet()
	it, err := cl.ds.ReverseQueryTuples(req.AtRevision).
		WithObjectRelation(req.StartRelation.Namespace, req.StartRelation.Relation).
		WithSubject(req.TargetONR).
		Execute(ctx)
	if err != nil {
		return ResolveError(err)
	}
	defer it.Close()

	for tpl := it.Next(); tpl != nil; tpl = it.Next() {
		objects.add(ResolvedObject{tpl.ObjectAndRelation})
	}

	if it.Err() != nil {
		return ResolveError(it.Err())
	}

	thisTracer := tracer.Child("_this")
	thisTracer.Add("Local", objects)

	// If we've hit the limit of results already, then nothing more to do.
	if objects.length() >= req.Limit {
		return ResolvedObjects(limitedSlice(objects.asSlice(), req.Limit))
	}

	// Dispatch to any allowed direct relation types that don't match the target ONR, collect
	// the found object IDs, and then search for those.
	allowedDirect, err := typeSystem.AllowedDirectRelations(req.StartRelation.Relation)
	if err != nil {
		return ResolveError(err)
	}

	requests := []ReduceableLookupFunc{}

	directTracer := thisTracer.Child("Inferred")
	requestsTracer := directTracer.Child("Requests")

	directStack := req.DirectStack.With(&pb.ObjectAndRelation{
		Namespace: req.StartRelation.Namespace,
		Relation:  req.StartRelation.Relation,
		ObjectId:  "",
	})

	for _, allowedDirectType := range allowedDirect {
		if allowedDirectType.Relation == Ellipsis {
			continue
		}

		if allowedDirectType.Namespace == req.StartRelation.Namespace &&
			allowedDirectType.Relation == req.StartRelation.Relation {
			continue
		}

		// Prevent recursive inferred lookups, which can cause an infinite loop.
		onr := &pb.ObjectAndRelation{
			Namespace: allowedDirectType.Namespace,
			Relation:  allowedDirectType.Relation,
			ObjectId:  "",
		}
		if directStack.Has(onr) {
			requestsTracer.Childf("Skipping %s", tuple.StringONR(onr))
			continue
		}

		requests = append(requests, cl.dispatch(LookupRequest{
			TargetONR: req.TargetONR,
			StartRelation: &pb.RelationReference{
				Namespace: allowedDirectType.Namespace,
				Relation:  allowedDirectType.Relation,
			},
			Limit:          noLimit, // Since this is an inferred lookup, we can't limit.
			AtRevision:     req.AtRevision,
			DepthRemaining: req.DepthRemaining - 1,
			DirectStack:    directStack,
			TTUStack:       req.TTUStack,
			DebugTracer:    requestsTracer.Childf("Incoming %s#%s", allowedDirectType.Namespace, allowedDirectType.Relation),
		}))
	}

	if len(requests) == 0 {
		return ResolvedObjects(objects.asSlice())
	}

	// TODO(jschorr): Turn this into a parallel Lookup+Map?
	result := LookupAny(ctx, req.Limit, requests)
	if result.Err != nil {
		return ResolveError(result.Err)
	}

	// For each inferred object found, check for the target ONR.
	resultsTracer := directTracer.Child("Results To Check")
	for _, resolvedObj := range result.ResolvedObjects {
		resultTracer := resultsTracer.Child(tuple.StringONR(resolvedObj.ONR))

		it, err := cl.ds.QueryTuples(req.StartRelation.Namespace, req.AtRevision).
			WithRelation(req.StartRelation.Relation).
			WithUserset(resolvedObj.ONR).
			Execute(ctx)
		if err != nil {
			return ResolveError(err)
		}
		defer it.Close()

		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			resultTracer.Child(tuple.StringONR(tpl.ObjectAndRelation))

			objects.add(ResolvedObject{tpl.ObjectAndRelation})
			if objects.length() >= req.Limit {
				return ResolvedObjects(limitedSlice(objects.asSlice(), req.Limit))
			}
		}

		if it.Err() != nil {
			return ResolveError(it.Err())
		}
	}

	return ResolvedObjects(limitedSlice(objects.asSlice(), req.Limit))
}

func (cl *concurrentLookup) processRewrite(ctx context.Context, req LookupRequest, tracer DebugTracer, nsdef *pb.NamespaceDefinition, typeSystem *namespace.NamespaceTypeSystem, usr *pb.UsersetRewrite) ReduceableLookupFunc {
	switch rw := usr.RewriteOperation.(type) {
	case *pb.UsersetRewrite_Union:
		return cl.processSetOperation(ctx, req, tracer.Child("union"), nsdef, typeSystem, rw.Union, LookupAny)
	case *pb.UsersetRewrite_Intersection:
		return cl.processSetOperation(ctx, req, tracer.Child("intersection"), nsdef, typeSystem, rw.Intersection, LookupAll)
	case *pb.UsersetRewrite_Exclusion:
		return cl.processSetOperation(ctx, req, tracer.Child("exclusion"), nsdef, typeSystem, rw.Exclusion, LookupExclude)
	default:
		return ResolveError(fmt.Errorf("unknown userset rewrite kind under `%s#%s`", req.StartRelation.Namespace, req.StartRelation.Relation))
	}
}

func (cl *concurrentLookup) processSetOperation(ctx context.Context, req LookupRequest, parentTracer DebugTracer, nsdef *pb.NamespaceDefinition, typeSystem *namespace.NamespaceTypeSystem, so *pb.SetOperation, reducer LookupReducer) ReduceableLookupFunc {
	var requests []ReduceableLookupFunc

	tracer := parentTracer.Child("rewrite")
	for _, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *pb.SetOperation_Child_XThis:
			requests = append(requests, cl.lookupDirect(ctx, req, tracer, typeSystem))
		case *pb.SetOperation_Child_ComputedUserset:
			requests = append(requests, cl.lookupComputed(ctx, req, tracer, child.ComputedUserset))
		case *pb.SetOperation_Child_UsersetRewrite:
			requests = append(requests, cl.processRewrite(ctx, req, tracer, nsdef, typeSystem, child.UsersetRewrite))
		case *pb.SetOperation_Child_TupleToUserset:
			requests = append(requests, cl.processTupleToUserset(ctx, req, tracer, nsdef, typeSystem, child.TupleToUserset))
		default:
			return ResolveError(fmt.Errorf("unknown set operation child"))
		}
	}
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		log.Trace().Object("set operation", req).Stringer("operation", so).Send()
		resultChan <- reducer(ctx, req.Limit, requests)
	}
}

func findRelation(nsdef *pb.NamespaceDefinition, relationName string) (*pb.Relation, bool) {
	for _, relation := range nsdef.Relation {
		if relation.Name == relationName {
			return relation, true
		}
	}

	return nil, false
}

func (cl *concurrentLookup) processTupleToUserset(ctx context.Context, req LookupRequest, tracer DebugTracer, nsdef *pb.NamespaceDefinition, typeSystem *namespace.NamespaceTypeSystem, ttu *pb.TupleToUserset) ReduceableLookupFunc {
	// Ensure that we don't process TTUs recursively, as that can cause an infinite loop.
	onr := &pb.ObjectAndRelation{
		Namespace: req.StartRelation.Namespace,
		Relation:  req.StartRelation.Relation,
		ObjectId:  "",
	}
	if req.TTUStack.Has(onr) {
		tracer.Childf("recursive ttu %s#%s", req.StartRelation.Namespace, req.StartRelation.Relation)
		return ResolvedObjects([]ResolvedObject{})
	}

	tuplesetDirectRelations, err := typeSystem.AllowedDirectRelations(ttu.Tupleset.Relation)
	if err != nil {
		return ResolveError(err)
	}

	ttuTracer := tracer.Childf("ttu %s#%s <- %s", req.StartRelation.Namespace, ttu.Tupleset, ttu.ComputedUserset.Relation)

	// Collect all the accessible namespaces for the computed userset.
	requests := []ReduceableLookupFunc{}
	namespaces := map[string]bool{}

	computedUsersetTracer := ttuTracer.Child("computed_userset")
	for _, directRelation := range tuplesetDirectRelations {
		_, ok := namespaces[directRelation.Namespace]
		if ok {
			continue
		}

		_, typeSystem, _, err := cl.nsm.ReadNamespaceAndTypes(ctx, directRelation.Namespace)
		if err != nil {
			return ResolveError(err)
		}

		if !typeSystem.HasRelation(ttu.ComputedUserset.Relation) {
			continue
		}

		namespaces[directRelation.Namespace] = true
		requests = append(requests, cl.dispatch(LookupRequest{
			TargetONR: req.TargetONR,
			StartRelation: &pb.RelationReference{
				Namespace: directRelation.Namespace,
				Relation:  ttu.ComputedUserset.Relation,
			},
			Limit:          noLimit, // Since this is a step in the lookup.
			AtRevision:     req.AtRevision,
			DepthRemaining: req.DepthRemaining - 1,
			DirectStack:    req.DirectStack,
			TTUStack:       req.TTUStack.With(onr),
			DebugTracer:    computedUsersetTracer.Childf("%s#%s", directRelation.Namespace, ttu.ComputedUserset.Relation),
		}))
	}

	// TODO(jschorr): Turn this into a parallel Lookup+Map?
	result := LookupAny(ctx, req.Limit, requests)
	if result.Err != nil {
		return ResolveError(result.Err)
	}

	objects := newSet()
	computedUsersetResultsTracer := computedUsersetTracer.Childf("Results")
	for _, resolvedObj := range result.ResolvedObjects {
		tuplesetResultsTracer := computedUsersetResultsTracer.Childf("tupleset from %s", tuple.StringONR(resolvedObj.ONR))

		// Determine the relation(s) to use or the tupleset. This is determined based on the allowed direct relations and
		// we always check for both the actual relation resolved, as well as `...`
		allowedRelations := []string{}
		allowedDirect, err := typeSystem.IsAllowedDirectRelation(ttu.Tupleset.Relation, resolvedObj.ONR.Namespace, resolvedObj.ONR.Relation)
		if err != nil {
			return ResolveError(err)
		}

		if allowedDirect == namespace.DirectRelationValid {
			allowedRelations = append(allowedRelations, resolvedObj.ONR.Relation)
		}

		if resolvedObj.ONR.Relation != Ellipsis {
			allowedEllipsis, err := typeSystem.IsAllowedDirectRelation(ttu.Tupleset.Relation, resolvedObj.ONR.Namespace, Ellipsis)
			if err != nil {
				return ResolveError(err)
			}

			if allowedEllipsis == namespace.DirectRelationValid {
				allowedRelations = append(allowedRelations, Ellipsis)
			}
		}

		for _, allowedRelation := range allowedRelations {
			userset := &pb.ObjectAndRelation{
				Namespace: resolvedObj.ONR.Namespace,
				ObjectId:  resolvedObj.ONR.ObjectId,
				Relation:  allowedRelation,
			}

			tuplesetSpecificResultsTracer := tuplesetResultsTracer.Childf(tuple.StringONR(userset))
			it, err := cl.ds.QueryTuples(req.StartRelation.Namespace, req.AtRevision).
				WithRelation(ttu.Tupleset.Relation).
				WithUserset(userset).
				Execute(ctx)
			if err != nil {
				return ResolveError(err)
			}
			defer it.Close()

			for tpl := it.Next(); tpl != nil; tpl = it.Next() {
				tuplesetSpecificResultsTracer.Child(tuple.String(tpl))

				if tpl.ObjectAndRelation.Namespace != req.StartRelation.Namespace {
					return ResolveError(fmt.Errorf("got unexpected namespace"))
				}

				objects.add(ResolvedObject{
					ONR: &pb.ObjectAndRelation{
						Namespace: req.StartRelation.Namespace,
						ObjectId:  tpl.ObjectAndRelation.ObjectId,
						Relation:  req.StartRelation.Relation,
					},
				})

				if objects.length() >= req.Limit {
					return ResolvedObjects(limitedSlice(objects.asSlice(), req.Limit))
				}
			}

			if it.Err() != nil {
				return ResolveError(it.Err())
			}
		}
	}

	ttuTracer.Add("ttu Results", objects)
	return ResolvedObjects(objects.asSlice())
}

func (cl *concurrentLookup) lookupComputed(ctx context.Context, req LookupRequest, tracer DebugTracer, cu *pb.ComputedUserset) ReduceableLookupFunc {
	result := LookupOne(ctx, cl.dispatch(LookupRequest{
		TargetONR: req.TargetONR,
		StartRelation: &pb.RelationReference{
			Namespace: req.StartRelation.Namespace,
			Relation:  cu.Relation,
		},
		Limit:          req.Limit,
		AtRevision:     req.AtRevision,
		DepthRemaining: req.DepthRemaining - 1,
		DirectStack: req.DirectStack.With(&pb.ObjectAndRelation{
			Namespace: req.StartRelation.Namespace,
			Relation:  req.StartRelation.Relation,
			ObjectId:  "",
		}),
		TTUStack:    req.TTUStack,
		DebugTracer: tracer.Childf("computed_userset %s", cu.Relation),
	}))

	if result.Err != nil {
		return ResolveError(result.Err)
	}

	// Rewrite the found ONRs to be this relation.
	rewrittenResolved := make([]ResolvedObject, 0, len(result.ResolvedObjects))
	for _, resolved := range result.ResolvedObjects {
		if resolved.ONR.Namespace != req.StartRelation.Namespace {
			return ResolveError(fmt.Errorf("invalid namespace: %s vs %s", tuple.StringONR(resolved.ONR), req.StartRelation.Namespace))
		}

		rewrittenResolved = append(rewrittenResolved, ResolvedObject{
			ONR: &pb.ObjectAndRelation{
				Namespace: resolved.ONR.Namespace,
				Relation:  req.StartRelation.Relation,
				ObjectId:  resolved.ONR.ObjectId,
			},
		})
	}

	return ResolvedObjects(rewrittenResolved)
}

func (cl *concurrentLookup) dispatch(req LookupRequest) ReduceableLookupFunc {
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		log.Trace().Object("dispatch lookup", req).Send()
		result := cl.d.Lookup(ctx, req)
		resultChan <- result
	}
}

func LookupOne(ctx context.Context, request ReduceableLookupFunc) LookupResult {
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	resultChan := make(chan LookupResult)
	go request(childCtx, resultChan)

	select {
	case result := <-resultChan:
		return result
	case <-ctx.Done():
		return LookupResult{Err: ErrRequestCanceled}
	}
}

func LookupAny(ctx context.Context, limit int, requests []ReduceableLookupFunc) LookupResult {
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	resultChans := make([]chan LookupResult, 0, len(requests))
	for _, req := range requests {
		resultChan := make(chan LookupResult)
		resultChans = append(resultChans, resultChan)
		go req(childCtx, resultChan)
	}

	objects := newSet()
	for _, resultChan := range resultChans {
		select {
		case result := <-resultChan:
			if result.Err != nil {
				return LookupResult{Err: result.Err}
			}

			objects.update(result.ResolvedObjects)

			if objects.length() >= int(limit) {
				return LookupResult{
					ResolvedObjects: limitedSlice(objects.asSlice(), limit),
				}
			}
		case <-ctx.Done():
			return LookupResult{Err: ErrRequestCanceled}
		}
	}

	return LookupResult{
		ResolvedObjects: limitedSlice(objects.asSlice(), limit),
	}
}

func LookupAll(ctx context.Context, limit int, requests []ReduceableLookupFunc) LookupResult {
	if len(requests) == 0 {
		return LookupResult{[]ResolvedObject{}, nil}
	}

	resultChan := make(chan LookupResult, len(requests))
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	for _, req := range requests {
		go req(childCtx, resultChan)
	}

	objSet := newSet()

	for i := 0; i < len(requests); i++ {
		select {
		case result := <-resultChan:
			if result.Err != nil {
				return result
			}

			subSet := newSet()
			subSet.update(result.ResolvedObjects)

			if i == 0 {
				objSet = subSet
			} else {
				objSet = objSet.intersect(subSet)
			}

			if objSet.length() == 0 {
				return LookupResult{[]ResolvedObject{}, nil}
			}
		case <-ctx.Done():
			return LookupResult{Err: ErrRequestCanceled}
		}
	}

	return LookupResult{objSet.asSlice(), nil}
}

func LookupExclude(ctx context.Context, limit int, requests []ReduceableLookupFunc) LookupResult {
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	baseChan := make(chan LookupResult, 1)
	othersChan := make(chan LookupResult, len(requests)-1)

	go requests[0](childCtx, baseChan)
	for _, req := range requests[1:] {
		go req(childCtx, othersChan)
	}

	objSet := newSet()
	excSet := newSet()

	for i := 0; i < len(requests); i++ {
		select {
		case base := <-baseChan:
			if base.Err != nil {
				return base
			}
			objSet.update(base.ResolvedObjects)

		case sub := <-othersChan:
			if sub.Err != nil {
				return sub
			}

			excSet.update(sub.ResolvedObjects)
		case <-ctx.Done():
			return LookupResult{Err: ErrRequestCanceled}
		}
	}

	return LookupResult{limitedSlice(objSet.exclude(excSet).asSlice(), limit), nil}
}

func ResolvedObjects(resolved []ResolvedObject) ReduceableLookupFunc {
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		resultChan <- LookupResult{ResolvedObjects: resolved}
	}
}

func Resolved(resolved ResolvedObject) ReduceableLookupFunc {
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		resultChan <- LookupResult{ResolvedObjects: []ResolvedObject{resolved}}
	}
}

func ResolveError(err error) ReduceableLookupFunc {
	return func(ctx context.Context, resultChan chan<- LookupResult) {
		resultChan <- LookupResult{Err: err}
	}
}

func limitedSlice(slice []ResolvedObject, limit int) []ResolvedObject {
	if len(slice) > int(limit) {
		return slice[0:limit]
	}

	return slice
}

type EmittableObjectSlice []ResolvedObject

func (s EmittableObjectSlice) EmitForTrace(tracer DebugTracer) {
	for _, value := range s {
		tracer.Child(tuple.StringONR(value.ONR))
	}
}
