package graph

import (
	"context"
	"encoding/base64"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sort"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/caveats"
	cter "github.com/authzed/spicedb/internal/cursorediterator"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/internal/graph/hints"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

const lr3DispatchVersion = 3

// NewCursoredLookupResources3 creates a new CursoredLookupResources3 instance with the given parameters.
func NewCursoredLookupResources3(dl dispatch.LookupResources3, dc dispatch.Check, caveatTypeSet *caveattypes.TypeSet, concurrencyLimit uint16, dispatchChunkSize uint16) *CursoredLookupResources3 {
	return &CursoredLookupResources3{dl, dc, caveatTypeSet, concurrencyLimit, dispatchChunkSize}
}

// CursoredLookupResources3 is a dispatch implementation for the LookupResources3 operation.
type CursoredLookupResources3 struct {
	// dl is a dispatcher for redispatching LookupResources3 requests.
	dl dispatch.LookupResources3

	// dc is a dispatcher for performing checks.
	dc dispatch.Check

	// caveatTypeSet is the set of caveat types that can be used in this dispatch when
	// computing caveats.
	caveatTypeSet *caveattypes.TypeSet

	// concurrencyLimit is the maximum number of concurrent operations that can be performed
	// when dispatching.
	concurrencyLimit uint16

	// dispatchChunkSize is the maximum number of relationships that can be dispatched
	// in a single request.
	dispatchChunkSize uint16
}

// ValidatedLookupResources3Request is a wrapper around the DispatchLookupResources3Request
// that includes a revision for the datastore reader, and indicates that the request has been validated.
type ValidatedLookupResources3Request struct {
	*v1.DispatchLookupResources3Request
	Revision datastore.Revision
}

// LookupResources3 implements the LookupResources operation, which finds all resources of a given
// type that a subject can access via a specific permission.
//
// # Algorithm Overview
//
// LookupResources3 uses a multi-phase iterator-based approach to efficiently traverse the permission
// graph and find accessible resources. The algorithm operates in two main portions:
//
// ## Portion #1: Direct Subject Match
//
// If the subject's type and relation exactly match the resource's type and relation, the subject
// IDs are immediately returned as accessible resources (a permission always grants access to itself).
//
//	Example: user:alice#... accessing user:alice#... → immediate match
//
// ## Portion #2: Entrypoint-Based Traversal
//
// For cases where traversal is needed, the algorithm identifies "entrypoints" - relations or
// permissions that provide paths from the subject type to the target resource permission.
//
//	Example: user:alice → document:doc1#read
//	Entrypoints might be: document#viewer, document#editor (both lead to #read)
//
// # Iterator Architecture
//
// The implementation uses a sophisticated iterator pipeline built from three core iterator patterns:
//
//	┌──────────────────────────────────────────────────────────────────────────────┐
//	│                         LookupResources3 Iterator Flow                       │
//	├──────────────────────────────────────────────────────────────────────────────┤
//	│                                                                              │
//	│  ┌─────────────────┐    ┌──────────────────────────────────────────────────┐ │
//	│  │ Direct Subject  │    │              Entrypoints Iterator                │ │
//	│  │ Match Iterator  │    │                                                  │ │
//	│  │                 │    │  ┌─────────────────────────────────────────────┐ │ │
//	│  │ CursoredWith    │ ┌──┤  │         Parallel Entrypoint Executors       │ │ │
//	│  │ IntegerHeader   │ │  │  │                                             │ │ │
//	│  │                 │ │  │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐   │ │ │
//	│  └─────────────────┘ │  │  │  │Entrypoint│  │Entrypoint│  │Entrypoint│   │ │ │
//	│                      │  │  │  │    #1    │  │    #2    │  │    #N    │   │ │ │
//	│                      │  │  │  └──────────┘  └──────────┘  └──────────┘   │ │ │
//	│                      │  │  │        │            │            │          │ │ │
//	│                      │  │  └─────────────────────────────────────────────┘ │ │
//	│                      │  │                     │                            │ │
//	│                      │  │  ┌─────────────────────────────────────────────┐ │ │
//	│                      │  │  │     CursoredParallelIterators               │ │ │
//	│                      │  │  │   (executes entrypoints concurrently)       │ │ │
//	│                      │  │  └─────────────────────────────────────────────┘ │ │
//	│                      │  └──────────────────────────────────────────────────┘ │
//	│                      │                                                       │
//	│                      └──────────────────┬────────────────────────────────────┘
//	│                                         │                                    │
//	│                              ┌─────────────────────┐                         │
//	│                              │   Combined Results  │                         │
//	│                              │    (yields to       │                         │
//	│                              │   response stream)  │                         │
//	│                              └─────────────────────┘                         │
//	└──────────────────────────────────────────────────────────────────────────────┘
//
// For each entrypoint, implements a producer-mapper pattern:
// - Producer: Fetches relationship chunks from the datastore
// - Mapper: Processes chunks by dispatching or checking, then recursively calling LookupResources3
//
//	┌─────────────────────────────────────────────────────────────────────────────┐
//	│                    Per-Entrypoint Processing Flow                           │
//	├─────────────────────────────────────────────────────────────────────────────┤
//	│                                                                             │
//	│  ┌─────────────────┐     ┌──────────────────┐     ┌──────────────────────┐  │
//	│  │   Entrypoint    │────▶│  Relationships   │────▶│     Processing       │  │
//	│  │   Analyzer      │     │     Iterator     │     │      (Mapper)        │  │
//	│  │                 │     │   (Producer)     │     │                      │  │
//	│  │ • Relation EP   │     │                  │     │ ┌──────────────────┐ │  │
//	│  │ • Computed EP   │     │ Fetches chunks   │     │ │  Filter via      │ │  │
//	│  │ • TTU EP        │     │ of relationships │     │ │  Check (if not   │ │  │
//	│  │                 │     │ from datastore   │     │ │  direct result)  │ │  │
//	│  └─────────────────┘     │                  │     │ └──────────────────┘ │  │
//	│                          │ Honors cursor    │     │          │           │  │
//	│                          │ for resumption   │     │          ▼           │  │
//	│                          │                  │     │ ┌──────────────────┐ │  │
//	│                          └──────────────────┘     │ │ Dispatch to      │ │  │
//	│                                                   │ │ LookupResources3 │ │  │
//	│                                                   │ │ (recursive)      │ │  │
//	│                                                   │ └──────────────────┘ │  │
//	│                                                   └──────────────────────┘  │
//	└─────────────────────────────────────────────────────────────────────────────┘
//
// # Entrypoint Types
//
// The algorithm handles three types of entrypoints based on schema reachability analysis:
//
// 1. **Relation Entrypoints**: Direct relation traversal
//   - Example: document#viewer → find all documents where subjects are viewers
//   - Query: SELECT resource_id FROM relations WHERE subject IN (...) AND relation = 'viewer'
//
// 2. **Computed User Set Entrypoints**: Permission rewrites
//   - Example: document#view = viewer → rewrite subjects as document#view
//   - Structural transformation without datastore queries
//
// 3. **Tupleset-to-Userset Entrypoints** (Arrows): Indirect relation traversal
//   - Example: document#read = viewer + parent->read
//   - Find documents where subjects are in 'parent' relation, then check 'read' on those parents
//
// # Chunk Processing and Dispatch
//
// Relationships are processed in configurable chunks (dispatchChunkSize) to balance memory usage
// and dispatch efficiency. Each chunk undergoes:
//
// 1. **Caveat Evaluation**: Relationships with caveats are evaluated against the request context
//   - Caveats that fail are filtered out early (tree shearing)
//   - Partial results collect missing context parameters
//
// 2. **Check Filtering**: For non-direct entrypoints, found resources are validated via Check dispatch
//   - Filters out resources that don't actually grant the permission (intersection/exclusion handling)
//   - Only validated resources proceed to recursive dispatch
//
// 3. **Recursive Dispatch**: Validated resource IDs become subject IDs for the next LookupResources3 call
//   - Creates new dispatch requests with found resources as subjects
//   - Continues until reaching the target resource type and permission
//
// # Cursor Management
//
// The cursor system enables efficient pagination and resumption:
//
//	Cursor Structure: [header_index, entrypoint_index, datastore_cursor, remaining...]
//	                   │             │                 │                 │
//	                   │             │                 │                 └─ Nested dispatch cursor
//	                   │             │                 └─ Position in relationship stream
//	                   │             └─ Which entrypoint is being processed
//	                   └─ Position in subject IDs list (-1 = header complete)
//
// This allows the operation to resume at any point in the complex iteration hierarchy without
// losing progress or re-processing completed work.
func (crr *CursoredLookupResources3) LookupResources3(req ValidatedLookupResources3Request, stream dispatch.LookupResources3Stream) error {
	ctx, span := tracer.Start(stream.Context(), "LookupResources3")
	defer span.End()

	if req.TerminalSubject == nil {
		return spiceerrors.MustBugf("no terminal subject given to lookup resources dispatch")
	}

	if slices.Contains(req.SubjectIds, tuple.PublicWildcard) {
		return NewWildcardNotAllowedErr("cannot perform lookup resources on wildcard", "subject_id")
	}

	if len(req.SubjectIds) == 0 {
		return spiceerrors.MustBugf("no subjects ids given to lookup resources dispatch")
	}

	// Sort for stability.
	if len(req.SubjectIds) > 1 {
		sort.Strings(req.SubjectIds)
	}

	// Build context for the lookup resources operation. The lr3ctx holds references to shared
	// interfaces used by various suboperations of the lookup resources operation.
	ds := datastoremw.MustFromContext(stream.Context())
	reader := ds.SnapshotReader(req.Revision)
	ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(reader))
	caveatRunner := caveats.NewCaveatRunner(crr.caveatTypeSet)

	lctx := lr3ctx{
		req:          req,
		reader:       reader,
		ts:           ts,
		caveatRunner: caveatRunner,
	}

	// Retrieve results from the unlimited iterator and publish them, respecting the limit,
	// if any.
	resultCount := uint32(0)
	for result, err := range crr.unlimitedLookupResourcesIter(ctx, lctx) {
		if err != nil {
			span.RecordError(err)
			return err
		}

		afterResponseCursor := &v1.Cursor{
			DispatchVersion: lr3DispatchVersion,
			Sections:        result.Cursor,
		}

		if result.Item.possibleResource == nil || result.Item.metadata == nil {
			return spiceerrors.MustBugf("invalid result from lookup resources: missing possible resource or metadata")
		}

		if err := stream.Publish(&v1.DispatchLookupResources3Response{
			Resource:            result.Item.possibleResource,
			Metadata:            addCallToResponseMetadata(result.Item.metadata),
			AfterResponseCursor: afterResponseCursor,
		}); err != nil {
			span.RecordError(err)
			return err
		}

		resultCount++
		if req.OptionalLimit > 0 && resultCount >= req.OptionalLimit {
			return nil
		}
	}
	return nil
}

// pram is a "possible resource and metadata" struct used for lookup resources results.
type pram struct {
	possibleResource *v1.PossibleResource
	metadata         *v1.ResponseMeta
}

// result is the result type for the lookup resources iterator, which contains a possible resource and metadata,
// as well as the cursor for the next iteration.
type result = cter.ItemAndCursor[pram]

// lr3ctx is a holder for various capabilities used by the LookupResources3 operation.
type lr3ctx struct {
	// req is the validated request for the LookupResources3 operation.
	req ValidatedLookupResources3Request

	// reader is the datastore reader used to perform the lookup resources operation.
	reader datastore.Reader

	// ts is the type system used to resolve types and relations for the lookup resources operation.
	ts *schema.TypeSystem

	// caveatRunner is the caveat runner used to run caveats for the lookup resources operation.
	caveatRunner *caveats.CaveatRunner
}

// unlimitedLookupResourcesIter performs the actual lookup resources operation, returning an iterator
// sequence of results. This iterator will yield results until all results are reached
// or an error occurs. It is the responsibility of the caller to handle limits.
func (crr *CursoredLookupResources3) unlimitedLookupResourcesIter(
	ctx context.Context,
	lctx lr3ctx,
) iter.Seq2[result, error] {
	ctx, span := tracer.Start(ctx, "unlimitedLookupResourcesIter")
	defer span.End()

	var cursor cter.Cursor
	if lctx.req.OptionalCursor != nil {
		if lctx.req.OptionalCursor.DispatchVersion != lr3DispatchVersion {
			return cter.YieldsError[result](fmt.Errorf("invalid dispatch version %d for lookup resources, expected %d", lctx.req.OptionalCursor.DispatchVersion, lr3DispatchVersion))
		}
		cursor = lctx.req.OptionalCursor.Sections
	}

	// Portion #1: If the requested resource type+relation matches the subject type+relation, then we've immediately
	// found matching resources, as a permission always matches itself.
	return cter.CursoredWithIntegerHeader(ctx, cursor,
		func(ctx context.Context, startIndex int) iter.Seq2[pram, error] {
			// If the subject relation does not match the resource relation, we cannot yield any results for this
			// portion.
			if lctx.req.SubjectRelation.Namespace != lctx.req.ResourceRelation.Namespace ||
				lctx.req.SubjectRelation.Relation != lctx.req.ResourceRelation.Relation {
				return cter.UncursoredEmpty[pram]()
			}

			_, span := tracer.Start(ctx, "unlimitedLookupResourcesIter::direct-subjects", trace.WithAttributes(
				attribute.String("subject-resource-type", lctx.req.SubjectRelation.Namespace),
				attribute.String("subject-resource-relation", lctx.req.SubjectRelation.Relation),
			))
			defer span.End()

			// Return an iterator that yields results for all subject IDs starting from startIndex.
			return func(yield func(pram, error) bool) {
				for _, subjectID := range lctx.req.SubjectIds[startIndex:] {
					if !yield(pram{
						possibleResource: &v1.PossibleResource{
							ResourceId:    subjectID,
							ForSubjectIds: []string{subjectID},
						},
						metadata: emptyMetadata,
					}, nil) {
						return
					}
				}
			}
		},

		// Portion #2: Next, we need to iterate over the entrypoints for the given subject IDs and resource relation,
		// yielding results for each entrypoint. An entrypoint is any relation or permission that is reachable from the
		// current subject type. For example, if the subject type is "user" and the target resource+permission is "document:read",
		// the entrypoints could be "document#viewer" and "document#editor", as they are the relations that must walked
		// *from* the user to reach the `read` permission on the `document`.
		crr.entrypointsIter(lctx),
	)
}

// entrypointsIter is the second portion of the lookup resources iterator, which iterates over
// the entrypoints for the given subject IDs and yields results for each entrypoint.
func (crr *CursoredLookupResources3) entrypointsIter(lctx lr3ctx) cter.Next[pram] {
	return func(ctx context.Context, currentCursor cter.Cursor) iter.Seq2[result, error] {
		ctx, span := tracer.Start(ctx, "entrypointsIter")
		defer span.End()

		// Determine the concurrency to use for this iteration. If any limit is requested,
		// we set to a fixed concurrency of 2 to avoid overwhelming the server, as there is
		// reasonable potential for only the first entrypoint to yield results before the limit
		// is reached.
		// In the case where OptionalLimit is 0, we can use the configured concurrency limit,
		// as we'll have to fetch all results anyway.
		var concurrency uint16 = 2
		if lctx.req.OptionalLimit == 0 {
			concurrency = crr.concurrencyLimit
		}

		// Load the entrypoints that are reachable from the subject relation to the resource relation/permission.
		entrypoints, err := crr.loadEntrypoints(ctx, lctx)
		if err != nil {
			return cter.YieldsError[result](err)
		}

		// For each entrypoint, create a cursor iterator that will yield results for that entrypoint via
		// the entrypointIter method.
		cursorOverEntrypoints := make([]cter.Next[pram], 0, len(entrypoints))
		for _, ep := range entrypoints {
			cursorOverEntrypoints = append(cursorOverEntrypoints, crr.entrypointIter(lctx, ep))
		}

		// Execute the iterators in parallel, yielding results for each entrypoint in the proper order.
		return cter.CursoredParallelIterators(ctx, currentCursor, concurrency, cursorOverEntrypoints...)
	}
}

// loadEntrypoints loads the entrypoints for the given subject relation and resource relation found in the request.
func (crr *CursoredLookupResources3) loadEntrypoints(ctx context.Context, lctx lr3ctx) ([]schema.ReachabilityEntrypoint, error) {
	// The entrypoints for a particular subject relation to a resource relation are defined by the reachability
	// graph, which is built from the type system definition.
	vdef, err := lctx.ts.GetValidatedDefinition(ctx, lctx.req.ResourceRelation.Namespace)
	if err != nil {
		return nil, err
	}

	rg := vdef.Reachability()
	return rg.FirstEntrypointsForSubjectToResource(ctx, &core.RelationReference{
		Namespace: lctx.req.SubjectRelation.Namespace,
		Relation:  lctx.req.SubjectRelation.Relation,
	}, lctx.req.ResourceRelation)
}

// entrypointIter is the iterator for a single entrypoint, which will yield results for the given
// entrypoint and the subject IDs in the request. It will return an iterator sequence of results
// for each entrypoint, dispatching further when necessary.
func (crr *CursoredLookupResources3) entrypointIter(lctx lr3ctx, entrypoint schema.ReachabilityEntrypoint) cter.Next[pram] {
	return func(ctx context.Context, currentCursor cter.Cursor) iter.Seq2[result, error] {
		ctx, span := tracer.Start(ctx, "entrypointIter", trace.WithAttributes(
			attribute.String("entrypoint", entrypoint.DebugStringOrEmpty()),
		))
		defer span.End()

		switch entrypoint.EntrypointKind() {
		// If the entrypoint is a relation entrypoint, we need to iterate over the relation's relationships
		// for the given subject IDs and yield results for dispatching for each. For example, given a relation
		// of `relation viewer: user`, we would lookup all relationships for the current user(s) and find
		// all the document(s) of which the user is a viewer.
		case core.ReachabilityEntrypoint_RELATION_ENTRYPOINT:
			return crr.relationEntrypointIter(ctx, lctx, entrypoint, currentCursor)

		// If the entrypoint is a computed user set entrypoint, we need to rewrite the subject relation
		// to the containing relation or permission of the entrypoint. For example, given a permission
		// of `permission view = viewer`, the current subject type of `document:{somedoc}#viewer` would
		// be rewritten to `document:{somedoc}#view`.
		case core.ReachabilityEntrypoint_COMPUTED_USERSET_ENTRYPOINT:
			containingRelation := entrypoint.ContainingRelationOrPermission()
			rewrittenSubjectRelation := &core.RelationReference{
				Namespace: containingRelation.Namespace,
				Relation:  containingRelation.Relation,
			}

			// Since this is a structural rewriting, the *entire set* of subject IDs is provided as the basis for
			// the rewritten subject relation.
			rm := subjectIDsToRelationshipsChunk(lctx.req.SubjectRelation, lctx.req.SubjectIds, rewrittenSubjectRelation)

			// Dispatch the rewritten subjects, to further find resources up the tree.
			return crr.checkedDispatchIter(ctx, lctx, currentCursor, rm, rewrittenSubjectRelation, entrypoint)

		// If the entrypoint is a TTU entrypoint (arrow), we need to iterate over the tupleset's relationships
		// for the given subject IDs and yield results for dispatching for each, rewriting based on the containing
		// relation. For example, given an arrow of `parent->view`, we would lookup all relationships for the `parent`
		// relation and then *dispatch* to the `view` permission for each.
		case core.ReachabilityEntrypoint_TUPLESET_TO_USERSET_ENTRYPOINT:
			return crr.ttuEntrypointIter(ctx, lctx, entrypoint, currentCursor)

		default:
			return cter.YieldsError[result](spiceerrors.MustBugf("Unknown kind of entrypoint: %v", entrypoint.EntrypointKind()))
		}
	}
}

// relationEntrypointIter returns an iterator for a relation entrypoint, which will yield results
// found by traversing the relation for the given entrypoint and the subject IDs in the request.
func (crr *CursoredLookupResources3) relationEntrypointIter(
	ctx context.Context,
	lctx lr3ctx,
	entrypoint schema.ReachabilityEntrypoint,
	currentCursor cter.Cursor,
) iter.Seq2[result, error] {
	ctx, span := tracer.Start(ctx, "relationEntrypointIter", trace.WithAttributes(
		attribute.String("entrypoint", entrypoint.DebugStringOrEmpty()),
	))
	defer span.End()

	// Build a subject filter for the subjects for which to lookup relationships.
	relationReference, err := entrypoint.DirectRelation()
	if err != nil {
		return cter.YieldsError[result](err)
	}

	relDefinition, err := lctx.ts.GetValidatedDefinition(ctx, relationReference.Namespace)
	if err != nil {
		return cter.YieldsError[result](err)
	}

	// Build the list of subjects to lookup based on the type information available.
	isDirectAllowed, err := relDefinition.IsAllowedDirectRelation(
		relationReference.Relation,
		lctx.req.SubjectRelation.Namespace,
		lctx.req.SubjectRelation.Relation,
	)
	if err != nil {
		return cter.YieldsError[result](err)
	}

	subjectIds := make([]string, 0, len(lctx.req.SubjectIds)+1)

	// If the subject relation is allowed directly on the relation, we can use the subject IDs
	// directly, as they are valid subjects for the relation.
	if isDirectAllowed == schema.DirectRelationValid {
		subjectIds = append(subjectIds, lctx.req.SubjectIds...)
	}

	// If the subject relation is a terminal subject, check for a wildcard. For example,
	// if the subject type is `user`, then `user:*` would also match any subjects of that
	// type.
	if lctx.req.SubjectRelation.Relation == tuple.Ellipsis {
		isWildcardAllowed, err := relDefinition.IsAllowedPublicNamespace(relationReference.Relation, lctx.req.SubjectRelation.Namespace)
		if err != nil {
			return cter.YieldsError[result](err)
		}

		if isWildcardAllowed == schema.PublicSubjectAllowed {
			subjectIds = append(subjectIds, "*")
		}
	}

	relationFilter := datastore.SubjectRelationFilter{
		NonEllipsisRelation: lctx.req.SubjectRelation.Relation,
	}

	if lctx.req.SubjectRelation.Relation == tuple.Ellipsis {
		relationFilter = datastore.SubjectRelationFilter{
			IncludeEllipsisRelation: true,
		}
	}

	subjectsFilter := datastore.SubjectsFilter{
		SubjectType:        lctx.req.SubjectRelation.Namespace,
		OptionalSubjectIds: subjectIds,
		RelationFilter:     relationFilter,
	}

	// Lookup relationships for the given subjects and relation reference, dispatching over the results.
	return crr.relationshipsIter(ctx, lctx, currentCursor, relationshipsIterConfig{
		subjectsFilter:     subjectsFilter,
		sourceResourceType: relationReference,
		foundResourceType:  relationReference,
		entrypoint:         entrypoint,
	})
}

// ttuEntrypointIter returns an iterator for a TTU entrypoint (arrow), which will yield results
// found by traversing the tupleset for the given entrypoint and the subject IDs in the request.
func (crr *CursoredLookupResources3) ttuEntrypointIter(
	ctx context.Context,
	lctx lr3ctx,
	entrypoint schema.ReachabilityEntrypoint,
	currentCursor cter.Cursor,
) iter.Seq2[result, error] {
	ctx, span := tracer.Start(ctx, "ttuEntrypointIter", trace.WithAttributes(
		attribute.String("entrypoint", entrypoint.DebugStringOrEmpty()),
	))
	defer span.End()

	containingRelation := entrypoint.ContainingRelationOrPermission()

	ttuDef, err := lctx.ts.GetValidatedDefinition(ctx, containingRelation.Namespace)
	if err != nil {
		return cter.YieldsError[result](err)
	}

	tuplesetRelation, err := entrypoint.TuplesetRelation()
	if err != nil {
		return cter.YieldsError[result](err)
	}

	// Determine whether this TTU should be followed, which will be the case if the subject relation's namespace
	// is allowed in any form on the relation; since arrows ignore the subject's relation (if any), we check
	// for the subject namespace as a whole.
	allowedRelations, err := ttuDef.GetAllowedDirectNamespaceSubjectRelations(tuplesetRelation, lctx.req.SubjectRelation.Namespace)
	if err != nil {
		return cter.YieldsError[result](err)
	}

	if allowedRelations == nil {
		return cter.UncursoredEmpty[result]()
	}

	// Search for the resolved subjects in the tupleset of the TTU.
	subjectsFilter := datastore.SubjectsFilter{
		SubjectType:        lctx.req.SubjectRelation.Namespace,
		OptionalSubjectIds: lctx.req.SubjectIds,
	}

	// Optimization: if there is a single allowed relation, pass it as a subject relation filter to make things faster
	// on querying.
	if allowedRelations.Len() == 1 {
		allowedRelationName := allowedRelations.AsSlice()[0]
		subjectsFilter.RelationFilter = datastore.SubjectRelationFilter{}.WithRelation(allowedRelationName)
	}

	tuplesetRelationReference := &core.RelationReference{
		Namespace: containingRelation.Namespace,
		Relation:  tuplesetRelation,
	}

	// Lookup relationships for the given subjects and relation reference, dispatching over the results.
	return crr.relationshipsIter(ctx, lctx, currentCursor, relationshipsIterConfig{
		subjectsFilter:     subjectsFilter,
		sourceResourceType: tuplesetRelationReference,
		foundResourceType:  containingRelation,
		entrypoint:         entrypoint,
	})
}

// relationshipsIterConfig is a configuration struct for the relationships iterator.
type relationshipsIterConfig struct {
	// subjectsFilter is the filter for the subjects to lookup relationships for.
	subjectsFilter datastore.SubjectsFilter

	// sourceResourceType is the relation reference for the source resource type to lookup.
	sourceResourceType *core.RelationReference

	// foundResourceType is the relation reference for the resource type that will be returned in results.
	// For relation entrypoints, this is the same as sourceResourceType. For arrows, this will be the type
	// of the resources found after the arrow traversal.
	foundResourceType *core.RelationReference

	// entrypoint is the reachability entrypoint for this lookup.
	entrypoint schema.ReachabilityEntrypoint
}

// relationshipsIter is an iterator that fetches relationships for the given subjects and processes them
// by further dispatching the found resources as the next step's subjects.
func (crr *CursoredLookupResources3) relationshipsIter(
	ctx context.Context,
	lctx lr3ctx,
	currentCursor cter.Cursor,
	config relationshipsIterConfig,
) iter.Seq2[result, error] {
	// The relationships iterator operates as a producer-mapper iterator, where the producer
	// fetches relationships for the given subjects and the mapper processes those relationships
	// to yield possible resources and metadata for the lookup resources response.
	//
	// The producer will yield chunks of relationships, maximum of the dispatchChunkSize, to be
	// dispatched by the mapper in parallel.
	return cter.CursoredProducerMapperIterator(ctx, currentCursor,
		datastoreCursorFromString,
		datastoreCursorToString,
		func(ctx context.Context, currentIndex *v1.DatastoreCursor) iter.Seq2[cter.ChunkAndFollowCursor[*relationshipsChunk, *v1.DatastoreCursor], error] {
			ctx, span := tracer.Start(ctx, "relationshipsIter::producer")
			defer span.End()

			return func(yield func(cter.ChunkAndFollowCursor[*relationshipsChunk, *v1.DatastoreCursor], error) bool) {
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				yieldError := func(err error) {
					_ = yield(cter.ChunkAndFollowCursor[*relationshipsChunk, *v1.DatastoreCursor]{}, err)
				}

				// Create a relationships map from the current index cursor, if any. The cursor will contain the *current*
				// chunk of relationships over which the process is iterating. If non-empty, then we first yield that chunk
				// before continuing to query for more relationships.
				rm := relationshipsChunkFromCursor(currentIndex)
				if rm.isPopulated() {
					if !yield(cter.ChunkAndFollowCursor[*relationshipsChunk, *v1.DatastoreCursor]{
						Chunk:  rm,
						Follow: currentIndex,
					}, nil) {
						return
					}
				}

				// Continue by querying for relationships from the datastore, after the optional cursor.
				it, err := lctx.reader.ReverseQueryRelationships(
					ctx,
					config.subjectsFilter,
					options.WithResRelation(&options.ResourceRelation{
						Namespace: config.sourceResourceType.Namespace,
						Relation:  config.sourceResourceType.Relation,
					}),
					options.WithSortForReverse(options.BySubject),
					options.WithAfterForReverse(rm.afterCursor()),
					options.WithQueryShapeForReverse(queryshape.MatchingResourcesForSubject),
				)
				if err != nil {
					yieldError(err)
					return
				}

				// Yield a subject map of chunks of relationships, based on the dispatchChunkSize.
				rm = newRelationshipsChunk(int(crr.dispatchChunkSize))
				for rel, err := range it {
					if err != nil {
						yieldError(err)
						return
					}

					// If a caveat exists on the relationship, run it and filter the results, marking those that have missing context.
					var missingContextParameters []string
					if rel.OptionalCaveat != nil && rel.OptionalCaveat.CaveatName != "" {
						caveatExpr := caveats.CaveatAsExpr(rel.OptionalCaveat)
						runResult, err := lctx.caveatRunner.RunCaveatExpression(ctx, caveatExpr, lctx.req.Context.AsMap(), lctx.reader, caveats.RunCaveatExpressionNoDebugging)
						if err != nil {
							yieldError(err)
							return
						}

						// If a partial result is returned, collect the missing context parameters.
						if runResult.IsPartial() {
							missingNames, err := runResult.MissingVarNames()
							if err != nil {
								yieldError(err)
								return
							}

							missingContextParameters = missingNames
						} else if !runResult.Value() {
							// If the run result shows the caveat does not apply, skip. This shears the tree of results early.
							continue
						}
					}

					// Add the relationship to the relationships chunk, marking it with any context parameters that were missing.
					relCount := rm.addRelationship(rel, missingContextParameters)
					if relCount >= int(crr.dispatchChunkSize) {
						// Yield the chunk of relationships for processing.
						if !yield(cter.ChunkAndFollowCursor[*relationshipsChunk, *v1.DatastoreCursor]{
							Chunk:  rm,
							Follow: rm.asCursor(),
						}, nil) {
							return
						}
						rm = newRelationshipsChunk(int(crr.dispatchChunkSize))
					}
				}

				if rm.isPopulated() {
					// Yield any remaining relationships in the chunk.
					if !yield(cter.ChunkAndFollowCursor[*relationshipsChunk, *v1.DatastoreCursor]{
						Chunk:  rm,
						Follow: rm.asCursor(),
					}, nil) {
						return
					}
				}
			}
		},
		func(ctx context.Context, remainingCursor cter.Cursor, rm *relationshipsChunk) iter.Seq2[result, error] {
			ctx, span := tracer.Start(ctx, "relationshipsIter::mapper")
			defer span.End()

			// Return an iterator to continue finding resources by dispatching.
			// NOTE(jschorr): Technically if we don't have any further entrypoints at this point, we could do checks locally,
			// but that would require additional code just to save one dispatch hop, thus complicating the code base.
			return crr.checkedDispatchIter(ctx, lctx, remainingCursor, rm, config.foundResourceType, config.entrypoint)
		},
	)
}

// checkedDispatchIter is a helper function that checks the subjects in the relationships chunk against the entrypoint
// and dispatches the results. If the entrypoint is a direct result, it simply dispatches the results without checking.
func (crr *CursoredLookupResources3) checkedDispatchIter(
	ctx context.Context,
	lctx lr3ctx,
	currentCursor cter.Cursor,
	rm *relationshipsChunk,
	foundResourceType *core.RelationReference,
	entrypoint schema.ReachabilityEntrypoint,
) iter.Seq2[result, error] {
	ctx, span := tracer.Start(ctx, "checkedDispatchIter", trace.WithAttributes(
		attribute.String("entrypoint", entrypoint.DebugStringOrEmpty()),
	))
	defer span.End()

	// If the entrypoint is a direct result, we can simply dispatch the results, as it means no intersection,
	// exclusion or intersection arrow was found "above" this entrypoint in the permission.
	if entrypoint.IsDirectResult() {
		return crr.dispatchIter(ctx, lctx, currentCursor, rm, foundResourceType, emptyMetadata)
	}

	// Otherwise, we need to check them before dispatching. This shears the tree of results for intersections, exclusions
	// and intersection arrows.
	filteredChunk, checkMetadata, err := crr.filterSubjectsByCheck(ctx, lctx, rm, foundResourceType, entrypoint)
	if err != nil {
		return cter.YieldsError[result](err)
	}

	return crr.dispatchIter(ctx, lctx, currentCursor, filteredChunk, foundResourceType, checkMetadata)
}

// dispatchIter is a helper function that dispatches the resources found in the relationships chunk,
// yielding results for each resource found. This performs the recursive LookupResources operation
// over the resources found in the `rm` chunk.
func (crr *CursoredLookupResources3) dispatchIter(
	ctx context.Context,
	lctx lr3ctx,
	currentCursor cter.Cursor,
	rm *relationshipsChunk,
	foundResourceType *core.RelationReference,
	metadata *v1.ResponseMeta,
) iter.Seq2[result, error] {
	ctx, span := tracer.Start(ctx, "dispatchIter")
	defer span.End()

	if !rm.isPopulated() {
		// If there are no relationships to dispatch, return an empty iterator.
		return cter.UncursoredEmpty[result]()
	}

	subjectIDs := rm.subjectIDsToDispatch()
	currentDispatchCursor := &v1.Cursor{
		DispatchVersion: lr3DispatchVersion,
		Sections:        currentCursor,
	}

	// Return an iterator that invokes the dispatch operation for the given resource relation and subject IDs,
	// yielding results for each resource found.
	return func(yield func(result, error) bool) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		stream := newYieldingStream(ctx, yield, rm, metadata)
		err := crr.dl.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
			ResourceRelation: lctx.req.ResourceRelation,
			SubjectRelation:  foundResourceType,
			SubjectIds:       subjectIDs,
			TerminalSubject:  lctx.req.TerminalSubject,
			Metadata: &v1.ResolverMeta{
				AtRevision:     lctx.req.Revision.String(),
				DepthRemaining: lctx.req.Metadata.DepthRemaining - 1,
			},
			OptionalCursor: currentDispatchCursor,
			OptionalLimit:  lctx.req.OptionalLimit,
			Context:        lctx.req.Context,
		}, stream)
		if err != nil && !stream.canceled {
			_ = yield(result{}, err)
			return
		}
	}
}

// filterSubjectsByCheck filters the subjects in the relationships chunk by checking them against the entrypoint.
// This handles intersections, exclusions and intersection arrows by using the DispatchCheck operation to shear
// the tree where applicable.
func (crr *CursoredLookupResources3) filterSubjectsByCheck(
	ctx context.Context,
	lctx lr3ctx,
	rm *relationshipsChunk,
	foundResourceType *core.RelationReference,
	entrypoint schema.ReachabilityEntrypoint,
) (*relationshipsChunk, *v1.ResponseMeta, error) {
	resourceIDsToCheck := rm.subjectIDsToDispatch()

	// Build a set of check hints for the resources to check, thus avoiding the work
	// LookupResources just performed to find these resources.
	checkHints := make([]*v1.CheckHint, 0, len(resourceIDsToCheck))
	for _, resourceIDToCheck := range resourceIDsToCheck {
		checkHint, err := hints.HintForEntrypoint(
			entrypoint,
			resourceIDToCheck,
			tuple.FromCoreObjectAndRelation(lctx.req.TerminalSubject),
			&v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			})
		if err != nil {
			return nil, nil, err
		}
		checkHints = append(checkHints, checkHint)
	}

	// NOTE: we are checking the containing permission here, *not* the target relation, as
	// the goal is to shear for the containing permission.
	resultsByResourceID, checkMetadata, _, err := computed.ComputeBulkCheck(ctx, crr.dc, crr.caveatTypeSet, computed.CheckParameters{
		ResourceType:  tuple.FromCoreRelationReference(foundResourceType),
		Subject:       tuple.FromCoreObjectAndRelation(lctx.req.TerminalSubject),
		CaveatContext: lctx.req.Context.AsMap(),
		AtRevision:    lctx.req.Revision,
		MaximumDepth:  lctx.req.Metadata.DepthRemaining - 1,
		DebugOption:   computed.NoDebugging,
		CheckHints:    checkHints,
	}, resourceIDsToCheck, crr.dispatchChunkSize)
	if err != nil {
		return nil, nil, err
	}

	// Dispatch any resources that are visible.
	updatedChunk := newRelationshipsChunk(len(resourceIDsToCheck))
	for _, resourceID := range resourceIDsToCheck {
		result, ok := resultsByResourceID[resourceID]
		if !ok {
			continue
		}

		switch result.Membership {
		case v1.ResourceCheckResult_MEMBER:
			// If the found resource is a member, we copy the relationships from the
			// relationships chunk to the updated chunk, as this is a valid resource.
			updatedChunk.copyRelationshipsFrom(rm, resourceID, nil)

		case v1.ResourceCheckResult_CAVEATED_MEMBER:
			// If the found resource is a caveated member, we copy the relationships from the
			// relationships chunk to the updated chunk, but also include the missing context parameters.
			updatedChunk.copyRelationshipsFrom(rm, resourceID, result.MissingExprFields)

		case v1.ResourceCheckResult_NOT_MEMBER:
			// If the found resource is not a member, we do not include it in the updated chunk.
			// This effectively shears the tree of results, as we do not dispatch these resources.
			continue

		default:
			return nil, nil, spiceerrors.MustBugf("unexpected result from check: %v", result.Membership)
		}
	}

	return updatedChunk, checkMetadata, nil
}

// newYieldingStream creates a new yielding stream for the LookupResources3 operation, which takes results
// published to the stream and yields them to the provided yield function. The stream will cancel
// itself if the yield function returns false, indicating that the stream is no longer interested in results.
func newYieldingStream(ctx context.Context, yield func(result, error) bool, rm *relationshipsChunk, metadata *v1.ResponseMeta) *yieldingStream {
	ctx, cancel := context.WithCancel(ctx)
	return &yieldingStream{
		ctx:                ctx,
		cancel:             cancel,
		yield:              yield,
		rm:                 rm,
		metadata:           metadata,
		isFirstPublishCall: true,
		canceled:           false,
	}
}

// yieldingStream is a stream implementation for the LookupResources3 operation that yields results
// to a provided yield function.
type yieldingStream struct {
	// ctx is the context for the stream, which will be canceled when the stream is no longer interested in results.
	ctx context.Context

	// cancel is the cancel function for the stream context, which will be called when the stream is no longer interested in results.
	cancel context.CancelFunc

	// yield is the function that will be called to yield results to the stream.
	yield func(result, error) bool

	// rm is the relationships chunk that contains the relationships to dispatch.
	rm *relationshipsChunk

	// metadata is the metadata for the stream, which will be combined with the response metadata.
	metadata *v1.ResponseMeta

	// canceled indicates whether the stream has been canceled, which will prevent further publishing of results.
	canceled bool

	// isFirstPublishCall indicates whether this is the first call to Publish, which is used to combine metadata
	// with the initial response metadata.
	isFirstPublishCall bool
}

func (y *yieldingStream) Context() context.Context {
	return y.ctx
}

func (y *yieldingStream) Publish(resp *v1.DispatchLookupResources3Response) error {
	if y.canceled {
		return context.Canceled
	}

	if resp == nil {
		return spiceerrors.MustBugf("cannot publish nil response")
	}

	if resp.Metadata == nil || resp.Resource == nil {
		return spiceerrors.MustBugf("invalid response from lookup resources: missing metadata or resource")
	}

	// Check if the context is done before publishing, to avoid unnecessary work.
	select {
	case <-y.ctx.Done():
		return y.ctx.Err()
	default:
		// publish
	}

	// If this is the first publish call, we combine the response metadata with the initial metadata,
	// to ensure the check's metadata is represented in at least one of the responses.
	metadata := resp.Metadata
	if y.isFirstPublishCall {
		metadata = combineResponseMetadata(y.ctx, metadata, y.metadata)
	}

	// Map the possible resource from the response, which combines missing context parameters
	// from the existing possible resource with that published.
	possibleResource, err := y.rm.mapPossibleResource(resp.Resource)
	if err != nil {
		return err
	}

	y.isFirstPublishCall = false
	yielded := y.yield(result{
		Item: pram{
			possibleResource: possibleResource,
			metadata:         metadata,
		},
		Cursor: resp.AfterResponseCursor.Sections,
	}, nil)
	if !yielded {
		y.canceled = true
		y.cancel()
		return context.Canceled
	}

	return nil
}

var _ dispatch.LookupResources3Stream = &yieldingStream{}

// relationshipsChunk is a chunk of relationships, produced by the relationships iterator,
// that can be used to dispatch further resources. It contains the relationships
// and the resources that are found in the relationships, along with any missing context parameters
// that were found during the relationship processing.
type relationshipsChunk struct {
	// underlyingCursor is the protocol buffer representation of the relationships chunk.
	underlyingCursor *v1.DatastoreCursor
}

// isPopulated returns true if the relationships chunk contains any relationships.
func (rm *relationshipsChunk) isPopulated() bool {
	return len(rm.underlyingCursor.OrderedRelationships) > 0
}

// addRelationship adds a relationship to the relationships chunk, creating the necessary
// resource and subject entries if they do not exist. It also collects any missing context parameters
// that were found during the relationship processing.
func (rm *relationshipsChunk) addRelationship(rel tuple.Relationship, missingContextParameters []string) int {
	existingResource, ok := rm.underlyingCursor.Resources[rel.Resource.ObjectID]
	if !ok {
		existingResource = &v1.ResourceAndMissingContext{
			ResourceId:                rel.Resource.ObjectID,
			MissingContextBySubjectId: make(map[string]*v1.SubjectAndMissingContext, 1),
		}
		rm.underlyingCursor.Resources[rel.Resource.ObjectID] = existingResource
	}

	existingSubject, ok := existingResource.MissingContextBySubjectId[rel.Subject.ObjectID]
	if !ok {
		existingSubject = &v1.SubjectAndMissingContext{
			SubjectId:            rel.Subject.ObjectID,
			MissingContextParams: make([]string, 0, len(missingContextParameters)),
		}
		existingResource.MissingContextBySubjectId[rel.Subject.ObjectID] = existingSubject
	}

	mcpSet := mapz.NewSet(existingSubject.MissingContextParams...)
	mcpSet.Extend(missingContextParameters)
	existingSubject.MissingContextParams = mcpSet.AsSlice()

	rm.underlyingCursor.OrderedRelationships = append(rm.underlyingCursor.OrderedRelationships, rel.ToCoreTuple())
	return len(rm.underlyingCursor.Resources)
}

// copyRelationshipsFrom copies relationships from another relationships chunk for a specific resource ID,
// adding them to the current relationships chunk. It also updates the missing context parameters for the
// resource if they are not already present. This is used for aggregating relationships from a source chunk
// after it has been checked.
func (rm *relationshipsChunk) copyRelationshipsFrom(src *relationshipsChunk, resourceID string, missingContextParameters []string) {
	if src == nil || !src.isPopulated() {
		return
	}

	// Copy the relationships for the given resource ID from the source chunk.
	for _, rel := range src.underlyingCursor.OrderedRelationships {
		if rel.ResourceAndRelation.ObjectId == resourceID {
			combinedMissingContextParameters := mapz.NewSet[string]()
			combinedMissingContextParameters.Extend(missingContextParameters)

			if existingResource, ok := src.underlyingCursor.Resources[resourceID]; ok {
				if existingSubject, ok := existingResource.MissingContextBySubjectId[rel.Subject.ObjectId]; ok {
					combinedMissingContextParameters.Extend(existingSubject.MissingContextParams)
				}
			}

			rm.addRelationship(tuple.FromCoreRelationTuple(rel), combinedMissingContextParameters.AsSlice())
		}
	}
}

// afterCursor returns the datastore cursor for the next chunk of relationships, if any.
func (rm *relationshipsChunk) afterCursor() options.Cursor {
	if len(rm.underlyingCursor.OrderedRelationships) == 0 {
		return nil
	}

	lastRel := rm.underlyingCursor.OrderedRelationships[len(rm.underlyingCursor.OrderedRelationships)-1]
	return options.ToCursor(tuple.FromCoreRelationTuple(lastRel))
}

// asCursor returns the Protocol Buffer representation of the relationships chunk.
func (rm *relationshipsChunk) asCursor() *v1.DatastoreCursor {
	return rm.underlyingCursor
}

// subjectIDsToDispatch returns the subject IDs that should be dispatched from the relationships chunk.
func (rm *relationshipsChunk) subjectIDsToDispatch() []string {
	// NOTE: the method is called subjectIDsToDispatch, but we are returning the resource IDs
	// here. This is because once we've collected resource IDs, they become the subject IDs for
	// the *next* dispatch operation.
	return slices.Collect(maps.Keys(rm.underlyingCursor.Resources))
}

// mapPossibleResource maps a possible resource from the relationships chunk, collecting the subject IDs
// and missing context parameters from the relationships chunk. It returns a new possible resource
// that contains the mapped subject IDs and missing context parameters, or an error if the mapping fails.
func (rm *relationshipsChunk) mapPossibleResource(foundResource *v1.PossibleResource) (*v1.PossibleResource, error) {
	forSubjectIDs := mapz.NewSet[string]()
	nonCaveatedSubjectIDs := mapz.NewSet[string]()

	missingContextParameters := mapz.NewSet[string]()
	missingContextParameters.Extend(foundResource.MissingContextParams)

	for _, forSubjectID := range foundResource.ForSubjectIds {
		// Map from the incoming subject ID to the subject ID(s) that caused the dispatch.
		infos, ok := rm.underlyingCursor.Resources[forSubjectID]
		if !ok {
			return nil, spiceerrors.MustBugf("missing for subject ID")
		}

		for _, info := range infos.MissingContextBySubjectId {
			forSubjectIDs.Insert(info.SubjectId)
			if len(info.MissingContextParams) == 0 {
				nonCaveatedSubjectIDs.Insert(info.SubjectId)
			} else {
				missingContextParameters.Extend(info.MissingContextParams)
			}
		}
	}

	// If there are some non-caveated IDs, return those and mark as the parent status.
	if len(foundResource.MissingContextParams) == 0 && nonCaveatedSubjectIDs.Len() > 0 {
		return &v1.PossibleResource{
			ResourceId:    foundResource.ResourceId,
			ForSubjectIds: nonCaveatedSubjectIDs.AsSlice(),
		}, nil
	}

	// Otherwise, everything is caveated, so return the full set of subject IDs and mark
	// as a check is required.
	return &v1.PossibleResource{
		ResourceId:           foundResource.ResourceId,
		ForSubjectIds:        forSubjectIDs.AsSlice(),
		MissingContextParams: missingContextParameters.AsSlice(),
	}, nil
}

// relationshipsChunkFromCursor creates a new relationships chunk from the given datastore cursor.
// If the cursor is nil or has no ordered relationships, it returns a new empty relationships chunk.
func relationshipsChunkFromCursor(cursor *v1.DatastoreCursor) *relationshipsChunk {
	if cursor == nil || len(cursor.OrderedRelationships) == 0 {
		return newRelationshipsChunk(0)
	}
	return &relationshipsChunk{underlyingCursor: cursor}
}

// newRelationshipsChunk creates a new relationships chunk with the specified capacity.
func newRelationshipsChunk(capacity int) *relationshipsChunk {
	return &relationshipsChunk{
		underlyingCursor: &v1.DatastoreCursor{
			OrderedRelationships: make([]*core.RelationTuple, 0, capacity),
			Resources:            make(map[string]*v1.ResourceAndMissingContext, capacity),
		},
	}
}

// subjectIDsToRelationshipsChunk creates a relationships chunk from the given subject IDs and the rewritten subject relation.
func subjectIDsToRelationshipsChunk(
	subjectRelation *core.RelationReference,
	subjectIDs []string,
	rewrittenSubjectRelation *core.RelationReference,
) *relationshipsChunk {
	rm := newRelationshipsChunk(len(subjectIDs))
	for _, subjectID := range subjectIDs {
		// Create a relationship for the subject ID with the rewritten relation.
		rel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{
					ObjectType: rewrittenSubjectRelation.Namespace,
					ObjectID:   subjectID,
					Relation:   rewrittenSubjectRelation.Relation,
				},
				Subject: tuple.ObjectAndRelation{
					ObjectType: subjectRelation.Namespace,
					ObjectID:   subjectID,
					Relation:   subjectRelation.Relation,
				},
			},
		}

		rm.addRelationship(rel, nil) // no missing context for direct lookup
	}
	return rm
}

// datastoreCursorFromString converts a base64-encoded string representation of a datastore cursor
// into a *v1.DatastoreCursor. If the string is empty, it returns nil.
func datastoreCursorFromString(cursorStr string) (*v1.DatastoreCursor, error) {
	// Convert the string from base64 to bytes.
	if cursorStr == "" {
		return nil, nil // no cursor to return
	}

	decodedBytes, err := base64.StdEncoding.DecodeString(cursorStr)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor string: %w", err)
	}

	decoded := &v1.DatastoreCursor{}
	err = decoded.UnmarshalVT(decodedBytes)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor data: %w", err)
	}

	return decoded, nil
}

// datastoreCursorToString converts a *v1.DatastoreCursor into a base64-encoded string representation.
// If the cursor is nil or has no ordered relationships, it returns an empty string.
func datastoreCursorToString(cursor *v1.DatastoreCursor) (string, error) {
	if cursor == nil {
		return "", nil
	}

	if len(cursor.OrderedRelationships) == 0 {
		return "", nil // no cursor to return
	}

	encodedBytes, err := cursor.MarshalVT()
	if err != nil {
		return "", fmt.Errorf("failed to marshal datastore cursor: %w", err)
	}

	return base64.StdEncoding.EncodeToString(encodedBytes), nil
}
