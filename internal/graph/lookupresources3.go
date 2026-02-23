package graph

import (
	"context"
	"iter"
	"slices"
	"sort"
	"strings"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/caveats"
	cter "github.com/authzed/spicedb/internal/cursorediterator"
	"github.com/authzed/spicedb/internal/digests"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/internal/graph/hints"
	"github.com/authzed/spicedb/internal/telemetry/otelconv"
	"github.com/authzed/spicedb/pkg/cache"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
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

const respBatchSize = 500

// NewCursoredLookupResources3 creates a new CursoredLookupResources3 instance with the given parameters.
func NewCursoredLookupResources3(
	dl dispatch.LookupResources3,
	dc dispatch.Check,
	caveatTypeSet *caveattypes.TypeSet,
	concurrencyLimit uint16,
	dispatchChunkSize uint16,
	chunkCache cache.Cache[cache.StringKey, any],
) (*CursoredLookupResources3, error) {
	rcc := &relationshipsChunkCache{
		cache: chunkCache,
	}

	return &CursoredLookupResources3{
		dl:                      dl,
		dc:                      dc,
		caveatTypeSet:           caveatTypeSet,
		concurrencyLimit:        concurrencyLimit,
		dispatchChunkSize:       dispatchChunkSize,
		digestMap:               digests.NewDigestMap(),
		relationshipsChunkCache: rcc,
	}, nil
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

	// digestMap is a map of digest keys to t-digests used for concurrency calculations.
	digestMap *digests.DigestMap

	// relationshipsChunkCache is a cache for storing relationships chunks across calls.
	relationshipsChunkCache *relationshipsChunkCache
}

// ValidatedLookupResources3Request is a wrapper around the DispatchLookupResources3Request
// that includes a revision for the datastore reader, and indicates that the request has been validated.
type ValidatedLookupResources3Request struct {
	*v1.DispatchLookupResources3Request
	Revision datastore.Revision
}

func (crr *CursoredLookupResources3) Close() {
	if crr.relationshipsChunkCache != nil {
		crr.relationshipsChunkCache.Close()
	}
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
	ctx, span := tracer.Start(
		stream.Context(),
		otelconv.EventDispatchLookupResources3,
		trace.WithAttributes(
			attribute.String(otelconv.AttrDispatchResourceType, req.ResourceRelation.Namespace),
			attribute.String(otelconv.AttrDispatchResourceRelation, req.ResourceRelation.Relation),
			attribute.String(otelconv.AttrDispatchSubjectType, req.SubjectRelation.Namespace),
			attribute.StringSlice(otelconv.AttrDispatchSubjectIDs, req.SubjectIds),
			attribute.String(otelconv.AttrDispatchSubjectRelation, req.SubjectRelation.Relation),
			attribute.String(otelconv.AttrDispatchTerminalSubject, tuple.StringCoreONR(req.TerminalSubject)),
			attribute.Int(otelconv.AttrDispatchCursorLimit, int(req.OptionalLimit)),
		),
	)
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

	// Build refs for the lookup resources operation. The lr3refs holds references to shared
	// interfaces used by various suboperations of the lookup resources operation.
	dl := datalayer.MustFromContext(stream.Context())
	reader := dl.SnapshotReader(req.Revision)
	sr, err := reader.ReadSchema()
	if err != nil {
		return err
	}
	ts := schema.NewTypeSystem(schema.ResolverFor(sr))
	caveatRunner := caveats.NewCaveatRunner(crr.caveatTypeSet)

	refs := lr3refs{
		req:              req,
		reader:           reader,
		ts:               ts,
		caveatRunner:     caveatRunner,
		concurrencyLimit: crr.concurrencyLimit,
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// If no limit is set, disable cursors for the entire operation, as they won't be returned.
	if req.OptionalLimit == 0 {
		ctx = cter.DisableCursorsInContext(ctx)
	}

	// Retrieve results from the unlimited iterator and publish them, respecting the limit,
	// if any.
	resultCount := uint32(0)
	lr3items := make([]*v1.LR3Item, 0, respBatchSize)

	for result, err := range crr.unlimitedLookupResourcesIter(ctx, refs) {
		if err != nil {
			span.RecordError(err)
			return err
		}

		lr3items = append(lr3items, &v1.LR3Item{
			ResourceId:                  result.Item.resourceID,
			ForSubjectIds:               result.Item.forSubjectIDs,
			MissingContextParams:        result.Item.missingContextParams,
			AfterResponseCursorSections: result.Cursor,
		})

		resultCount++
		if req.OptionalLimit > 0 && resultCount == req.OptionalLimit {
			break
		}

		if len(lr3items) < respBatchSize {
			continue
		}

		if err := stream.Publish(&v1.DispatchLookupResources3Response{
			Items: lr3items,
		}); err != nil {
			span.RecordError(err)
			return err
		}

		lr3items = make([]*v1.LR3Item, 0, respBatchSize)
	}

	if len(lr3items) > 0 {
		if err := stream.Publish(&v1.DispatchLookupResources3Response{
			Items: lr3items,
		}); err != nil {
			span.RecordError(err)
			return err
		}
	}

	return nil
}

// possibleResource is a helper struct that combines a possible resource.
type possibleResource struct {
	resourceID           string
	forSubjectIDs        []string
	missingContextParams []string
}

// result is the result type for the lookup resources iterator, which contains a possible resource and metadata,
// as well as the cursor for the next iteration.
type result = cter.ItemAndCursor[possibleResource]

// lr3refs is a holder for various capabilities used by the LookupResources3 operation.
type lr3refs struct {
	// req is the validated request for the LookupResources3 operation.
	req ValidatedLookupResources3Request

	// reader is the datastore reader used to perform the lookup resources operation.
	reader datalayer.RevisionedReader

	// ts is the type system used to resolve types and relations for the lookup resources operation.
	ts *schema.TypeSystem

	// caveatRunner is the caveat runner used to run caveats for the lookup resources operation.
	caveatRunner *caveats.CaveatRunner

	// concurrencyLimit is the maximum number of concurrent operations that can be performed.
	concurrencyLimit uint16
}

// unlimitedLookupResourcesIter performs the actual lookup resources operation, returning an iterator
// sequence of results. This iterator will yield results until all results are reached
// or an error occurs. It is the responsibility of the caller to handle limits.
func (crr *CursoredLookupResources3) unlimitedLookupResourcesIter(
	ctx context.Context,
	refs lr3refs,
) iter.Seq2[result, error] {
	ctx, span := tracer.Start(ctx, otelconv.EventDispatchLR3UnlimitedResults)
	defer span.End()

	cursor := refs.req.OptionalCursor

	// Portion #1: If the requested resource type+relation matches the subject type+relation, then we've immediately
	// found matching resources, as a permission always matches itself.
	return cter.CursoredWithIntegerHeader(ctx, cursor,
		func(ctx context.Context, startIndex int) iter.Seq2[possibleResource, error] {
			// If the subject relation does not match the resource relation, we cannot yield any results for this
			// portion.
			if refs.req.SubjectRelation.Namespace != refs.req.ResourceRelation.Namespace ||
				refs.req.SubjectRelation.Relation != refs.req.ResourceRelation.Relation {
				return cter.UncursoredEmpty[possibleResource]()
			}

			// Return an iterator that yields results for all subject IDs starting from startIndex.
			iter := func(yield func(possibleResource, error) bool) {
				for _, subjectID := range refs.req.SubjectIds[startIndex:] {
					if !yield(possibleResource{
						resourceID:    subjectID,
						forSubjectIDs: []string{subjectID},
					}, nil) {
						return
					}
				}
			}

			return cter.Spanned(
				ctx,
				iter,
				tracer,
				otelconv.EventDispatchLR3UnlimitedResultsDirectSubjects,
				attribute.String(otelconv.AttrDispatchSubjectType, refs.req.SubjectRelation.Namespace),
				attribute.String(otelconv.AttrDispatchSubjectRelation, refs.req.SubjectRelation.Relation),
			)
		},

		// Portion #2: Next, we need to iterate over the entrypoints for the given subject IDs and resource relation,
		// yielding results for each entrypoint. An entrypoint is any relation or permission that is reachable from the
		// current subject type. For example, if the subject type is "user" and the target resource+permission is "document:read",
		// the entrypoints could be "document#viewer" and "document#editor", as they are the relations that must walked
		// *from* the user to reach the `read` permission on the `document`.
		crr.entrypointsIter(refs),
	)
}

// entrypointsIter is the second portion of the lookup resources iterator, which iterates over
// the entrypoints for the given subject IDs and yields results for each entrypoint.
func (crr *CursoredLookupResources3) entrypointsIter(refs lr3refs) cter.Next[possibleResource] {
	return func(ctx context.Context, currentCursor cter.Cursor) iter.Seq2[result, error] {
		// Load the entrypoints that are reachable from the subject relation to the resource relation/permission.
		entrypoints, err := crr.loadEntrypoints(ctx, refs)
		if err != nil {
			return cter.YieldsError[result](err)
		}

		if len(entrypoints) == 0 {
			return cter.UncursoredEmpty[result]()
		}

		// For each entrypoint, create an iterator that will yield results for that entrypoint via
		// the entrypointIter method.
		entrypointIterators := make([]cter.Next[possibleResource], 0, len(entrypoints))
		for _, ep := range entrypoints {
			entrypointIterators = append(entrypointIterators, crr.entrypointIter(refs, ep))
		}

		// Execute the iterators in parallel, yielding results for each entrypoint in the proper order.
		iter := estimatedConcurrencyLimit(crr, refs, keyedEntrypoints(entrypoints), func(computedConcurrencyLimit uint16) iter.Seq2[result, error] {
			ccl, err := safecast.Convert[int64](computedConcurrencyLimit)
			if err != nil {
				return cter.YieldsError[result](err)
			}

			iter := cter.CursoredParallelIterators(ctx, currentCursor, computedConcurrencyLimit, entrypointIterators...)
			return cter.Spanned(
				ctx,
				iter,
				tracer,
				otelconv.EventDispatchLookupResources3ConcurrentEntrypointsIter,
				attribute.Int64(otelconv.AttrDispatchLRConcurrencyLimit, ccl),
			)
		})
		return cter.Spanned(
			ctx,
			iter,
			tracer,
			otelconv.EventDispatchLookupResources3EntrypointsIter,
			attribute.Int(otelconv.AttrDispatchLREntrypointCount, len(entrypoints)),
		)
	}
}

// loadEntrypoints loads the entrypoints for the given subject relation and resource relation found in the request.
func (crr *CursoredLookupResources3) loadEntrypoints(ctx context.Context, refs lr3refs) ([]schema.ReachabilityEntrypoint, error) {
	// The entrypoints for a particular subject relation to a resource relation are defined by the reachability
	// graph, which is built from the type system definition.
	vdef, err := refs.ts.GetValidatedDefinition(ctx, refs.req.ResourceRelation.Namespace)
	if err != nil {
		return nil, err
	}

	rg := vdef.Reachability(refs.ts)
	return rg.FirstEntrypointsForSubjectToResource(ctx, &core.RelationReference{
		Namespace: refs.req.SubjectRelation.Namespace,
		Relation:  refs.req.SubjectRelation.Relation,
	}, refs.req.ResourceRelation)
}

// entrypointIter is the iterator for a single entrypoint, which will yield results for the given
// entrypoint and the subject IDs in the request. It will return an iterator sequence of results
// for each entrypoint, dispatching further when necessary.
func (crr *CursoredLookupResources3) entrypointIter(refs lr3refs, entrypoint schema.ReachabilityEntrypoint) cter.Next[possibleResource] {
	return func(ctx context.Context, currentCursor cter.Cursor) iter.Seq2[result, error] {
		switch entrypoint.EntrypointKind() {
		case core.ReachabilityEntrypoint_SELF_ENTRYPOINT:
			// Self refers to the resource itself as a subject with relation `...`
			// The subject IDs directly correspond to resource IDs of the containing relation.
			containingRelation := entrypoint.ContainingRelationOrPermission()

			// Create a synthetic relationships chunk that maps the subject IDs to resources
			// of the containing relation type.
			rm := subjectIDsToRelationshipsChunk(refs.req.SubjectRelation, refs.req.SubjectIds, containingRelation)

			// Dispatch to continue finding resources up the tree.
			return crr.checkedDispatchIter(ctx, refs, currentCursor, rm, containingRelation, entrypoint)

		// If the entrypoint is a relation entrypoint, we need to iterate over the relation's relationships
		// for the given subject IDs and yield results for dispatching for each. For example, given a relation
		// of `relation viewer: user`, we would lookup all relationships for the current user(s) and find
		// all the document(s) on which the user is a viewer.
		case core.ReachabilityEntrypoint_RELATION_ENTRYPOINT:
			iter := crr.relationEntrypointIter(ctx, refs, entrypoint, currentCursor)
			return cter.Spanned(
				ctx,
				iter,
				tracer,
				otelconv.EventDispatchLookupResources3RelationEntrypoint,
				attribute.String(otelconv.AttrDispatchLREntrypoint, entrypoint.DebugStringOrEmpty()),
			)

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
			rm := subjectIDsToRelationshipsChunk(refs.req.SubjectRelation, refs.req.SubjectIds, rewrittenSubjectRelation)

			// Dispatch the rewritten subjects, to further find resources up the tree.
			return crr.checkedDispatchIter(ctx, refs, currentCursor, rm, rewrittenSubjectRelation, entrypoint)

		// If the entrypoint is a TTU entrypoint (arrow), we need to iterate over the tupleset's relationships
		// for the given subject IDs and yield results for dispatching for each, rewriting based on the containing
		// relation. For example, given an arrow of `parent->view`, we would lookup all relationships for the `parent`
		// relation and then *dispatch* to the `view` permission for each.
		case core.ReachabilityEntrypoint_TUPLESET_TO_USERSET_ENTRYPOINT:
			iter := crr.ttuEntrypointIter(ctx, refs, entrypoint, currentCursor)
			return cter.Spanned(
				ctx,
				iter,
				tracer,
				otelconv.EventDispatchLookupResources3ArrowEntrypoint,
				attribute.String(otelconv.AttrDispatchLREntrypoint, entrypoint.DebugStringOrEmpty()),
			)

		default:
			return cter.YieldsError[result](spiceerrors.MustBugf("Unknown kind of entrypoint: %v", entrypoint.EntrypointKind()))
		}
	}
}

// relationEntrypointIter returns an iterator for a relation entrypoint, which will yield results
// found by traversing the relation for the given entrypoint and the subject IDs in the request.
func (crr *CursoredLookupResources3) relationEntrypointIter(
	ctx context.Context,
	refs lr3refs,
	entrypoint schema.ReachabilityEntrypoint,
	currentCursor cter.Cursor,
) iter.Seq2[result, error] {
	// Build a subject filter for the subjects for which to lookup relationships.
	relationReference, err := entrypoint.DirectRelation()
	if err != nil {
		return cter.YieldsError[result](err)
	}

	relDefinition, err := refs.ts.GetValidatedDefinition(ctx, relationReference.Namespace)
	if err != nil {
		return cter.YieldsError[result](err)
	}

	// Build the list of subjects to lookup based on the type information available.
	isDirectAllowed, err := relDefinition.IsAllowedDirectRelation(
		relationReference.Relation,
		refs.req.SubjectRelation.Namespace,
		refs.req.SubjectRelation.Relation,
	)
	if err != nil {
		return cter.YieldsError[result](err)
	}

	subjectIds := make([]string, 0, len(refs.req.SubjectIds)+1)

	// If the subject relation is allowed directly on the relation, we can use the subject IDs
	// directly, as they are valid subjects for the relation.
	if isDirectAllowed == schema.DirectRelationValid {
		subjectIds = append(subjectIds, refs.req.SubjectIds...)
	}

	// If the subject relation is a terminal subject, check for a wildcard. For example,
	// if the subject type is `user`, then `user:*` would also match any subjects of that
	// type.
	if refs.req.SubjectRelation.Relation == tuple.Ellipsis {
		isWildcardAllowed, err := relDefinition.IsAllowedPublicNamespace(relationReference.Relation, refs.req.SubjectRelation.Namespace)
		if err != nil {
			return cter.YieldsError[result](err)
		}

		if isWildcardAllowed == schema.PublicSubjectAllowed {
			subjectIds = append(subjectIds, "*")
		}
	}

	relationFilter := datastore.SubjectRelationFilter{
		NonEllipsisRelation: refs.req.SubjectRelation.Relation,
	}

	if refs.req.SubjectRelation.Relation == tuple.Ellipsis {
		relationFilter = datastore.SubjectRelationFilter{
			IncludeEllipsisRelation: true,
		}
	}

	subjectsFilter := datastore.SubjectsFilter{
		SubjectType:        refs.req.SubjectRelation.Namespace,
		OptionalSubjectIds: subjectIds,
		RelationFilter:     relationFilter,
	}

	// Lookup relationships for the given subjects and relation reference, dispatching over the results.
	return crr.relationshipsIter(ctx, refs, currentCursor, relationshipsIterConfig{
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
	refs lr3refs,
	entrypoint schema.ReachabilityEntrypoint,
	currentCursor cter.Cursor,
) iter.Seq2[result, error] {
	containingRelation := entrypoint.ContainingRelationOrPermission()

	ttuDef, err := refs.ts.GetValidatedDefinition(ctx, containingRelation.Namespace)
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
	allowedRelations, err := ttuDef.GetAllowedDirectNamespaceSubjectRelations(tuplesetRelation, refs.req.SubjectRelation.Namespace)
	if err != nil {
		return cter.YieldsError[result](err)
	}

	if allowedRelations == nil {
		return cter.UncursoredEmpty[result]()
	}

	// Search for the resolved subjects in the tupleset of the TTU.
	subjectsFilter := datastore.SubjectsFilter{
		SubjectType:        refs.req.SubjectRelation.Namespace,
		OptionalSubjectIds: refs.req.SubjectIds,
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
	return crr.relationshipsIter(ctx, refs, currentCursor, relationshipsIterConfig{
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

type coh = cter.ChunkOrHold[*relationshipsChunk, datastoreIndex]

// relationshipsIter is an iterator that fetches relationships for the given subjects and processes them
// by further dispatching the found resources as the next step's subjects.
func (crr *CursoredLookupResources3) relationshipsIter(
	ctx context.Context,
	refs lr3refs,
	currentCursor cter.Cursor,
	config relationshipsIterConfig,
) iter.Seq2[result, error] {
	// The relationships iterator operates as a producer-mapper iterator, where the producer
	// fetches relationships for the given subjects and the mapper processes those relationships
	// to yield possible resources and metadata for the lookup resources response.
	//
	// The producer will yield chunks of relationships, maximum of the dispatchChunkSize, to be
	// dispatched by the mapper in parallel.
	return estimatedConcurrencyLimit(crr, refs, keyedEntrypoint(config.entrypoint), func(computedConcurrencyLimit uint16) iter.Seq2[result, error] {
		return cter.CursoredProducerMapperIterator(ctx, currentCursor,
			computedConcurrencyLimit,
			datastoreIndexFromString,
			mustDatastoreIndexToString,
			func(ctx context.Context, currentIndex datastoreIndex, remainingCursor cter.Cursor) iter.Seq2[coh, error] {
				ccl, err := safecast.Convert[int64](computedConcurrencyLimit)
				if err != nil {
					return cter.YieldsError[coh](err)
				}

				iter := func(yield func(coh, error) bool) {
					yieldError := func(err error) {
						_ = yield(cter.Chunk[*relationshipsChunk, datastoreIndex]{}, err)
					}

					dbCursor := currentIndex.toDatastoreCursor()

					// Create a relationships map from the current index cursor, if any. The cursor will contain the *current*
					// chunk of relationships over which the process is iterating. If non-empty, then we first yield that chunk
					// before continuing to query for more relationships.
					if crc, ok := currentIndex.lookupRelationshipsChunk(crr.relationshipsChunkCache); ok {
						spiceerrors.DebugAssertf(crc.isPopulated, "expected relationships chunk to be populated")

						// Yield the current chunk of relationships.
						if !yield(cter.Chunk[*relationshipsChunk, datastoreIndex]{
							CurrentChunk:       crc,
							CurrentChunkCursor: currentIndex,
						}, nil) {
							return
						}

						// Since the chunk was already processed, we move the cursor forward to the next position.
						dbCursor = &crc.finalRelationship

						// Check if the remaining cursor contains another datastore index. If so, we have this producer
						// yield a "hold" to allow mapping/dispatching to process *before* we hit the datastore again.
						//
						// This handles the common case where the limited number of results requested is reached without having
						// to load further information from the datastore, thus saving on unnecessary work.
						//
						// We only do so if there is a defined limit on the number of results to fetch, as otherwise, we need
						// to run the full set of work anyway.
						if refs.req.OptionalLimit > 0 && containsDatastoreIndex(remainingCursor) {
							// We have a remaining cursor, so we yield a hold to allow the mapper to process the current chunk
							// before we continue fetching more relationships.
							if !yield(cter.HoldForMappingComplete[*relationshipsChunk, datastoreIndex]{}, nil) {
								return
							}
						}
					}

					// Determine whether we need to fetch caveats and/or expiration.
					vts, err := refs.ts.GetValidatedDefinition(ctx, config.sourceResourceType.Namespace)
					if err != nil {
						yieldError(err)
						return
					}

					possibleTraits, err := vts.PossibleTraitsForSubject(config.sourceResourceType.Relation, config.subjectsFilter.SubjectType)
					if err != nil {
						yieldError(err)
						return
					}

					skipCaveats := !possibleTraits.AllowsCaveats
					skipExpiration := !possibleTraits.AllowsExpiration

					// Continue by querying for relationships from the datastore, after the optional cursor.
					it, err := refs.reader.ReverseQueryRelationships(
						ctx,
						config.subjectsFilter,
						options.WithResRelation(&options.ResourceRelation{
							Namespace: config.sourceResourceType.Namespace,
							Relation:  config.sourceResourceType.Relation,
						}),
						options.WithSortForReverse(options.BySubject),
						options.WithAfterForReverse(dbCursor),
						options.WithQueryShapeForReverse(queryshape.MatchingResourcesForSubject),
						options.WithSkipCaveatsForReverse(skipCaveats),
						options.WithSkipExpirationForReverse(skipExpiration),
					)
					if err != nil {
						yieldError(err)
						return
					}

					// Start the new relationships chunk that we will fill as we iterate over the results.
					// It starts at the given dbCursor, which may be nil/empty if this is the first chunk.
					caveatSR, err := refs.reader.ReadSchema()
					if err != nil {
						yieldError(err)
						return
					}

					rm := newRelationshipsChunk(int(crr.dispatchChunkSize), dbCursor)
					for rel, err := range it {
						if err != nil {
							yieldError(err)
							return
						}

						// If a caveat exists on the relationship, run it and filter the results, marking those that have missing context.
						var missingContextParameters []string
						if rel.OptionalCaveat != nil && rel.OptionalCaveat.CaveatName != "" {
							caveatExpr := caveats.CaveatAsExpr(rel.OptionalCaveat)
							runResult, err := refs.caveatRunner.RunCaveatExpression(ctx, caveatExpr, refs.req.Context.AsMap(), caveatSR, caveats.RunCaveatExpressionNoDebugging)
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
							if refs.req.OptionalLimit > 0 {
								crr.relationshipsChunkCache.storeRelationshipsChunk(rm)
							}

							// Yield the chunk of relationships for processing.
							if !yield(cter.Chunk[*relationshipsChunk, datastoreIndex]{
								CurrentChunk:       rm,
								CurrentChunkCursor: rm.mustAsIndex(),
							}, nil) {
								return
							}

							// Create a new relationships chunk to continue processing, with its cursor set to the
							// resume after the current relationship.
							rm = newRelationshipsChunk(int(crr.dispatchChunkSize), &rel)
						}
					}

					if rm.isPopulated() {
						if refs.req.OptionalLimit > 0 {
							crr.relationshipsChunkCache.storeRelationshipsChunk(rm)
						}

						// Yield any remaining relationships in the chunk.
						if !yield(cter.Chunk[*relationshipsChunk, datastoreIndex]{
							CurrentChunk:       rm,
							CurrentChunkCursor: rm.mustAsIndex(),
						}, nil) {
							return
						}
					}
				}

				return cter.Spanned(
					ctx,
					iter,
					tracer,
					otelconv.EventDispatchLookupResources3RelationshipsIterProducer,
					attribute.Int64(otelconv.AttrDispatchLRConcurrencyLimit, ccl),
				)
			},
			func(ctx context.Context, remainingCursor cter.Cursor, rm *relationshipsChunk) iter.Seq2[result, error] {
				// Return an iterator to continue finding resources by dispatching.
				// NOTE(jschorr): Technically if we don't have any further entrypoints at this point, we could do checks locally,
				// but that would require additional code just to save one dispatch hop, thus complicating the code base.
				iter := crr.checkedDispatchIter(ctx, refs, remainingCursor, rm, config.foundResourceType, config.entrypoint)
				return cter.Spanned(
					ctx,
					iter,
					tracer,
					otelconv.EventDispatchLookupResources3RelationshipsIterMapper,
				)
			},
		)
	})
}

func (crr *CursoredLookupResources3) checkedDispatchIter(
	ctx context.Context,
	refs lr3refs,
	currentCursor cter.Cursor,
	rm *relationshipsChunk,
	foundResourceType *core.RelationReference,
	entrypoint schema.ReachabilityEntrypoint,
) iter.Seq2[result, error] {
	// If the entrypoint is a direct result, we can simply dispatch the results, as it means no intersection,
	// exclusion or intersection arrow was found "above" this entrypoint in the permission.
	if entrypoint.IsDirectResult() {
		return crr.dispatchIter(ctx, refs, currentCursor, rm, foundResourceType)
	}

	// Otherwise, we need to check them before dispatching. This shears the tree of results for intersections, exclusions
	// and intersection arrows.
	filteredChunk, err := crr.filterSubjectsByCheck(ctx, refs, rm, foundResourceType, entrypoint)
	if err != nil {
		return cter.YieldsError[result](err)
	}

	return crr.dispatchIter(ctx, refs, currentCursor, filteredChunk, foundResourceType)
}

// dispatchIter is a helper function that dispatches the resources found in the relationships chunk,
// yielding results for each resource found. This performs the recursive LookupResources operation
// over the resources found in the `rm` chunk.
func (crr *CursoredLookupResources3) dispatchIter(
	ctx context.Context,
	refs lr3refs,
	currentCursor cter.Cursor,
	rm *relationshipsChunk,
	foundResourceType *core.RelationReference,
) iter.Seq2[result, error] {
	if !rm.isPopulated() {
		// If there are no relationships to dispatch, return an empty iterator.
		return cter.UncursoredEmpty[result]()
	}

	subjectIDs := rm.subjectIDsToDispatch()

	// Return an iterator that invokes the dispatch operation for the given resource relation and subject IDs,
	// yielding results for each resource found.
	iter := func(yield func(result, error) bool) {
		stream := newYieldingStream(ctx, yield, rm)
		err := crr.dl.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
			ResourceRelation: refs.req.ResourceRelation,
			SubjectRelation:  foundResourceType,
			SubjectIds:       subjectIDs,
			TerminalSubject:  refs.req.TerminalSubject,
			Metadata: &v1.ResolverMeta{
				AtRevision:     refs.req.Revision.String(),
				DepthRemaining: refs.req.Metadata.DepthRemaining - 1,
			},
			OptionalCursor: currentCursor,
			OptionalLimit:  refs.req.OptionalLimit,
			Context:        refs.req.Context,
		}, stream)
		if err != nil && !stream.canceled {
			_ = yield(result{}, err)
			return
		}
	}
	return cter.Spanned(
		ctx,
		iter,
		tracer,
		otelconv.EventDispatchLookupResources3DispatchIter,
	)
}

// filterSubjectsByCheck filters the subjects in the relationships chunk by checking them against the entrypoint.
// This handles intersections, exclusions and intersection arrows by using the DispatchCheck operation to shear
// the tree where applicable.
func (crr *CursoredLookupResources3) filterSubjectsByCheck(
	ctx context.Context,
	refs lr3refs,
	rm *relationshipsChunk,
	foundResourceType *core.RelationReference,
	entrypoint schema.ReachabilityEntrypoint,
) (*relationshipsChunk, error) {
	resourceIDsToCheck := rm.subjectIDsToDispatch()

	// Build a set of check hints for the resources to check, thus avoiding the work
	// LookupResources just performed to find these resources.
	checkHints := make([]*v1.CheckHint, 0, len(resourceIDsToCheck))
	for _, resourceIDToCheck := range resourceIDsToCheck {
		checkHint, err := hints.HintForEntrypoint(
			entrypoint,
			resourceIDToCheck,
			tuple.FromCoreObjectAndRelation(refs.req.TerminalSubject),
			&v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			})
		if err != nil {
			return nil, err
		}
		checkHints = append(checkHints, checkHint)
	}

	// NOTE: we are checking the containing permission here, *not* the target relation, as
	// the goal is to shear for the containing permission.
	resultsByResourceID, _, _, err := computed.ComputeBulkCheck(ctx, crr.dc, crr.caveatTypeSet, computed.CheckParameters{
		ResourceType:  tuple.FromCoreRelationReference(foundResourceType),
		Subject:       tuple.FromCoreObjectAndRelation(refs.req.TerminalSubject),
		CaveatContext: refs.req.Context.AsMap(),
		AtRevision:    refs.req.Revision,
		MaximumDepth:  refs.req.Metadata.DepthRemaining - 1,
		DebugOption:   computed.NoDebugging,
		CheckHints:    checkHints,
	}, resourceIDsToCheck, crr.dispatchChunkSize)
	if err != nil {
		return nil, err
	}

	// Dispatch any resources that are visible.
	updatedChunk := newRelationshipsChunk(len(resourceIDsToCheck), nil)
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
			return nil, spiceerrors.MustBugf("unexpected result from check: %v", result.Membership)
		}
	}

	return updatedChunk, nil
}

// newYieldingStream creates a new yielding stream for the LookupResources3 operation, which takes results
// published to the stream and yields them to the provided yield function. The stream will cancel
// itself if the yield function returns false, indicating that the stream is no longer interested in results.
func newYieldingStream(ctx context.Context, yield func(result, error) bool, rm *relationshipsChunk) *yieldingStream {
	ctx, cancel := context.WithCancel(ctx)
	return &yieldingStream{
		ctx:      ctx,
		cancel:   cancel,
		yield:    yield,
		rm:       rm,
		canceled: false,
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

	// canceled indicates whether the stream has been canceled, which will prevent further publishing of results.
	canceled bool
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

	// Map the possible resource from the response, which combines missing context parameters
	// from the existing possible resource with that published.
	for _, item := range resp.Items {
		mappedPossibleResource, err := y.rm.mapPossibleResource(possibleResource{
			resourceID:           item.ResourceId,
			forSubjectIDs:        item.ForSubjectIds,
			missingContextParams: item.MissingContextParams,
		})
		if err != nil {
			return err
		}

		if !y.yield(result{
			Item:   mappedPossibleResource,
			Cursor: item.AfterResponseCursorSections,
		}, nil) {
			y.canceled = true
			y.cancel()
			return context.Canceled
		}
	}

	return nil
}

var _ dispatch.LookupResources3Stream = &yieldingStream{}

// relationshipsChunk is a chunk of relationships, produced by the relationships iterator,
// that can be used to dispatch further resources. It contains the relationships
// and the resources that are found in the relationships, along with any missing context parameters
// that were found during the relationship processing.
type relationshipsChunk struct {
	uniqueID             string
	dbCursor             *tuple.Relationship
	finalRelationship    tuple.Relationship
	subjectsByResourceID map[string]map[string]subjectIDAndMissingContext
}

type subjectIDAndMissingContext struct {
	subjectID            string
	missingContextParams *mapz.Set[string]
}

// newRelationshipsChunk creates a new relationships chunk with the specified capacity.
func newRelationshipsChunk(capacity int, dbCursor options.Cursor) *relationshipsChunk {
	return &relationshipsChunk{
		uniqueID:             uuid.NewString(),
		dbCursor:             dbCursor,
		subjectsByResourceID: make(map[string]map[string]subjectIDAndMissingContext, capacity),
	}
}

// isPopulated returns true if the relationships chunk contains any relationships.
func (rm *relationshipsChunk) isPopulated() bool {
	return len(rm.subjectsByResourceID) > 0
}

// estimatedSize returns an estimate of the size of the relationships chunk. Used for the cost
// portion of the cache.
func (rm *relationshipsChunk) estimatedSize() int {
	estimatedCost := 100 // estimated base cost
	for resourceID, subjects := range rm.subjectsByResourceID {
		estimatedCost += len(resourceID)
		for subjectID, info := range subjects {
			estimatedCost += len(subjectID)
			estimatedCost += info.missingContextParams.Len() * 10
		}
	}
	return estimatedCost
}

// addRelationship adds a relationship to the relationships chunk, creating the necessary
// resource and subject entries if they do not exist. It also collects any missing context parameters
// that were found during the relationship processing.
func (rm *relationshipsChunk) addRelationship(rel tuple.Relationship, missingContextParameters []string) int {
	resourceRef, ok := rm.subjectsByResourceID[rel.Resource.ObjectID]
	if !ok {
		rm.subjectsByResourceID[rel.Resource.ObjectID] = make(map[string]subjectIDAndMissingContext, 1)
		resourceRef = rm.subjectsByResourceID[rel.Resource.ObjectID]
	}

	subjectRef, ok := resourceRef[rel.Subject.ObjectID]
	if !ok {
		subjectRef = subjectIDAndMissingContext{
			subjectID:            rel.Subject.ObjectID,
			missingContextParams: mapz.NewSet[string](),
		}
		resourceRef[rel.Subject.ObjectID] = subjectRef
	}

	subjectRef.missingContextParams.Extend(missingContextParameters)
	rm.finalRelationship = rel
	return len(rm.subjectsByResourceID)
}

// copyRelationshipsFrom copies relationships from another relationships chunk for a specific resource ID,
// adding them to the current relationships chunk. It also updates the missing context parameters for the
// resource if they are not already present. This is used for aggregating relationships from a source chunk
// after it has been checked.
func (rm *relationshipsChunk) copyRelationshipsFrom(src *relationshipsChunk, resourceID string, missingContextParameters []string) {
	if src == nil || !src.isPopulated() {
		return
	}

	srcResourceInfo, ok := src.subjectsByResourceID[resourceID]
	if !ok {
		return
	}

	destResourceInfo, ok := rm.subjectsByResourceID[resourceID]
	if !ok {
		destResourceInfo = make(map[string]subjectIDAndMissingContext, len(srcResourceInfo))
		rm.subjectsByResourceID[resourceID] = destResourceInfo
	}

	for subjectID, srcSubjectInfo := range srcResourceInfo {
		destSubjectInfo, ok := destResourceInfo[subjectID]
		if !ok {
			destSubjectInfo = subjectIDAndMissingContext{
				subjectID:            subjectID,
				missingContextParams: mapz.NewSet[string](),
			}
		}

		destSubjectInfo.missingContextParams.Merge(srcSubjectInfo.missingContextParams)
		if len(missingContextParameters) > 0 {
			destSubjectInfo.missingContextParams.Extend(missingContextParameters)
		}

		// Update the map with the modified destSubjectInfo
		destResourceInfo[subjectID] = destSubjectInfo
	}
}

func (rm *relationshipsChunk) mustAsIndex() datastoreIndex {
	spiceerrors.DebugAssertf(rm.isPopulated, "cannot create index from empty relationships chunk")
	return datastoreIndex{
		dbCursor: rm.dbCursor,
		chunkID:  rm.uniqueID,
	}
}

// subjectIDsToDispatch returns the subject IDs that should be dispatched from the relationships chunk.
func (rm *relationshipsChunk) subjectIDsToDispatch() []string {
	// NOTE: the method is called subjectIDsToDispatch, but we are returning the resource IDs
	// here. This is because once we've collected resource IDs, they become the subject IDs for
	// the *next* dispatch operation.
	resourceIDs := make([]string, 0, len(rm.subjectsByResourceID))
	for resourceID := range rm.subjectsByResourceID {
		resourceIDs = append(resourceIDs, resourceID)
	}
	sort.Strings(resourceIDs)
	return resourceIDs
}

// mapPossibleResource maps a possible resource from the relationships chunk, collecting the subject IDs
// and missing context parameters from the relationships chunk. It returns a new possible resource
// that contains the mapped subject IDs and missing context parameters, or an error if the mapping fails.
func (rm *relationshipsChunk) mapPossibleResource(foundResource possibleResource) (possibleResource, error) {
	forSubjectIDs := mapz.NewSet[string]()
	nonCaveatedSubjectIDs := mapz.NewSet[string]()

	missingContextParameters := mapz.NewSet[string]()
	missingContextParameters.Extend(foundResource.missingContextParams)

	for _, forSubjectID := range foundResource.forSubjectIDs {
		// Map from the incoming subject ID to the subject ID(s) that caused the dispatch.
		infos, ok := rm.subjectsByResourceID[forSubjectID]
		if !ok {
			return possibleResource{}, spiceerrors.MustBugf("missing for subject ID")
		}

		for _, info := range infos {
			forSubjectIDs.Insert(info.subjectID)

			if info.missingContextParams.IsEmpty() {
				nonCaveatedSubjectIDs.Insert(info.subjectID)
			} else {
				missingContextParameters.Merge(info.missingContextParams)
			}
		}
	}

	// If there are some non-caveated IDs, return those and mark as the parent status.
	if len(foundResource.missingContextParams) == 0 && nonCaveatedSubjectIDs.Len() > 0 {
		return possibleResource{
			resourceID:    foundResource.resourceID,
			forSubjectIDs: nonCaveatedSubjectIDs.AsSlice(),
		}, nil
	}

	// Otherwise, everything is caveated, so return the full set of subject IDs and mark
	// as a check is required.
	return possibleResource{
		resourceID:           foundResource.resourceID,
		forSubjectIDs:        forSubjectIDs.AsSlice(),
		missingContextParams: missingContextParameters.AsSlice(),
	}, nil
}

// subjectIDsToRelationshipsChunk creates a relationships chunk from the given subject IDs and the rewritten subject relation.
func subjectIDsToRelationshipsChunk(
	subjectRelation *core.RelationReference,
	subjectIDs []string,
	rewrittenSubjectRelation *core.RelationReference,
) *relationshipsChunk {
	rm := newRelationshipsChunk(len(subjectIDs), nil)
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

type relationshipsChunkCache struct {
	cache cache.Cache[cache.StringKey, any]
}

func (crc *relationshipsChunkCache) Close() {
	if crc.cache != nil {
		crc.cache.Close()
	}
}

func (crc *relationshipsChunkCache) storeRelationshipsChunk(rm *relationshipsChunk) {
	if crc.cache == nil {
		return // caching is disabled
	}
	crc.cache.Set(cache.StringKey(rm.uniqueID), rm, int64(rm.estimatedSize()))
}

func (crc *relationshipsChunkCache) lookupChunkByID(chunkID string) (*relationshipsChunk, bool) {
	if crc.cache == nil {
		return nil, false // caching is disabled
	}

	value, found := crc.cache.Get(cache.StringKey(chunkID))
	if !found {
		return nil, false
	}

	chunk, ok := value.(*relationshipsChunk)
	if !ok {
		return nil, false
	}

	return chunk, true
}

const dsIndexPrefix = "$dsi:"

// containsDatastoreIndex checks if the given cursor contains a datastore index section.
func containsDatastoreIndex(cursor cter.Cursor) bool {
	for _, section := range cursor {
		if strings.HasPrefix(section, dsIndexPrefix) {
			return true
		}
	}
	return false
}

// datastoreIndexFromString converts a string representation of a datastore cursor
// into a datastoreIndex. If the string is empty, it returns nil.
func datastoreIndexFromString(cursorStr string) (datastoreIndex, error) {
	if cursorStr == "" {
		return datastoreIndex{}, nil
	}

	if !strings.HasPrefix(cursorStr, dsIndexPrefix) {
		return datastoreIndex{}, spiceerrors.MustBugf("invalid datastore index cursor: missing prefix")
	}

	withoutPrefix := strings.TrimPrefix(cursorStr, dsIndexPrefix)
	if withoutPrefix == "" {
		return datastoreIndex{}, spiceerrors.MustBugf("invalid datastore index cursor: missing encoded part")
	}

	pieces := strings.SplitN(withoutPrefix, ":", 2)
	if len(pieces) > 2 || len(pieces) < 1 {
		return datastoreIndex{}, spiceerrors.MustBugf("invalid datastore index cursor: missing parts")
	}

	chunkID := pieces[0]
	if chunkID == "" {
		return datastoreIndex{}, spiceerrors.MustBugf("invalid datastore index cursor: missing chunk ID")
	}

	if len(pieces) == 1 {
		// No relationship part, so this is just the chunk ID with no cursor.
		return datastoreIndex{
			chunkID: chunkID,
		}, nil
	}

	relString := pieces[1]
	cursorRel, err := tuple.Parse(relString)
	if err != nil {
		return datastoreIndex{}, spiceerrors.MustBugf("invalid datastore index cursor: malformed relationship")
	}

	return datastoreIndex{
		chunkID:  chunkID,
		dbCursor: &cursorRel,
	}, nil
}

// mustDatastoreIndexToString converts a datastoreIndex into a string representation.
func mustDatastoreIndexToString(index datastoreIndex) (string, error) {
	spiceerrors.DebugAssertf(func() bool { return index.chunkID != "" }, "chunk ID must be set")

	if index.dbCursor == nil {
		return dsIndexPrefix + index.chunkID, nil
	}

	return dsIndexPrefix + index.chunkID + ":" + index.dbCursor.RelationshipReference.String(), nil
}

type datastoreIndex struct {
	dbCursor options.Cursor
	chunkID  string
}

func (di datastoreIndex) toDatastoreCursor() options.Cursor {
	return di.dbCursor
}

func (di datastoreIndex) lookupRelationshipsChunk(crc *relationshipsChunkCache) (*relationshipsChunk, bool) {
	if di.chunkID == "" {
		return nil, false
	}

	return crc.lookupChunkByID(di.chunkID)
}
