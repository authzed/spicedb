package graph

import (
	"context"
	"slices"
	"sort"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/internal/graph/hints"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// dispatchVersion defines the "version" of this dispatcher. Must be incremented
// anytime an incompatible change is made to the dispatcher itself or its cursor
// production.
const dispatchVersion = 1

func NewCursoredLookupResources2(dl dispatch.LookupResources2, dc dispatch.Check, concurrencyLimit uint16, dispatchChunkSize uint16) *CursoredLookupResources2 {
	return &CursoredLookupResources2{dl, dc, concurrencyLimit, dispatchChunkSize}
}

type CursoredLookupResources2 struct {
	dl                dispatch.LookupResources2
	dc                dispatch.Check
	concurrencyLimit  uint16
	dispatchChunkSize uint16
}

type ValidatedLookupResources2Request struct {
	*v1.DispatchLookupResources2Request
	Revision datastore.Revision
}

func (crr *CursoredLookupResources2) LookupResources2(
	req ValidatedLookupResources2Request,
	stream dispatch.LookupResources2Stream,
) error {
	ctx, span := tracer.Start(stream.Context(), "lookupResources2")
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

	limits := newLimitTracker(req.OptionalLimit)
	ci, err := newCursorInformation(req.OptionalCursor, limits, dispatchVersion)
	if err != nil {
		return err
	}

	return withSubsetInCursor(ci,
		func(currentOffset int, nextCursorWith afterResponseCursor) error {
			// If the resource type matches the subject type, yield directly as a one-to-one result
			// for each subjectID.
			if req.SubjectRelation.Namespace == req.ResourceRelation.Namespace &&
				req.SubjectRelation.Relation == req.ResourceRelation.Relation {
				for index, subjectID := range req.SubjectIds {
					if index < currentOffset {
						continue
					}

					if !ci.limits.prepareForPublishing() {
						return nil
					}

					err := stream.Publish(&v1.DispatchLookupResources2Response{
						Resource: &v1.PossibleResource{
							ResourceId:    subjectID,
							ForSubjectIds: []string{subjectID},
						},
						Metadata:            emptyMetadata,
						AfterResponseCursor: nextCursorWith(index + 1),
					})
					if err != nil {
						return err
					}
				}
			}
			return nil
		}, func(ci cursorInformation) error {
			// Once done checking for the matching subject type, yield by dispatching over entrypoints.
			return crr.afterSameType(ctx, ci, req, stream)
		})
}

func (crr *CursoredLookupResources2) afterSameType(
	ctx context.Context,
	ci cursorInformation,
	req ValidatedLookupResources2Request,
	parentStream dispatch.LookupResources2Stream,
) error {
	reachabilityForString := req.ResourceRelation.Namespace + "#" + req.ResourceRelation.Relation
	ctx, span := tracer.Start(ctx, "reachability: "+reachabilityForString)
	defer span.End()

	dispatched := NewSyncONRSet()

	// Load the type system and reachability graph to find the entrypoints for the reachability.
	ds := datastoremw.MustFromContext(ctx)
	reader := ds.SnapshotReader(req.Revision)
	ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(reader))
	vdef, err := ts.GetValidatedDefinition(ctx, req.ResourceRelation.Namespace)
	if err != nil {
		return err
	}

	rg := vdef.Reachability()
	entrypoints, err := rg.FirstEntrypointsForSubjectToResource(ctx, &core.RelationReference{
		Namespace: req.SubjectRelation.Namespace,
		Relation:  req.SubjectRelation.Relation,
	}, req.ResourceRelation)
	if err != nil {
		return err
	}

	// For each entrypoint, load the necessary data and re-dispatch if a subproblem was found.
	return withParallelizedStreamingIterableInCursor(ctx, ci, entrypoints, parentStream, crr.concurrencyLimit,
		func(ctx context.Context, ci cursorInformation, entrypoint schema.ReachabilityEntrypoint, stream dispatch.LookupResources2Stream) error {
			ds, err := entrypoint.DebugString()
			spiceerrors.DebugAssert(func() bool {
				return err == nil
			}, "Error in entrypoint.DebugString()")
			ctx, span := tracer.Start(ctx, "entrypoint: "+ds, trace.WithAttributes())
			defer span.End()

			switch entrypoint.EntrypointKind() {
			case core.ReachabilityEntrypoint_RELATION_ENTRYPOINT:
				return crr.lookupRelationEntrypoint(ctx, ci, entrypoint, rg, ts, reader, req, stream, dispatched)

			case core.ReachabilityEntrypoint_COMPUTED_USERSET_ENTRYPOINT:
				containingRelation := entrypoint.ContainingRelationOrPermission()
				rewrittenSubjectRelation := &core.RelationReference{
					Namespace: containingRelation.Namespace,
					Relation:  containingRelation.Relation,
				}

				rsm := subjectIDsToResourcesMap2(rewrittenSubjectRelation, req.SubjectIds)
				drsm := rsm.asReadOnly()

				return crr.redispatchOrReport(
					ctx,
					ci,
					rewrittenSubjectRelation,
					drsm,
					rg,
					entrypoint,
					stream,
					req,
					dispatched,
				)

			case core.ReachabilityEntrypoint_TUPLESET_TO_USERSET_ENTRYPOINT:
				return crr.lookupTTUEntrypoint(ctx, ci, entrypoint, rg, ts, reader, req, stream, dispatched)

			default:
				return spiceerrors.MustBugf("Unknown kind of entrypoint: %v", entrypoint.EntrypointKind())
			}
		})
}

func (crr *CursoredLookupResources2) lookupRelationEntrypoint(
	ctx context.Context,
	ci cursorInformation,
	entrypoint schema.ReachabilityEntrypoint,
	rg *schema.DefinitionReachability,
	ts *schema.TypeSystem,
	reader datastore.Reader,
	req ValidatedLookupResources2Request,
	stream dispatch.LookupResources2Stream,
	dispatched *syncONRSet,
) error {
	relationReference, err := entrypoint.DirectRelation()
	if err != nil {
		return err
	}

	relDefinition, err := ts.GetValidatedDefinition(ctx, relationReference.Namespace)
	if err != nil {
		return err
	}

	// Build the list of subjects to lookup based on the type information available.
	isDirectAllowed, err := relDefinition.IsAllowedDirectRelation(
		relationReference.Relation,
		req.SubjectRelation.Namespace,
		req.SubjectRelation.Relation,
	)
	if err != nil {
		return err
	}

	subjectIds := make([]string, 0, len(req.SubjectIds)+1)
	if isDirectAllowed == schema.DirectRelationValid {
		subjectIds = append(subjectIds, req.SubjectIds...)
	}

	if req.SubjectRelation.Relation == tuple.Ellipsis {
		isWildcardAllowed, err := relDefinition.IsAllowedPublicNamespace(relationReference.Relation, req.SubjectRelation.Namespace)
		if err != nil {
			return err
		}

		if isWildcardAllowed == schema.PublicSubjectAllowed {
			subjectIds = append(subjectIds, "*")
		}
	}

	// Lookup the subjects and then redispatch/report results.
	relationFilter := datastore.SubjectRelationFilter{
		NonEllipsisRelation: req.SubjectRelation.Relation,
	}

	if req.SubjectRelation.Relation == tuple.Ellipsis {
		relationFilter = datastore.SubjectRelationFilter{
			IncludeEllipsisRelation: true,
		}
	}

	subjectsFilter := datastore.SubjectsFilter{
		SubjectType:        req.SubjectRelation.Namespace,
		OptionalSubjectIds: subjectIds,
		RelationFilter:     relationFilter,
	}

	return crr.redispatchOrReportOverDatabaseQuery(
		ctx,
		redispatchOverDatabaseConfig2{
			ci:                 ci,
			ts:                 ts,
			reader:             reader,
			subjectsFilter:     subjectsFilter,
			sourceResourceType: relationReference,
			foundResourceType:  relationReference,
			entrypoint:         entrypoint,
			rg:                 rg,
			concurrencyLimit:   crr.concurrencyLimit,
			parentStream:       stream,
			parentRequest:      req,
			dispatched:         dispatched,
		},
	)
}

type redispatchOverDatabaseConfig2 struct {
	ci cursorInformation

	ts *schema.TypeSystem

	// Direct reader for reverse ReverseQueryRelationships
	reader datastore.Reader

	subjectsFilter     datastore.SubjectsFilter
	sourceResourceType *core.RelationReference
	foundResourceType  *core.RelationReference

	entrypoint schema.ReachabilityEntrypoint
	rg         *schema.DefinitionReachability

	concurrencyLimit uint16
	parentStream     dispatch.LookupResources2Stream
	parentRequest    ValidatedLookupResources2Request
	dispatched       *syncONRSet
}

func (crr *CursoredLookupResources2) redispatchOrReportOverDatabaseQuery(
	ctx context.Context,
	config redispatchOverDatabaseConfig2,
) error {
	ctx, span := tracer.Start(ctx, "datastorequery", trace.WithAttributes(
		attribute.String("source-resource-type-namespace", config.sourceResourceType.Namespace),
		attribute.String("source-resource-type-relation", config.sourceResourceType.Relation),
		attribute.String("subjects-filter-subject-type", config.subjectsFilter.SubjectType),
		attribute.Int("subjects-filter-subject-ids-count", len(config.subjectsFilter.OptionalSubjectIds)),
	))
	defer span.End()

	return withDatastoreCursorInCursor(ctx, config.ci, config.parentStream, config.concurrencyLimit,
		// Find the target resources for the subject.
		func(queryCursor options.Cursor) ([]itemAndPostCursor[dispatchableResourcesSubjectMap2], error) {
			it, err := config.reader.ReverseQueryRelationships(
				ctx,
				config.subjectsFilter,
				options.WithResRelation(&options.ResourceRelation{
					Namespace: config.sourceResourceType.Namespace,
					Relation:  config.sourceResourceType.Relation,
				}),
				options.WithSortForReverse(options.BySubject),
				options.WithAfterForReverse(queryCursor),
				options.WithIncludeObjectDataForReverse(config.parentRequest.IncludeObjectData),
			)
			if err != nil {
				return nil, err
			}

			// Chunk based on the FilterMaximumIDCount, to ensure we never send more than that amount of
			// results to a downstream dispatch.
			rsm := newResourcesSubjectMap2WithCapacity(config.sourceResourceType, uint32(crr.dispatchChunkSize))
			toBeHandled := make([]itemAndPostCursor[dispatchableResourcesSubjectMap2], 0)
			currentCursor := queryCursor
			caveatRunner := caveats.NewCaveatRunner()

			for rel, err := range it {
				if err != nil {
					return nil, err
				}

				var missingContextParameters []string

				// If a caveat exists on the relationship, run it and filter the results, marking those that have missing context.
				if rel.OptionalCaveat != nil && rel.OptionalCaveat.CaveatName != "" {
					caveatExpr := caveats.CaveatAsExpr(rel.OptionalCaveat)
					runResult, err := caveatRunner.RunCaveatExpression(ctx, caveatExpr, config.parentRequest.Context.AsMap(), config.reader, caveats.RunCaveatExpressionNoDebugging)
					if err != nil {
						return nil, err
					}

					// If a partial result is returned, collect the missing context parameters.
					if runResult.IsPartial() {
						missingNames, err := runResult.MissingVarNames()
						if err != nil {
							return nil, err
						}

						missingContextParameters = missingNames
					} else if !runResult.Value() {
						// If the run result shows the caveat does not apply, skip. This shears the tree of results early.
						continue
					}
				}

				if err := rsm.addRelationship(rel, missingContextParameters); err != nil {
					return nil, err
				}

				if rsm.len() == int(crr.dispatchChunkSize) {
					toBeHandled = append(toBeHandled, itemAndPostCursor[dispatchableResourcesSubjectMap2]{
						item:   rsm.asReadOnly(),
						cursor: currentCursor,
					})
					rsm = newResourcesSubjectMap2WithCapacity(config.sourceResourceType, uint32(crr.dispatchChunkSize))
					currentCursor = options.ToCursor(rel)
				}
			}

			if rsm.len() > 0 {
				toBeHandled = append(toBeHandled, itemAndPostCursor[dispatchableResourcesSubjectMap2]{
					item:   rsm.asReadOnly(),
					cursor: currentCursor,
				})
			}

			return toBeHandled, nil
		},

		// Redispatch or report the results.
		func(
			ctx context.Context,
			ci cursorInformation,
			drsm dispatchableResourcesSubjectMap2,
			currentStream dispatch.LookupResources2Stream,
		) error {
			return crr.redispatchOrReport(
				ctx,
				ci,
				config.foundResourceType,
				drsm,
				config.rg,
				config.entrypoint,
				currentStream,
				config.parentRequest,
				config.dispatched,
			)
		},
	)
}

func (crr *CursoredLookupResources2) lookupTTUEntrypoint(ctx context.Context,
	ci cursorInformation,
	entrypoint schema.ReachabilityEntrypoint,
	rg *schema.DefinitionReachability,
	ts *schema.TypeSystem,
	reader datastore.Reader,
	req ValidatedLookupResources2Request,
	stream dispatch.LookupResources2Stream,
	dispatched *syncONRSet,
) error {
	containingRelation := entrypoint.ContainingRelationOrPermission()

	ttuDef, err := ts.GetValidatedDefinition(ctx, containingRelation.Namespace)
	if err != nil {
		return err
	}

	tuplesetRelation, err := entrypoint.TuplesetRelation()
	if err != nil {
		return err
	}

	// Determine whether this TTU should be followed, which will be the case if the subject relation's namespace
	// is allowed in any form on the relation; since arrows ignore the subject's relation (if any), we check
	// for the subject namespace as a whole.
	allowedRelations, err := ttuDef.GetAllowedDirectNamespaceSubjectRelations(tuplesetRelation, req.SubjectRelation.Namespace)
	if err != nil {
		return err
	}

	if allowedRelations == nil {
		return nil
	}

	// Search for the resolved subjects in the tupleset of the TTU.
	subjectsFilter := datastore.SubjectsFilter{
		SubjectType:        req.SubjectRelation.Namespace,
		OptionalSubjectIds: req.SubjectIds,
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

	return crr.redispatchOrReportOverDatabaseQuery(
		ctx,
		redispatchOverDatabaseConfig2{
			ci:                 ci,
			ts:                 ts,
			reader:             reader,
			subjectsFilter:     subjectsFilter,
			sourceResourceType: tuplesetRelationReference,
			foundResourceType:  containingRelation,
			entrypoint:         entrypoint,
			rg:                 rg,
			parentStream:       stream,
			parentRequest:      req,
			dispatched:         dispatched,
		},
	)
}

type possibleResourceAndIndex struct {
	resource *v1.PossibleResource
	index    int
}

// redispatchOrReport checks if further redispatching is necessary for the found resource
// type. If not, and the found resource type+relation matches the target resource type+relation,
// the resource is reported to the parent stream.
func (crr *CursoredLookupResources2) redispatchOrReport(
	ctx context.Context,
	ci cursorInformation,
	foundResourceType *core.RelationReference,
	foundResources dispatchableResourcesSubjectMap2,
	rg *schema.DefinitionReachability,
	entrypoint schema.ReachabilityEntrypoint,
	parentStream dispatch.LookupResources2Stream,
	parentRequest ValidatedLookupResources2Request,
	dispatched *syncONRSet,
) error {
	if foundResources.isEmpty() {
		// Nothing more to do.
		return nil
	}

	ctx, span := tracer.Start(ctx, "redispatchOrReport", trace.WithAttributes(
		attribute.Int("found-resources-count", foundResources.len()),
	))
	defer span.End()

	// Check for entrypoints for the new found resource type.
	hasResourceEntrypoints, err := rg.HasOptimizedEntrypointsForSubjectToResource(ctx, foundResourceType, parentRequest.ResourceRelation)
	if err != nil {
		return err
	}

	return withSubsetInCursor(ci,
		func(currentOffset int, nextCursorWith afterResponseCursor) error {
			if !hasResourceEntrypoints {
				// If the found resource matches the target resource type and relation, potentially yield the resource.
				if foundResourceType.Namespace == parentRequest.ResourceRelation.Namespace && foundResourceType.Relation == parentRequest.ResourceRelation.Relation {
					resources := foundResources.asPossibleResources()
					if len(resources) == 0 {
						return nil
					}

					if currentOffset >= len(resources) {
						return nil
					}

					offsetted := resources[currentOffset:]
					if len(offsetted) == 0 {
						return nil
					}

					filtered := make([]possibleResourceAndIndex, 0, len(offsetted))
					for index, resource := range offsetted {
						filtered = append(filtered, possibleResourceAndIndex{
							resource: resource,
							index:    index,
						})
					}

					metadata := emptyMetadata

					// If the entrypoint is not a direct result, issue a check to further filter the results on the intersection or exclusion.
					if !entrypoint.IsDirectResult() {
						resourceIDs := make([]string, 0, len(offsetted))
						checkHints := make([]*v1.CheckHint, 0, len(offsetted))
						for _, resource := range offsetted {
							resourceIDs = append(resourceIDs, resource.ResourceId)

							checkHint, err := hints.HintForEntrypoint(
								entrypoint,
								resource.ResourceId,
								tuple.FromCoreObjectAndRelation(parentRequest.TerminalSubject),
								&v1.ResourceCheckResult{
									Membership: v1.ResourceCheckResult_MEMBER,
								})
							if err != nil {
								return err
							}
							checkHints = append(checkHints, checkHint)
						}

						resultsByResourceID, checkMetadata, _, err := computed.ComputeBulkCheck(ctx, crr.dc, computed.CheckParameters{
							ResourceType:  tuple.FromCoreRelationReference(parentRequest.ResourceRelation),
							Subject:       tuple.FromCoreObjectAndRelation(parentRequest.TerminalSubject),
							CaveatContext: parentRequest.Context.AsMap(),
							AtRevision:    parentRequest.Revision,
							MaximumDepth:  parentRequest.Metadata.DepthRemaining - 1,
							DebugOption:   computed.NoDebugging,
							CheckHints:    checkHints,
						}, resourceIDs, crr.dispatchChunkSize)
						if err != nil {
							return err
						}

						metadata = addCallToResponseMetadata(checkMetadata)

						filtered = make([]possibleResourceAndIndex, 0, len(offsetted))
						for index, resource := range offsetted {
							resource.ResourceData = foundResources.GetObjectData(resource.ResourceId)
							result, ok := resultsByResourceID[resource.ResourceId]
							if !ok {
								continue
							}

							switch result.Membership {
							case v1.ResourceCheckResult_MEMBER:
								filtered = append(filtered, possibleResourceAndIndex{
									resource: resource,
									index:    index,
								})

							case v1.ResourceCheckResult_CAVEATED_MEMBER:
								missingContextParams := mapz.NewSet(result.MissingExprFields...)
								missingContextParams.Extend(resource.MissingContextParams)

								filtered = append(filtered, possibleResourceAndIndex{
									resource: &v1.PossibleResource{
										ResourceId:           resource.ResourceId,
										ResourceData:         resource.ResourceData,
										ForSubjectIds:        resource.ForSubjectIds,
										MissingContextParams: missingContextParams.AsSlice(),
									},
									index: index,
								})

							case v1.ResourceCheckResult_NOT_MEMBER:
								// Skip.

							default:
								return spiceerrors.MustBugf("unexpected result from check: %v", result.Membership)
							}
						}
					}

					for _, resourceAndIndex := range filtered {
						if !ci.limits.prepareForPublishing() {
							return nil
						}

						err := parentStream.Publish(&v1.DispatchLookupResources2Response{
							Resource:            resourceAndIndex.resource,
							Metadata:            metadata,
							AfterResponseCursor: nextCursorWith(currentOffset + resourceAndIndex.index + 1),
						})
						if err != nil {
							return err
						}

						metadata = emptyMetadata
					}
					return nil
				}
			}
			return nil
		}, func(ci cursorInformation) error {
			if !hasResourceEntrypoints {
				return nil
			}

			// The new subject type for dispatching was the found type of the *resource*.
			newSubjectType := foundResourceType

			// To avoid duplicate work, remove any subjects already dispatched.
			filteredSubjectIDs := foundResources.filterSubjectIDsToDispatch(dispatched, newSubjectType)
			if len(filteredSubjectIDs) == 0 {
				return nil
			}

			// If the entrypoint is a direct result then we can simply dispatch directly and map
			// all found results, as no further filtering will be needed.
			if entrypoint.IsDirectResult() {
				stream := unfilteredLookupResourcesDispatchStreamForEntrypoint(ctx, foundResources, parentStream, ci)
				return crr.dl.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
					ResourceRelation: parentRequest.ResourceRelation,
					SubjectRelation:  newSubjectType,
					SubjectIds:       filteredSubjectIDs,
					TerminalSubject:  parentRequest.TerminalSubject,
					Metadata: &v1.ResolverMeta{
						AtRevision:     parentRequest.Revision.String(),
						DepthRemaining: parentRequest.Metadata.DepthRemaining - 1,
					},
					OptionalCursor:    ci.currentCursor,
					OptionalLimit:     parentRequest.OptionalLimit,
					Context:           parentRequest.Context,
					IncludeObjectData: parentRequest.IncludeObjectData,
				}, stream)
			}

			// Otherwise, we need to filter results by batch checking along the way before dispatching.
			return runCheckerAndDispatch(
				ctx,
				parentRequest,
				foundResources,
				ci,
				parentStream,
				newSubjectType,
				filteredSubjectIDs,
				entrypoint,
				crr.dl,
				crr.dc,
				crr.concurrencyLimit,
				crr.dispatchChunkSize,
			)
		})
}
