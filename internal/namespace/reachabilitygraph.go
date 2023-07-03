package namespace

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/exp/maps"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ReachabilityGraph is a helper struct that provides an easy way to determine all entrypoints
// for a subject of a particular type into a schema, for the purpose of walking from the subject
// to a specific resource relation.
type ReachabilityGraph struct {
	ts                          *TypeSystem
	cachedGraphs                sync.Map
	hasOptimizedEntrypointCache sync.Map
}

// ReachabilityEntrypoint is an entrypoint into the reachability graph for a subject of particular
// type.
type ReachabilityEntrypoint struct {
	re             *core.ReachabilityEntrypoint
	parentRelation *core.RelationReference
}

// Hash returns a hash representing the data in the entrypoint, for comparison to other entrypoints.
// This is ONLY stable within a single version of SpiceDB and should NEVER be stored for later
// comparison outside of the process.
func (re ReachabilityEntrypoint) Hash() (uint64, error) {
	size := re.re.SizeVT()
	if re.parentRelation != nil {
		size += re.parentRelation.SizeVT()
	}

	hashData := make([]byte, 0, size)

	data, err := re.re.MarshalVT()
	if err != nil {
		return 0, err
	}

	hashData = append(hashData, data...)

	if re.parentRelation != nil {
		data, err := re.parentRelation.MarshalVT()
		if err != nil {
			return 0, err
		}

		hashData = append(hashData, data...)
	}

	return xxhash.Sum64(hashData), nil
}

// EntrypointKind is the kind of the entrypoint.
func (re ReachabilityEntrypoint) EntrypointKind() core.ReachabilityEntrypoint_ReachabilityEntrypointKind {
	return re.re.Kind
}

// TuplesetRelation returns the tupleset relation of the TTU, if a TUPLESET_TO_USERSET_ENTRYPOINT.
func (re ReachabilityEntrypoint) TuplesetRelation() (string, error) {
	if re.EntrypointKind() != core.ReachabilityEntrypoint_TUPLESET_TO_USERSET_ENTRYPOINT {
		return "", fmt.Errorf("cannot call TupleToUserset for kind %v", re.EntrypointKind())
	}

	return re.re.TuplesetRelation, nil
}

// DirectRelation is the relation that this entrypoint represents, if a RELATION_ENTRYPOINT.
func (re ReachabilityEntrypoint) DirectRelation() (*core.RelationReference, error) {
	if re.EntrypointKind() != core.ReachabilityEntrypoint_RELATION_ENTRYPOINT {
		return nil, fmt.Errorf("cannot call DirectRelation for kind %v", re.EntrypointKind())
	}

	return re.re.TargetRelation, nil
}

// ContainingRelationOrPermission is the relation or permission containing this entrypoint.
func (re ReachabilityEntrypoint) ContainingRelationOrPermission() *core.RelationReference {
	return re.parentRelation
}

// IsDirectResult returns whether the entrypoint, when evaluated, becomes a direct result of
// the parent relation/permission. A direct result only exists if the entrypoint is not contained
// under an intersection or exclusion, which makes the entrypoint's object merely conditionally
// reachable.
func (re ReachabilityEntrypoint) IsDirectResult() bool {
	return re.re.ResultStatus == core.ReachabilityEntrypoint_DIRECT_OPERATION_RESULT
}

func (re ReachabilityEntrypoint) String() string {
	return re.MustDebugString()
}

func (re ReachabilityEntrypoint) MustDebugString() string {
	switch re.EntrypointKind() {
	case core.ReachabilityEntrypoint_RELATION_ENTRYPOINT:
		return fmt.Sprintf("relation-entrypoint: %s#%s", re.re.TargetRelation.Namespace, re.re.TargetRelation.Relation)

	case core.ReachabilityEntrypoint_TUPLESET_TO_USERSET_ENTRYPOINT:
		return fmt.Sprintf("ttu-entrypoint: %s#%s | %s | %s#%s", re.parentRelation.Namespace, re.parentRelation.Relation, re.re.TuplesetRelation, re.re.TargetRelation.Namespace, re.re.TargetRelation.Relation)

	case core.ReachabilityEntrypoint_COMPUTED_USERSET_ENTRYPOINT:
		return fmt.Sprintf("computed-entrypoint: %s#%s", re.re.TargetRelation.Namespace, re.re.TargetRelation.Relation)

	default:
		panic("unknown relation entrypoint kind")
	}
}

// ReachabilityGraphFor returns a reachability graph for the given namespace.
func ReachabilityGraphFor(ts *ValidatedNamespaceTypeSystem) *ReachabilityGraph {
	return &ReachabilityGraph{ts.TypeSystem, sync.Map{}, sync.Map{}}
}

// AllEntrypointsForSubjectToResource returns the entrypoints into the reachability graph, starting
// at the given subject type and walking to the given resource type.
func (rg *ReachabilityGraph) AllEntrypointsForSubjectToResource(
	ctx context.Context,
	subjectType *core.RelationReference,
	resourceType *core.RelationReference,
) ([]ReachabilityEntrypoint, error) {
	return rg.entrypointsForSubjectToResource(ctx, subjectType, resourceType, reachabilityFull, entrypointLookupFindAll)
}

// OptimizedEntrypointsForSubjectToResource returns the *optimized* set of entrypoints into the
// reachability graph, starting at the given subject type and walking to the given resource type.
//
// The optimized set will skip branches on intersections and exclusions in an attempt to minimize
// the number of entrypoints.
func (rg *ReachabilityGraph) OptimizedEntrypointsForSubjectToResource(
	ctx context.Context,
	subjectType *core.RelationReference,
	resourceType *core.RelationReference,
) ([]ReachabilityEntrypoint, error) {
	return rg.entrypointsForSubjectToResource(ctx, subjectType, resourceType, reachabilityOptimized, entrypointLookupFindAll)
}

// HasOptimizedEntrypointsForSubjectToResource returns whether there exists any *optimized*
// entrypoints into the reachability graph, starting at the given subject type and walking
// to the given resource type.
//
// The optimized set will skip branches on intersections and exclusions in an attempt to minimize
// the number of entrypoints.
func (rg *ReachabilityGraph) HasOptimizedEntrypointsForSubjectToResource(
	ctx context.Context,
	subjectType *core.RelationReference,
	resourceType *core.RelationReference,
) (bool, error) {
	cacheKey := tuple.StringRR(subjectType) + "=>" + tuple.StringRR(resourceType)
	if result, ok := rg.hasOptimizedEntrypointCache.Load(cacheKey); ok {
		return result.(bool), nil
	}

	// TODO(jzelinskie): measure to see if it's worth singleflighting this
	found, err := rg.entrypointsForSubjectToResource(ctx, subjectType, resourceType, reachabilityOptimized, entrypointLookupFindOne)
	if err != nil {
		return false, err
	}

	result := len(found) > 0
	rg.hasOptimizedEntrypointCache.Store(cacheKey, result)
	return result, nil
}

type entrypointLookupOption int

const (
	entrypointLookupFindAll entrypointLookupOption = iota
	entrypointLookupFindOne
)

func (rg *ReachabilityGraph) entrypointsForSubjectToResource(
	ctx context.Context,
	subjectType *core.RelationReference,
	resourceType *core.RelationReference,
	reachabilityOption reachabilityOption,
	entrypointLookupOption entrypointLookupOption,
) ([]ReachabilityEntrypoint, error) {
	if resourceType.Namespace != rg.ts.nsDef.Name {
		return nil, fmt.Errorf("gave mismatching namespace name for resource type to reachability graph")
	}

	collected := &[]ReachabilityEntrypoint{}
	err := rg.collectEntrypoints(ctx, subjectType, resourceType, collected, map[string]struct{}{}, reachabilityOption, entrypointLookupOption)
	if err != nil {
		return nil, err
	}

	collectedEntrypoints := *collected

	// Deduplicate any entrypoints found. An example that can cause a duplicate is a relation which references
	// the same subject type multiple times due to caveats:
	//
	// relation somerel: user | user with somecaveat
	//
	// This will produce two entrypoints (one per user reference), but as entrypoints themselves are not caveated,
	// one is spurious.
	entrypointMap := make(map[uint64]ReachabilityEntrypoint, len(collectedEntrypoints))
	uniqueEntrypoints := make([]ReachabilityEntrypoint, 0, len(collectedEntrypoints))
	for _, entrypoint := range collectedEntrypoints {
		hash, err := entrypoint.Hash()
		if err != nil {
			return nil, err
		}

		if _, ok := entrypointMap[hash]; !ok {
			entrypointMap[hash] = entrypoint
			uniqueEntrypoints = append(uniqueEntrypoints, entrypoint)
		}
	}

	return uniqueEntrypoints, nil
}

func (rg *ReachabilityGraph) getOrBuildGraph(ctx context.Context, resourceType *core.RelationReference, reachabilityOption reachabilityOption) (*core.ReachabilityGraph, error) {
	// Check the cache.
	// TODO(jschorr): Move this to a global cache.
	cacheKey := tuple.StringRR(resourceType) + "-" + strconv.Itoa(int(reachabilityOption))
	if cached, ok := rg.cachedGraphs.Load(cacheKey); ok {
		return cached.(*core.ReachabilityGraph), nil
	}

	// Load the type system for the target resource relation.
	namespace, err := rg.ts.resolver.LookupNamespace(ctx, resourceType.Namespace)
	if err != nil {
		return nil, err
	}

	rts, err := NewNamespaceTypeSystem(namespace, rg.ts.resolver)
	if err != nil {
		return nil, err
	}

	rrg, err := computeReachability(ctx, rts, resourceType.Relation, reachabilityOption)
	if err != nil {
		return nil, err
	}

	rg.cachedGraphs.Store(cacheKey, rrg)
	return rrg, err
}

func (rg *ReachabilityGraph) collectEntrypoints(
	ctx context.Context,
	subjectType *core.RelationReference,
	resourceType *core.RelationReference,
	collected *[]ReachabilityEntrypoint,
	encounteredRelations map[string]struct{},
	reachabilityOption reachabilityOption,
	entrypointLookupOption entrypointLookupOption,
) error {
	// Ensure that we only process each relation once.
	key := tuple.JoinRelRef(resourceType.Namespace, resourceType.Relation)
	if _, ok := encounteredRelations[key]; ok {
		return nil
	}

	encounteredRelations[key] = struct{}{}

	rrg, err := rg.getOrBuildGraph(ctx, resourceType, reachabilityOption)
	if err != nil {
		return err
	}

	// Add subject type entrypoints.
	subjectTypeEntrypoints, ok := rrg.EntrypointsBySubjectType[subjectType.Namespace]
	if ok {
		addEntrypoints(subjectTypeEntrypoints, resourceType, collected)
	}

	if entrypointLookupOption == entrypointLookupFindOne && len(*collected) > 0 {
		return nil
	}

	// Add subject relation entrypoints.
	subjectRelationEntrypoints, ok := rrg.EntrypointsBySubjectRelation[tuple.JoinRelRef(subjectType.Namespace, subjectType.Relation)]
	if ok {
		addEntrypoints(subjectRelationEntrypoints, resourceType, collected)
	}

	if entrypointLookupOption == entrypointLookupFindOne && len(*collected) > 0 {
		return nil
	}

	// Sort the keys to ensure a stable graph is produced.
	keys := maps.Keys(rrg.EntrypointsBySubjectRelation)
	sort.Strings(keys)

	// Recursively collect over any reachability graphs for subjects with non-ellipsis relations.
	for _, entrypointSetKey := range keys {
		entrypointSet := rrg.EntrypointsBySubjectRelation[entrypointSetKey]
		if entrypointSet.SubjectRelation != nil && entrypointSet.SubjectRelation.Relation != tuple.Ellipsis {
			err := rg.collectEntrypoints(ctx, subjectType, entrypointSet.SubjectRelation, collected, encounteredRelations, reachabilityOption, entrypointLookupOption)
			if err != nil {
				return err
			}

			if entrypointLookupOption == entrypointLookupFindOne && len(*collected) > 0 {
				return nil
			}
		}
	}

	return nil
}

func addEntrypoints(entrypoints *core.ReachabilityEntrypoints, parentRelation *core.RelationReference, collected *[]ReachabilityEntrypoint) {
	for _, entrypoint := range entrypoints.Entrypoints {
		*collected = append(*collected, ReachabilityEntrypoint{entrypoint, parentRelation})
	}
}
