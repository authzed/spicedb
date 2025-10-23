package schema

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// DefinitionReachability is a helper struct that provides an easy way to determine all entrypoints
// for a subject of a particular type into a schema, for the purpose of walking from the subject
// to a specific resource relation.
type DefinitionReachability struct {
	def                         *Definition
	cachedGraphs                sync.Map
	hasOptimizedEntrypointCache sync.Map
}

// Reachability returns a reachability graph for the given namespace.
func (def *Definition) Reachability() *DefinitionReachability {
	return &DefinitionReachability{def, sync.Map{}, sync.Map{}}
}

// RelationsEncounteredForResource returns all relations that are encountered when walking outward from a resource+relation.
func (rg *DefinitionReachability) RelationsEncounteredForResource(
	ctx context.Context,
	resourceType *core.RelationReference,
) ([]*core.RelationReference, error) {
	_, relationNames, err := rg.computeEntrypoints(ctx, resourceType, nil /* include all entrypoints */, reachabilityFull, entrypointLookupFindAll)
	if err != nil {
		return nil, err
	}

	relationRefs := make([]*core.RelationReference, 0, len(relationNames))
	for _, relationName := range relationNames {
		namespace, relation := tuple.MustSplitRelRef(relationName)
		relationRefs = append(relationRefs, &core.RelationReference{
			Namespace: namespace,
			Relation:  relation,
		})
	}
	return relationRefs, nil
}

// RelationsEncounteredForSubject returns all relations that are encountered when walking outward from a subject+relation.
func (rg *DefinitionReachability) RelationsEncounteredForSubject(
	ctx context.Context,
	allDefinitions []*core.NamespaceDefinition,
	startingSubjectType *core.RelationReference,
) ([]*core.RelationReference, error) {
	if startingSubjectType.GetNamespace() != rg.def.nsDef.GetName() {
		return nil, spiceerrors.MustBugf("gave mismatching namespace name for subject type to reachability graph")
	}

	allRelationNames := mapz.NewSet[string]()

	subjectTypesToCheck := []*core.RelationReference{startingSubjectType}

	// TODO(jschorr): optimize this to not require walking over all types recursively.
	added := mapz.NewSet[string]()
	for len(subjectTypesToCheck) != 0 {
		collected := &[]ReachabilityEntrypoint{}
		for _, nsDef := range allDefinitions {
			nts, err := rg.def.ts.GetDefinition(ctx, nsDef.GetName())
			if err != nil {
				return nil, err
			}

			nrg := nts.Reachability()

			for _, relation := range nsDef.GetRelation() {
				for _, subjectType := range subjectTypesToCheck {
					if subjectType.GetNamespace() == nsDef.GetName() && subjectType.GetRelation() == relation.GetName() {
						continue
					}

					encounteredRelations := map[string]struct{}{}
					err := nrg.collectEntrypoints(ctx, &core.RelationReference{
						Namespace: nsDef.GetName(),
						Relation:  relation.GetName(),
					}, subjectType, collected, encounteredRelations, reachabilityFull, entrypointLookupFindAll)
					if err != nil {
						return nil, err
					}
				}
			}
		}

		subjectTypesToCheck = make([]*core.RelationReference, 0, len(*collected))

		for _, entrypoint := range *collected {
			st := tuple.JoinRelRef(entrypoint.re.GetTargetRelation().GetNamespace(), entrypoint.re.GetTargetRelation().GetRelation())
			if !added.Add(st) {
				continue
			}

			allRelationNames.Add(st)
			subjectTypesToCheck = append(subjectTypesToCheck, entrypoint.re.GetTargetRelation())
		}
	}

	relationRefs := make([]*core.RelationReference, 0, allRelationNames.Len())
	for _, relationName := range allRelationNames.AsSlice() {
		namespace, relation := tuple.MustSplitRelRef(relationName)
		relationRefs = append(relationRefs, &core.RelationReference{
			Namespace: namespace,
			Relation:  relation,
		})
	}
	return relationRefs, nil
}

// AllEntrypointsForSubjectToResource returns the entrypoints into the reachability graph, starting
// at the given subject type and walking to the given resource type.
func (rg *DefinitionReachability) AllEntrypointsForSubjectToResource(
	ctx context.Context,
	subjectType *core.RelationReference,
	resourceType *core.RelationReference,
) ([]ReachabilityEntrypoint, error) {
	entrypoints, _, err := rg.computeEntrypoints(ctx, resourceType, subjectType, reachabilityFull, entrypointLookupFindAll)
	return entrypoints, err
}

// FirstEntrypointsForSubjectToResource returns the *optimized* set of entrypoints into the
// reachability graph, starting at the given subject type and walking to the given resource type.
//
// It does this by limiting the number of entrypoints (and checking the alternatives) and so simply returns the first entrypoint in an
// intersection or exclusion branch.
func (rg *DefinitionReachability) FirstEntrypointsForSubjectToResource(
	ctx context.Context,
	subjectType *core.RelationReference,
	resourceType *core.RelationReference,
) ([]ReachabilityEntrypoint, error) {
	entrypoints, _, err := rg.computeEntrypoints(ctx, resourceType, subjectType, reachabilityFirst, entrypointLookupFindAll)
	return entrypoints, err
}

// HasOptimizedEntrypointsForSubjectToResource returns whether there exists any *optimized*
// entrypoints into the reachability graph, starting at the given subject type and walking
// to the given resource type.
//
// The optimized set will skip branches on intersections and exclusions in an attempt to minimize
// the number of entrypoints.
func (rg *DefinitionReachability) HasOptimizedEntrypointsForSubjectToResource(
	ctx context.Context,
	subjectType *core.RelationReference,
	resourceType *core.RelationReference,
) (bool, error) {
	// TODO(jschorr): Change this to be indexed by a struct
	cacheKey := tuple.StringCoreRR(subjectType) + "=>" + tuple.StringCoreRR(resourceType)
	if result, ok := rg.hasOptimizedEntrypointCache.Load(cacheKey); ok {
		return result.(bool), nil
	}

	// TODO(jzelinskie): measure to see if it's worth singleflighting this
	found, _, err := rg.computeEntrypoints(ctx, resourceType, subjectType, reachabilityFirst, entrypointLookupFindOne)
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

func (rg *DefinitionReachability) computeEntrypoints(
	ctx context.Context,
	resourceType *core.RelationReference,
	optionalSubjectType *core.RelationReference,
	reachabilityOption reachabilityOption,
	entrypointLookupOption entrypointLookupOption,
) ([]ReachabilityEntrypoint, []string, error) {
	if resourceType.GetNamespace() != rg.def.nsDef.GetName() {
		return nil, nil, errors.New("gave mismatching namespace name for resource type to reachability graph")
	}

	collected := &[]ReachabilityEntrypoint{}
	encounteredRelations := map[string]struct{}{}
	err := rg.collectEntrypoints(ctx, resourceType, optionalSubjectType, collected, encounteredRelations, reachabilityOption, entrypointLookupOption)
	if err != nil {
		return nil, slices.Collect(maps.Keys(encounteredRelations)), err
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
			return nil, slices.Collect(maps.Keys(encounteredRelations)), err
		}

		if _, ok := entrypointMap[hash]; !ok {
			entrypointMap[hash] = entrypoint
			uniqueEntrypoints = append(uniqueEntrypoints, entrypoint)
		}
	}

	return uniqueEntrypoints, slices.Collect(maps.Keys(encounteredRelations)), nil
}

func (rg *DefinitionReachability) getOrBuildGraph(ctx context.Context, resourceType *core.RelationReference, reachabilityOption reachabilityOption) (*core.ReachabilityGraph, error) {
	// Check the cache.
	// TODO(jschorr): Change to be indexed by a struct.
	cacheKey := tuple.StringCoreRR(resourceType) + "-" + strconv.Itoa(int(reachabilityOption))
	if cached, ok := rg.cachedGraphs.Load(cacheKey); ok {
		return cached.(*core.ReachabilityGraph), nil
	}

	// Load the type system for the target resource relation.
	tdef, err := rg.def.ts.GetDefinition(ctx, resourceType.GetNamespace())
	if err != nil {
		return nil, err
	}

	rrg, err := computeReachability(ctx, tdef, resourceType.GetRelation(), reachabilityOption)
	if err != nil {
		return nil, err
	}

	rg.cachedGraphs.Store(cacheKey, rrg)
	return rrg, err
}

func (rg *DefinitionReachability) collectEntrypoints(
	ctx context.Context,
	resourceType *core.RelationReference,
	optionalSubjectType *core.RelationReference,
	collected *[]ReachabilityEntrypoint,
	encounteredRelations map[string]struct{},
	reachabilityOption reachabilityOption,
	entrypointLookupOption entrypointLookupOption,
) error {
	// Ensure that we only process each relation once.
	key := tuple.JoinRelRef(resourceType.GetNamespace(), resourceType.GetRelation())
	if _, ok := encounteredRelations[key]; ok {
		return nil
	}

	encounteredRelations[key] = struct{}{}

	rrg, err := rg.getOrBuildGraph(ctx, resourceType, reachabilityOption)
	if err != nil {
		return err
	}

	if optionalSubjectType != nil {
		// Add subject type entrypoints.
		subjectTypeEntrypoints, ok := rrg.GetEntrypointsBySubjectType()[optionalSubjectType.GetNamespace()]
		if ok {
			addEntrypoints(subjectTypeEntrypoints, resourceType, collected, encounteredRelations)
		}

		if entrypointLookupOption == entrypointLookupFindOne && len(*collected) > 0 {
			return nil
		}

		// Add subject relation entrypoints.
		subjectRelationEntrypoints, ok := rrg.GetEntrypointsBySubjectRelation()[tuple.JoinRelRef(optionalSubjectType.GetNamespace(), optionalSubjectType.GetRelation())]
		if ok {
			addEntrypoints(subjectRelationEntrypoints, resourceType, collected, encounteredRelations)
		}

		if entrypointLookupOption == entrypointLookupFindOne && len(*collected) > 0 {
			return nil
		}
	} else {
		// Add all entrypoints.
		for _, entrypoints := range rrg.GetEntrypointsBySubjectType() {
			addEntrypoints(entrypoints, resourceType, collected, encounteredRelations)
		}

		for _, entrypoints := range rrg.GetEntrypointsBySubjectRelation() {
			addEntrypoints(entrypoints, resourceType, collected, encounteredRelations)
		}
	}

	// Sort the keys to ensure a stable graph is produced.
	keys := slices.Collect(maps.Keys(rrg.GetEntrypointsBySubjectRelation()))
	sort.Strings(keys)

	// Recursively collect over any reachability graphs for subjects with non-ellipsis relations.
	for _, entrypointSetKey := range keys {
		entrypointSet := rrg.GetEntrypointsBySubjectRelation()[entrypointSetKey]
		if entrypointSet.GetSubjectRelation() != nil && entrypointSet.GetSubjectRelation().GetRelation() != tuple.Ellipsis {
			err := rg.collectEntrypoints(ctx, entrypointSet.GetSubjectRelation(), optionalSubjectType, collected, encounteredRelations, reachabilityOption, entrypointLookupOption)
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

func addEntrypoints(entrypoints *core.ReachabilityEntrypoints, parentRelation *core.RelationReference, collected *[]ReachabilityEntrypoint, encounteredRelations map[string]struct{}) {
	for _, entrypoint := range entrypoints.GetEntrypoints() {
		if entrypoint.GetTuplesetRelation() != "" {
			key := tuple.JoinRelRef(entrypoint.GetTargetRelation().GetNamespace(), entrypoint.GetTuplesetRelation())
			encounteredRelations[key] = struct{}{}
		}

		*collected = append(*collected, ReachabilityEntrypoint{entrypoint, parentRelation})
	}
}

// ReachabilityEntrypoint is an entrypoint into the reachability graph for a subject of particular
// type.
type ReachabilityEntrypoint struct {
	re             *core.ReachabilityEntrypoint
	parentRelation *core.RelationReference
}

// HashKey returns a unique key for the entrypoint. Note this is *not* stable across versions of SpiceDB,
// and should not be stored for later comparison. It consists of the entrypoint's hash and a prefix.
func (re ReachabilityEntrypoint) HashKey() (string, error) {
	hash, err := re.Hash()
	if err != nil {
		return "", fmt.Errorf("failed to hash entrypoint: %w", err)
	}
	return "entrypoint:" + strconv.FormatUint(hash, 10), nil
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
	return re.re.GetKind()
}

// ComputedUsersetRelation returns the tupleset relation of the computed userset, if any.
func (re ReachabilityEntrypoint) ComputedUsersetRelation() (string, error) {
	if re.EntrypointKind() == core.ReachabilityEntrypoint_RELATION_ENTRYPOINT {
		return "", fmt.Errorf("cannot call ComputedUsersetRelation for kind %v", re.EntrypointKind())
	}
	return re.re.GetComputedUsersetRelation(), nil
}

// TuplesetRelation returns the tupleset relation of the TTU, if a TUPLESET_TO_USERSET_ENTRYPOINT.
func (re ReachabilityEntrypoint) TuplesetRelation() (string, error) {
	if re.EntrypointKind() != core.ReachabilityEntrypoint_TUPLESET_TO_USERSET_ENTRYPOINT {
		return "", fmt.Errorf("cannot call TupleToUserset for kind %v", re.EntrypointKind())
	}

	return re.re.GetTuplesetRelation(), nil
}

// DirectRelation is the relation that this entrypoint represents, if a RELATION_ENTRYPOINT.
func (re ReachabilityEntrypoint) DirectRelation() (*core.RelationReference, error) {
	if re.EntrypointKind() != core.ReachabilityEntrypoint_RELATION_ENTRYPOINT {
		return nil, fmt.Errorf("cannot call DirectRelation for kind %v", re.EntrypointKind())
	}

	return re.re.GetTargetRelation(), nil
}

// TargetNamespace returns the namespace for the entrypoint's target relation.
func (re ReachabilityEntrypoint) TargetNamespace() string {
	return re.re.GetTargetRelation().GetNamespace()
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
	return re.re.GetResultStatus() == core.ReachabilityEntrypoint_DIRECT_OPERATION_RESULT
}

func (re ReachabilityEntrypoint) String() string {
	return re.MustDebugString()
}

func (re ReachabilityEntrypoint) MustDebugString() string {
	ds, err := re.DebugString()
	if err != nil {
		panic(err)
	}

	return ds
}

func (re ReachabilityEntrypoint) DebugStringOrEmpty() string {
	ds, err := re.DebugString()
	if err != nil {
		return ""
	}
	return ds
}

func (re ReachabilityEntrypoint) DebugString() (string, error) {
	switch re.EntrypointKind() {
	case core.ReachabilityEntrypoint_RELATION_ENTRYPOINT:
		return "relation-entrypoint: " + re.re.GetTargetRelation().GetNamespace() + "#" + re.re.GetTargetRelation().GetRelation(), nil

	case core.ReachabilityEntrypoint_TUPLESET_TO_USERSET_ENTRYPOINT:
		return "ttu-entrypoint: " + re.re.GetTuplesetRelation() + " -> " + re.re.GetTargetRelation().GetNamespace() + "#" + re.re.GetTargetRelation().GetRelation(), nil

	case core.ReachabilityEntrypoint_COMPUTED_USERSET_ENTRYPOINT:
		return "computed-userset-entrypoint: " + re.re.GetTargetRelation().GetNamespace() + "#" + re.re.GetTargetRelation().GetRelation(), nil

	default:
		return "", fmt.Errorf("unknown entrypoint kind %v", re.EntrypointKind())
	}
}
