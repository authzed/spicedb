package graph

import (
	"sort"
	"sync"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

type syncONRSet struct {
	items sync.Map
}

func (s *syncONRSet) Add(onr *core.ObjectAndRelation) bool {
	key := tuple.StringONR(onr)
	_, existed := s.items.LoadOrStore(key, struct{}{})
	return !existed
}

// resourcesSubjectMap is a multimap which tracks mappings from found resource IDs
// to the subject IDs (may be more than one) for each, as well as whether the mapping
// is conditional due to the use of a caveat on the relationship which formed the mapping.
type resourcesSubjectMap struct {
	resourceType         *core.RelationReference
	resourcesAndSubjects *mapz.MultiMap[string, subjectInfo]
}

// subjectInfo is the information about a subject contained in a resourcesSubjectMap.
type subjectInfo struct {
	subjectID  string
	isCaveated bool
}

func newResourcesSubjectMap(resourceType *core.RelationReference) resourcesSubjectMap {
	return resourcesSubjectMap{
		resourceType:         resourceType,
		resourcesAndSubjects: mapz.NewMultiMap[string, subjectInfo](),
	}
}

func newResourcesSubjectMapWithCapacity(resourceType *core.RelationReference, capacity uint32) resourcesSubjectMap {
	return resourcesSubjectMap{
		resourceType:         resourceType,
		resourcesAndSubjects: mapz.NewMultiMapWithCap[string, subjectInfo](capacity),
	}
}

func subjectIDsToResourcesMap(resourceType *core.RelationReference, subjectIDs []string) resourcesSubjectMap {
	rsm := newResourcesSubjectMap(resourceType)
	for _, subjectID := range subjectIDs {
		rsm.addSubjectIDAsFoundResourceID(subjectID)
	}
	return rsm
}

// addRelationship adds the relationship to the resource subject map, recording a mapping from
// the resource of the relationship to the subject, as well as whether the relationship was caveated.
func (rsm resourcesSubjectMap) addRelationship(rel *core.RelationTuple) error {
	if rel.ResourceAndRelation.Namespace != rsm.resourceType.Namespace ||
		rel.ResourceAndRelation.Relation != rsm.resourceType.Relation {
		return spiceerrors.MustBugf("invalid relationship for addRelationship. expected: %v, found: %v", rsm.resourceType, rel.ResourceAndRelation)
	}

	rsm.resourcesAndSubjects.Add(rel.ResourceAndRelation.ObjectId, subjectInfo{rel.Subject.ObjectId, rel.Caveat != nil && rel.Caveat.CaveatName != ""})
	return nil
}

// addSubjectIDAsFoundResourceID adds a subject ID directly as a found subject for itself as the resource,
// with no associated caveat.
func (rsm resourcesSubjectMap) addSubjectIDAsFoundResourceID(subjectID string) {
	rsm.resourcesAndSubjects.Add(subjectID, subjectInfo{subjectID, false})
}

// asReadOnly returns a read-only dispatchableResourcesSubjectMap for dispatching for the
// resources in this map (if any).
func (rsm resourcesSubjectMap) asReadOnly() dispatchableResourcesSubjectMap {
	return dispatchableResourcesSubjectMap{rsm.resourceType, rsm.resourcesAndSubjects.AsReadOnly()}
}

func (rsm resourcesSubjectMap) len() int {
	return rsm.resourcesAndSubjects.Len()
}

// dispatchableResourcesSubjectMap is a read-only, frozen version of the resourcesSubjectMap that
// can be used for mapping conditionals once calls have been dispatched. This is read-only due to
// its use by concurrent callers.
type dispatchableResourcesSubjectMap struct {
	resourceType         *core.RelationReference
	resourcesAndSubjects mapz.ReadOnlyMultimap[string, subjectInfo]
}

func (rsm dispatchableResourcesSubjectMap) isEmpty() bool {
	return rsm.resourcesAndSubjects.IsEmpty()
}

func (rsm dispatchableResourcesSubjectMap) resourceIDs() []string {
	return rsm.resourcesAndSubjects.Keys()
}

// filterSubjectIDsToDispatch returns the set of subject IDs that have not yet been
// dispatched, by adding them to the dispatched set.
func (rsm dispatchableResourcesSubjectMap) filterSubjectIDsToDispatch(dispatched *syncONRSet, dispatchSubjectType *core.RelationReference) []string {
	resourceIDs := rsm.resourceIDs()
	filtered := make([]string, 0, len(resourceIDs))
	for _, resourceID := range resourceIDs {
		if dispatched.Add(&core.ObjectAndRelation{
			Namespace: dispatchSubjectType.Namespace,
			ObjectId:  resourceID,
			Relation:  dispatchSubjectType.Relation,
		}) {
			filtered = append(filtered, resourceID)
		}
	}

	return filtered
}

// asReachableResources converts the resources found in the map into a slice of ReachableResource
// messages, with isDirectEntrypoint and each subject's caveat indicating whether the resource
// is directly found or requires an additional Check operation.
func (rsm dispatchableResourcesSubjectMap) asReachableResources(isDirectEntrypoint bool) []*v1.ReachableResource {
	resources := make([]*v1.ReachableResource, 0, rsm.resourcesAndSubjects.Len())

	// Sort for stability.
	sortedResourceIds := rsm.resourcesAndSubjects.Keys()
	sort.Strings(sortedResourceIds)

	for _, resourceID := range sortedResourceIds {
		status := v1.ReachableResource_REQUIRES_CHECK
		if isDirectEntrypoint {
			status = v1.ReachableResource_HAS_PERMISSION
		}

		subjectInfos, _ := rsm.resourcesAndSubjects.Get(resourceID)
		subjectIDs := make([]string, 0, len(subjectInfos))
		allCaveated := true
		nonCaveatedSubjectIDs := make([]string, 0, len(subjectInfos))

		for _, info := range subjectInfos {
			subjectIDs = append(subjectIDs, info.subjectID)
			if !info.isCaveated {
				allCaveated = false
				nonCaveatedSubjectIDs = append(nonCaveatedSubjectIDs, info.subjectID)
			}
		}

		// Sort for stability.
		sort.Strings(subjectIDs)

		// If all the incoming edges are caveated, then the entire status has to be marked as a check
		// is required. Otherwise, if there is at least *one* non-caveated incoming edge, then we can
		// return the existing status as a short-circuit for those non-caveated found subjects.
		if allCaveated {
			resources = append(resources, &v1.ReachableResource{
				ResourceId:    resourceID,
				ForSubjectIds: subjectIDs,
				ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
			})
		} else {
			resources = append(resources, &v1.ReachableResource{
				ResourceId:    resourceID,
				ForSubjectIds: nonCaveatedSubjectIDs,
				ResultStatus:  status,
			})
		}
	}
	return resources
}

// mapFoundResource takes in a found resource and maps it via its parent relationship to
// the resulting found resource.
func (rsm dispatchableResourcesSubjectMap) mapFoundResource(foundResource *v1.ReachableResource, isDirectEntrypoint bool) (*v1.ReachableResource, error) {
	// For the found resource, lookup the associated entry(s) for the "ForSubjectIDs" and
	// check if *all* are conditional. If so, then the overall status *must* be conditional.
	// Otherwise, the status depends on the status of the incoming result and whether the result
	// was for a direct entrypoint.
	// Start with the status from the found resource.
	status := foundResource.ResultStatus

	// If not a direct entrypoint, then the status, by definition, is to require a check.
	if !isDirectEntrypoint {
		status = v1.ReachableResource_REQUIRES_CHECK
	}

	forSubjectIDs := mapz.NewSet[string]()
	nonCaveatedSubjectIDs := mapz.NewSet[string]()
	for _, forSubjectID := range foundResource.ForSubjectIds {
		// Map from the incoming subject ID to the subject ID(s) that caused the dispatch.
		infos, ok := rsm.resourcesAndSubjects.Get(forSubjectID)
		if !ok {
			return nil, spiceerrors.MustBugf("missing for subject ID")
		}

		for _, info := range infos {
			forSubjectIDs.Add(info.subjectID)
			if !info.isCaveated {
				nonCaveatedSubjectIDs.Add(info.subjectID)
			}
		}
	}

	// If there are some non-caveated IDs, return those and mark as the parent status.
	if nonCaveatedSubjectIDs.Len() > 0 {
		return &v1.ReachableResource{
			ResourceId:    foundResource.ResourceId,
			ForSubjectIds: nonCaveatedSubjectIDs.AsSlice(),
			ResultStatus:  status,
		}, nil
	}

	// Otherwise, everything is caveated, so return the full set of subject IDs and mark
	// as a check is required.
	return &v1.ReachableResource{
		ResourceId:    foundResource.ResourceId,
		ForSubjectIds: forSubjectIDs.AsSlice(),
		ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
	}, nil
}
