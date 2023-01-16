package graph

import (
	"sync"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/util"
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
	resourcesAndSubjects *util.MultiMap[string, subjectInfo]
}

// subjectInfo is the information about a subject contained in a resourcesSubjectMap.
type subjectInfo struct {
	subjectID  string
	isCaveated bool
}

func newResourcesSubjectMap(resourceType *core.RelationReference) resourcesSubjectMap {
	return resourcesSubjectMap{
		resourceType:         resourceType,
		resourcesAndSubjects: util.NewMultiMap[string, subjectInfo](),
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

// filterForDispatch filters out any resources already found in the dispatched set, returning a
// dispatchableResourcesSubjectMap for dispatching for the remaining resources (if any).
func (rsm resourcesSubjectMap) filterForDispatch(dispatched *syncONRSet) dispatchableResourcesSubjectMap {
	for _, resourceID := range rsm.resourcesAndSubjects.Keys() {
		if !dispatched.Add(&core.ObjectAndRelation{
			Namespace: rsm.resourceType.Namespace,
			ObjectId:  resourceID,
			Relation:  rsm.resourceType.Relation,
		}) {
			rsm.resourcesAndSubjects.RemoveKey(resourceID)
			continue
		}
	}

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
	resourcesAndSubjects util.ReadOnlyMultimap[string, subjectInfo]
}

func (rsm dispatchableResourcesSubjectMap) isEmpty() bool {
	return rsm.resourcesAndSubjects.IsEmpty()
}

func (rsm dispatchableResourcesSubjectMap) resourceIDs() []string {
	return rsm.resourcesAndSubjects.Keys()
}

// asReachableResources converts the resources found in the map into a slice of ReachableResource
// messages, with isDirectEntrypoint and each subject's caveat indicating whether the resource
// is directly found or requires an additional Check operation.
func (rsm dispatchableResourcesSubjectMap) asReachableResources(isDirectEntrypoint bool) []*v1.ReachableResource {
	resources := make([]*v1.ReachableResource, 0, rsm.resourcesAndSubjects.Len())
	for _, resourceID := range rsm.resourcesAndSubjects.Keys() {
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

// mapFoundResources takes in found resources and maps them via their parent relationships to
// the resulting found resources.
func (rsm dispatchableResourcesSubjectMap) mapFoundResources(foundResources []*v1.ReachableResource, isDirectEntrypoint bool) ([]*v1.ReachableResource, error) {
	// For each found resource, lookup the associated entry(s) for the "ForSubjectIDs" and
	// check if *all* are conditional. If so, then the overall status *must* be conditional.
	// Otherwise, the status depends on the status of the incoming result and whether the result
	// was for a direct entrypoint.
	resources := make([]*v1.ReachableResource, 0, len(foundResources))
	for _, foundResource := range foundResources {
		// Start with the status from the found resource.
		status := foundResource.ResultStatus

		// If not a direct entrypoint, then the status, by definition, is to require a check.
		if !isDirectEntrypoint {
			status = v1.ReachableResource_REQUIRES_CHECK
		}

		forSubjectIDs := util.NewSet[string]()
		nonCaveatedSubjectIDs := util.NewSet[string]()
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
			resources = append(resources, &v1.ReachableResource{
				ResourceId:    foundResource.ResourceId,
				ForSubjectIds: nonCaveatedSubjectIDs.AsSlice(),
				ResultStatus:  status,
			})
			continue
		}

		// Otherwise, everything is caveated, so return the full set of subject IDs and mark
		// as a check is required.
		resources = append(resources, &v1.ReachableResource{
			ResourceId:    foundResource.ResourceId,
			ForSubjectIds: forSubjectIDs.AsSlice(),
			ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
		})
	}

	return resources, nil
}
