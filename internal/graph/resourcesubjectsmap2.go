package graph

import (
	"sort"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// resourcesSubjectMap2 is a multimap which tracks mappings from found resource IDs
// to the subject IDs (may be more than one) for each, as well as whether the mapping
// is conditional due to the use of a caveat on the relationship which formed the mapping.
type resourcesSubjectMap2 struct {
	resourceType         *core.RelationReference
	resourcesAndSubjects *mapz.MultiMap[string, subjectInfo2]
}

// subjectInfo2 is the information about a subject contained in a resourcesSubjectMap2.
type subjectInfo2 struct {
	subjectID                string
	missingContextParameters []string
}

func newResourcesSubjectMap2(resourceType *core.RelationReference) resourcesSubjectMap2 {
	return resourcesSubjectMap2{
		resourceType:         resourceType,
		resourcesAndSubjects: mapz.NewMultiMap[string, subjectInfo2](),
	}
}

func newResourcesSubjectMap2WithCapacity(resourceType *core.RelationReference, capacity uint32) resourcesSubjectMap2 {
	return resourcesSubjectMap2{
		resourceType:         resourceType,
		resourcesAndSubjects: mapz.NewMultiMapWithCap[string, subjectInfo2](capacity),
	}
}

func subjectIDsToResourcesMap2(resourceType *core.RelationReference, subjectIDs []string) resourcesSubjectMap2 {
	rsm := newResourcesSubjectMap2(resourceType)
	for _, subjectID := range subjectIDs {
		rsm.addSubjectIDAsFoundResourceID(subjectID)
	}
	return rsm
}

// addRelationship adds the relationship to the resource subject map, recording a mapping from
// the resource of the relationship to the subject, as well as whether the relationship was caveated.
func (rsm resourcesSubjectMap2) addRelationship(rel *core.RelationTuple, missingContextParameters []string) error {
	if rel.ResourceAndRelation.Namespace != rsm.resourceType.Namespace ||
		rel.ResourceAndRelation.Relation != rsm.resourceType.Relation {
		return spiceerrors.MustBugf("invalid relationship for addRelationship. expected: %v, found: %v", rsm.resourceType, rel.ResourceAndRelation)
	}

	if len(missingContextParameters) > 0 && rel.Caveat == nil {
		return spiceerrors.MustBugf("missing caveat for caveated relationship")
	}

	rsm.resourcesAndSubjects.Add(rel.ResourceAndRelation.ObjectId, subjectInfo2{rel.Subject.ObjectId, missingContextParameters})
	return nil
}

// withAdditionalMissingContextForDispatchedResourceID adds additional missing context parameters
// to the existing missing context parameters for the dispatched resource ID.
func (rsm resourcesSubjectMap2) withAdditionalMissingContextForDispatchedResourceID(
	resourceID string,
	additionalMissingContext []string,
) {
	if len(additionalMissingContext) == 0 {
		return
	}

	subjectInfo2s, _ := rsm.resourcesAndSubjects.Get(resourceID)
	updatedInfos := make([]subjectInfo2, 0, len(subjectInfo2s))
	for _, info := range subjectInfo2s {
		info.missingContextParameters = append(info.missingContextParameters, additionalMissingContext...)
		updatedInfos = append(updatedInfos, info)
	}
	rsm.resourcesAndSubjects.Set(resourceID, updatedInfos)
}

// addSubjectIDAsFoundResourceID adds a subject ID directly as a found subject for itself as the resource,
// with no associated caveat.
func (rsm resourcesSubjectMap2) addSubjectIDAsFoundResourceID(subjectID string) {
	rsm.resourcesAndSubjects.Add(subjectID, subjectInfo2{subjectID, nil})
}

// asReadOnly returns a read-only dispatchableResourcesSubjectMap2 for dispatching for the
// resources in this map (if any).
func (rsm resourcesSubjectMap2) asReadOnly() dispatchableResourcesSubjectMap2 {
	return dispatchableResourcesSubjectMap2{rsm}
}

func (rsm resourcesSubjectMap2) len() int {
	return rsm.resourcesAndSubjects.Len()
}

// dispatchableResourcesSubjectMap2 is a read-only, frozen version of the resourcesSubjectMap2 that
// can be used for mapping conditionals once calls have been dispatched. This is read-only due to
// its use by concurrent callers.
type dispatchableResourcesSubjectMap2 struct {
	resourcesSubjectMap2
}

func (rsm dispatchableResourcesSubjectMap2) isEmpty() bool {
	return rsm.resourcesAndSubjects.IsEmpty()
}

func (rsm dispatchableResourcesSubjectMap2) resourceIDs() []string {
	return rsm.resourcesAndSubjects.Keys()
}

// filterSubjectIDsToDispatch returns the set of subject IDs that have not yet been
// dispatched, by adding them to the dispatched set.
func (rsm dispatchableResourcesSubjectMap2) filterSubjectIDsToDispatch(dispatched *syncONRSet, dispatchSubjectType *core.RelationReference) []string {
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

// cloneAsMutable returns a mutable clone of this dispatchableResourcesSubjectMap2.
func (rsm dispatchableResourcesSubjectMap2) cloneAsMutable() resourcesSubjectMap2 {
	return resourcesSubjectMap2{
		resourceType:         rsm.resourceType,
		resourcesAndSubjects: rsm.resourcesAndSubjects.Clone(),
	}
}

func (rsm dispatchableResourcesSubjectMap2) asPossibleResources() []*v1.PossibleResource {
	resources := make([]*v1.PossibleResource, 0, rsm.resourcesAndSubjects.Len())

	// Sort for stability.
	sortedResourceIds := rsm.resourcesAndSubjects.Keys()
	sort.Strings(sortedResourceIds)

	for _, resourceID := range sortedResourceIds {
		subjectInfo2s, _ := rsm.resourcesAndSubjects.Get(resourceID)
		subjectIDs := make([]string, 0, len(subjectInfo2s))
		allCaveated := true
		nonCaveatedSubjectIDs := make([]string, 0, len(subjectInfo2s))
		missingContextParameters := mapz.NewSet[string]()

		for _, info := range subjectInfo2s {
			subjectIDs = append(subjectIDs, info.subjectID)
			if len(info.missingContextParameters) == 0 {
				allCaveated = false
				nonCaveatedSubjectIDs = append(nonCaveatedSubjectIDs, info.subjectID)
			} else {
				missingContextParameters.Extend(info.missingContextParameters)
			}
		}

		// Sort for stability.
		sort.Strings(subjectIDs)

		// If all the incoming edges are caveated, then the entire status has to be marked as a check
		// is required. Otherwise, if there is at least *one* non-caveated incoming edge, then we can
		// return the existing status as a short-circuit for those non-caveated found subjects.
		if allCaveated {
			resources = append(resources, &v1.PossibleResource{
				ResourceId:           resourceID,
				ForSubjectIds:        subjectIDs,
				MissingContextParams: missingContextParameters.AsSlice(),
			})
		} else {
			resources = append(resources, &v1.PossibleResource{
				ResourceId:    resourceID,
				ForSubjectIds: nonCaveatedSubjectIDs,
			})
		}
	}
	return resources
}

func (rsm dispatchableResourcesSubjectMap2) mapPossibleResource(foundResource *v1.PossibleResource) (*v1.PossibleResource, error) {
	forSubjectIDs := mapz.NewSet[string]()
	nonCaveatedSubjectIDs := mapz.NewSet[string]()
	missingContextParameters := mapz.NewSet[string]()

	for _, forSubjectID := range foundResource.ForSubjectIds {
		// Map from the incoming subject ID to the subject ID(s) that caused the dispatch.
		infos, ok := rsm.resourcesAndSubjects.Get(forSubjectID)
		if !ok {
			return nil, spiceerrors.MustBugf("missing for subject ID")
		}

		for _, info := range infos {
			forSubjectIDs.Insert(info.subjectID)
			if len(info.missingContextParameters) == 0 {
				nonCaveatedSubjectIDs.Insert(info.subjectID)
			} else {
				missingContextParameters.Extend(info.missingContextParameters)
			}
		}
	}

	// If there are some non-caveated IDs, return those and mark as the parent status.
	if nonCaveatedSubjectIDs.Len() > 0 {
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
