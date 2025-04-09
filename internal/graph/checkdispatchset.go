package graph

import (
	"sort"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// checkDispatchSet is the set of subjects over which check will need to dispatch
// as subproblems in order to answer the parent problem.
type checkDispatchSet struct {
	// bySubjectType is a map from the type of subject to the set of subjects of that type
	// over which to dispatch, along with information indicating whether caveats are present
	// for that chunk.
	bySubjectType map[tuple.RelationReference]map[string]bool

	// bySubject is a map from the subject to the set of resources for which the subject
	// has a relationship, along with the caveats that apply to that relationship.
	bySubject *mapz.MultiMap[tuple.ObjectAndRelation, resourceIDAndCaveat]
}

// checkDispatchChunk is a chunk of subjects over which to dispatch a check operation.
type checkDispatchChunk struct {
	// resourceType is the type of the subjects in this chunk.
	resourceType tuple.RelationReference

	// resourceIds is the set of subjects in this chunk.
	resourceIds []string

	// hasIncomingCaveats is true if any of the subjects in this chunk have incoming caveats.
	// This is used to determine whether the check operation should be dispatched requiring
	// all results.
	hasIncomingCaveats bool
}

// subjectIDAndHasCaveat is a tuple of a subject ID and whether it has a caveat.
type subjectIDAndHasCaveat struct {
	// objectID is the ID of the subject.
	objectID string

	// hasIncomingCaveats is true if the subject has a caveat.
	hasIncomingCaveats bool
}

// resourceIDAndCaveat is a tuple of a resource ID and a caveat.
type resourceIDAndCaveat struct {
	// resourceID is the ID of the resource.
	resourceID string

	// caveat is the caveat that applies to the relationship between the subject and the resource.
	// May be nil.
	caveat *core.ContextualizedCaveat
}

// newCheckDispatchSet creates and returns a new checkDispatchSet.
func newCheckDispatchSet() *checkDispatchSet {
	return &checkDispatchSet{
		bySubjectType: map[tuple.RelationReference]map[string]bool{},
		bySubject:     mapz.NewMultiMap[tuple.ObjectAndRelation, resourceIDAndCaveat](),
	}
}

// Add adds the specified ObjectAndRelation to the set.
func (s *checkDispatchSet) addForRelationship(rel tuple.Relationship) {
	// Add an entry for the subject pointing to the resource ID and caveat for the subject.
	riac := resourceIDAndCaveat{
		resourceID: rel.Resource.ObjectID,
		caveat:     rel.OptionalCaveat,
	}
	subjectWithoutObjectData := tuple.ONR(rel.Subject.ObjectType, rel.Subject.ObjectID, rel.Subject.Relation)
	s.bySubject.Add(subjectWithoutObjectData, riac)

	// Add the subject ID to the map of subjects for the type of subject.
	siac := subjectIDAndHasCaveat{
		objectID:           rel.Subject.ObjectID,
		hasIncomingCaveats: rel.OptionalCaveat != nil && rel.OptionalCaveat.CaveatName != "",
	}

	subjectIDsForType, ok := s.bySubjectType[rel.Subject.RelationReference()]
	if !ok {
		subjectIDsForType = make(map[string]bool)
		s.bySubjectType[rel.Subject.RelationReference()] = subjectIDsForType
	}

	// If a caveat exists for the subject ID in any branch, the whole branch is considered caveated.
	subjectIDsForType[rel.Subject.ObjectID] = siac.hasIncomingCaveats || subjectIDsForType[rel.Subject.ObjectID]
}

func (s *checkDispatchSet) dispatchChunks(dispatchChunkSize uint16) []checkDispatchChunk {
	// Start with an estimate of one chunk per type, plus one for the remainder.
	expectedNumberOfChunks := len(s.bySubjectType) + 1
	toDispatch := make([]checkDispatchChunk, 0, expectedNumberOfChunks)

	// For each type of subject, create chunks of the IDs over which to dispatch.
	for subjectType, subjectIDsAndHasCaveats := range s.bySubjectType {
		entries := make([]subjectIDAndHasCaveat, 0, len(subjectIDsAndHasCaveats))
		for objectID, hasIncomingCaveats := range subjectIDsAndHasCaveats {
			entries = append(entries, subjectIDAndHasCaveat{objectID: objectID, hasIncomingCaveats: hasIncomingCaveats})
		}

		// Sort the list of subject IDs by whether they have caveats and then the ID itself.
		sort.Slice(entries, func(i, j int) bool {
			iHasCaveat := entries[i].hasIncomingCaveats
			jHasCaveat := entries[j].hasIncomingCaveats
			if iHasCaveat == jHasCaveat {
				return entries[i].objectID < entries[j].objectID
			}
			return iHasCaveat && !jHasCaveat
		})

		chunkCount := 0.0
		slicez.ForEachChunk(entries, dispatchChunkSize, func(subjectIdChunk []subjectIDAndHasCaveat) {
			chunkCount++

			subjectIDsToDispatch := make([]string, 0, len(subjectIdChunk))
			hasIncomingCaveats := false
			for _, entry := range subjectIdChunk {
				subjectIDsToDispatch = append(subjectIDsToDispatch, entry.objectID)
				hasIncomingCaveats = hasIncomingCaveats || entry.hasIncomingCaveats
			}

			toDispatch = append(toDispatch, checkDispatchChunk{
				resourceType:       subjectType,
				resourceIds:        subjectIDsToDispatch,
				hasIncomingCaveats: hasIncomingCaveats,
			})
		})
		dispatchChunkCountHistogram.Observe(chunkCount)
	}

	return toDispatch
}

// mappingsForSubject returns the mappings that apply to the relationship between the specified
// subject and any of its resources. The returned caveats include the resource ID of the resource
// that the subject has a relationship with.
func (s *checkDispatchSet) mappingsForSubject(subjectType string, subjectObjectID string, subjectRelation string) []resourceIDAndCaveat {
	results, ok := s.bySubject.Get(tuple.ONR(subjectType, subjectObjectID, subjectRelation))
	spiceerrors.DebugAssert(func() bool { return ok }, "no caveats found for subject %s:%s:%s", subjectType, subjectObjectID, subjectRelation)
	return results
}
