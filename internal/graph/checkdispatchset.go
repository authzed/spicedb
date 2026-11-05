package graph

import (
	"context"
	"sort"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
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

	// hint is the routing hint for the dispatch of this chunk.
	// It only improves cache locality. A dispatch with no hint or a different hint gives the same result.
	hint dispatch.RoutingHint
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
	s.bySubject.Add(rel.Subject, riac)

	// Add the subject ID to the map of subjects for the type of subject.
	hasIncomingCaveats := rel.OptionalCaveat != nil && rel.OptionalCaveat.CaveatName != ""

	subjectIDsForType, ok := s.bySubjectType[rel.Subject.RelationReference()]
	if !ok {
		subjectIDsForType = make(map[string]bool)
		s.bySubjectType[rel.Subject.RelationReference()] = subjectIDsForType
	}

	// If a caveat exists for the subject ID in any branch, the whole branch is considered caveated.
	subjectIDsForType[rel.Subject.ObjectID] = hasIncomingCaveats || subjectIDsForType[rel.Subject.ObjectID]
}

// dispatchChunks forms the dispatch chunks with the chunker of ctx.
func (s *checkDispatchSet) dispatchChunks(ctx context.Context, dispatchChunkSize uint16) []checkDispatchChunk {
	chunker := dispatch.ChunkerFromContext(ctx)

	// Start with an estimate of one chunk per type, plus one for the remainder.
	expectedNumberOfChunks := len(s.bySubjectType) + 1
	toDispatch := make([]checkDispatchChunk, 0, expectedNumberOfChunks)

	// For each type of subject, create chunks of the IDs over which to dispatch.
	for subjectType, hasCaveatBySubjectID := range s.bySubjectType {
		subjectIDs := make([]string, 0, len(hasCaveatBySubjectID))
		for objectID := range hasCaveatBySubjectID {
			subjectIDs = append(subjectIDs, objectID)
		}

		// Sort the subject IDs by whether they have caveats and then by the ID itself.
		sort.Slice(subjectIDs, func(i, j int) bool {
			iHasCaveat := hasCaveatBySubjectID[subjectIDs[i]]
			jHasCaveat := hasCaveatBySubjectID[subjectIDs[j]]
			if iHasCaveat == jHasCaveat {
				return subjectIDs[i] < subjectIDs[j]
			}
			return iHasCaveat && !jHasCaveat
		})

		// The chunker can move IDs between chunks, so compute the caveat flag for each chunk.
		chunks := chunker.Chunk(ctx, subjectType.ObjectType, subjectIDs, dispatchChunkSize)
		for _, chunk := range chunks {
			hasIncomingCaveats := false
			for _, subjectID := range chunk.ResourceIDs {
				if hasCaveatBySubjectID[subjectID] {
					hasIncomingCaveats = true
					break
				}
			}

			toDispatch = append(toDispatch, checkDispatchChunk{
				resourceType:       subjectType,
				resourceIds:        chunk.ResourceIDs,
				hasIncomingCaveats: hasIncomingCaveats,
				hint:               chunk.Hint,
			})
		}
		dispatchChunkCountHistogram.Observe(float64(len(chunks)))
	}

	return toDispatch
}

// mappingsForSubject returns the mappings that apply to the relationship between the specified
// subject and any of its resources. The returned caveats include the resource ID of the resource
// that the subject has a relationship with.
func (s *checkDispatchSet) mappingsForSubject(subjectType string, subjectObjectID string, subjectRelation string) []resourceIDAndCaveat {
	results, ok := s.bySubject.Get(tuple.ONR(subjectType, subjectObjectID, subjectRelation))
	spiceerrors.DebugAssertf(func() bool { return ok }, "no caveats found for subject %s:%s:%s", subjectType, subjectObjectID, subjectRelation)
	return results
}
