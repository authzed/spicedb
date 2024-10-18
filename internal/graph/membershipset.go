package graph

import (
	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	caveatOr   = caveats.Or
	caveatAnd  = caveats.And
	caveatSub  = caveats.Subtract
	wrapCaveat = caveats.CaveatAsExpr
)

// CheckResultsMap defines a type that is a map from resource ID to ResourceCheckResult.
// This must match that defined in the DispatchCheckResponse for the `results_by_resource_id`
// field.
type CheckResultsMap map[string]*v1.ResourceCheckResult

// NewMembershipSet constructs a new helper set for tracking the membership found for a dispatched
// check request.
func NewMembershipSet() *MembershipSet {
	return &MembershipSet{
		hasDeterminedMember: false,
		membersByID:         map[string]*core.CaveatExpression{},
	}
}

func membershipSetFromMap(mp map[string]*core.CaveatExpression) *MembershipSet {
	ms := NewMembershipSet()
	for resourceID, result := range mp {
		ms.addMember(resourceID, result)
	}
	return ms
}

// MembershipSet is a helper set that trackes the membership results for a dispatched Check
// request, including tracking of the caveats associated with found resource IDs.
type MembershipSet struct {
	membersByID         map[string]*core.CaveatExpression
	hasDeterminedMember bool
}

// AddDirectMember adds a resource ID that was *directly* found for the dispatched check, with
// optional caveat found on the relationship.
func (ms *MembershipSet) AddDirectMember(resourceID string, caveat *core.ContextualizedCaveat) {
	ms.addMember(resourceID, wrapCaveat(caveat))
}

// AddMemberViaRelationship adds a resource ID that was found via another relationship, such
// as the result of an arrow operation. The `parentRelationship` is the relationship that was
// followed before the resource itself was resolved. This method will properly apply the caveat(s)
// from both the parent relationship and the resource's result itself, assuming either have a caveat
// associated.
func (ms *MembershipSet) AddMemberViaRelationship(
	resourceID string,
	resourceCaveatExpression *core.CaveatExpression,
	parentRelationship tuple.Relationship,
) {
	ms.AddMemberWithParentCaveat(resourceID, resourceCaveatExpression, parentRelationship.OptionalCaveat)
}

// AddMemberWithParentCaveat adds the given resource ID as a member with the parent caveat
// combined via intersection with the resource's caveat. The parent caveat may be nil.
func (ms *MembershipSet) AddMemberWithParentCaveat(
	resourceID string,
	resourceCaveatExpression *core.CaveatExpression,
	parentCaveat *core.ContextualizedCaveat,
) {
	intersection := caveatAnd(wrapCaveat(parentCaveat), resourceCaveatExpression)
	ms.addMember(resourceID, intersection)
}

// AddMemberWithOptionalCaveats adds the given resource ID as a member with the optional caveats combined
// via intersection.
func (ms *MembershipSet) AddMemberWithOptionalCaveats(
	resourceID string,
	caveats []*core.CaveatExpression,
) {
	if len(caveats) == 0 {
		ms.addMember(resourceID, nil)
		return
	}

	intersection := caveats[0]
	for _, caveat := range caveats[1:] {
		intersection = caveatAnd(intersection, caveat)
	}

	ms.addMember(resourceID, intersection)
}

func (ms *MembershipSet) addMember(resourceID string, caveatExpr *core.CaveatExpression) {
	existing, ok := ms.membersByID[resourceID]
	if !ok {
		ms.hasDeterminedMember = ms.hasDeterminedMember || caveatExpr == nil
		ms.membersByID[resourceID] = caveatExpr
		return
	}

	// If a determined membership result has already been found (i.e. there is no caveat),
	// then nothing more to do.
	if existing == nil {
		return
	}

	// If the new caveat expression is nil, then we are adding a determined result.
	if caveatExpr == nil {
		ms.hasDeterminedMember = true
		ms.membersByID[resourceID] = nil
		return
	}

	// Otherwise, the caveats get unioned together.
	ms.membersByID[resourceID] = caveatOr(existing, caveatExpr)
}

// UnionWith combines the results found in the given map with the members of this set.
// The changes are made in-place.
func (ms *MembershipSet) UnionWith(resultsMap CheckResultsMap) {
	for resourceID, details := range resultsMap {
		if details.Membership != v1.ResourceCheckResult_NOT_MEMBER {
			ms.addMember(resourceID, details.Expression)
		}
	}
}

// IntersectWith intersects the results found in the given map with the members of this set.
// The changes are made in-place.
func (ms *MembershipSet) IntersectWith(resultsMap CheckResultsMap) {
	for resourceID := range ms.membersByID {
		if details, ok := resultsMap[resourceID]; !ok || details.Membership == v1.ResourceCheckResult_NOT_MEMBER {
			delete(ms.membersByID, resourceID)
		}
	}

	ms.hasDeterminedMember = false
	for resourceID, details := range resultsMap {
		existing, ok := ms.membersByID[resourceID]
		if !ok || details.Membership == v1.ResourceCheckResult_NOT_MEMBER {
			continue
		}
		if existing == nil && details.Expression == nil {
			ms.hasDeterminedMember = true
			continue
		}

		ms.membersByID[resourceID] = caveatAnd(existing, details.Expression)
	}
}

// Subtract subtracts the results found in the given map with the members of this set.
// The changes are made in-place.
func (ms *MembershipSet) Subtract(resultsMap CheckResultsMap) {
	ms.hasDeterminedMember = false
	for resourceID, expression := range ms.membersByID {
		if details, ok := resultsMap[resourceID]; ok && details.Membership != v1.ResourceCheckResult_NOT_MEMBER {
			// If the incoming member has no caveat, then this removal is absolute.
			if details.Expression == nil {
				delete(ms.membersByID, resourceID)
				continue
			}

			// Otherwise, the caveat expression gets combined with an intersection of the inversion
			// of the expression.
			ms.membersByID[resourceID] = caveatSub(expression, details.Expression)
		} else {
			if expression == nil {
				ms.hasDeterminedMember = true
			}
		}
	}
}

// HasConcreteResourceID returns whether the resourceID was found in the set
// and has no caveat attached.
func (ms *MembershipSet) HasConcreteResourceID(resourceID string) bool {
	if ms == nil {
		return false
	}

	found, ok := ms.membersByID[resourceID]
	return ok && found == nil
}

// GetResourceID returns a bool indicating whether the resource is found in the set and the
// associated caveat expression, if any.
func (ms *MembershipSet) GetResourceID(resourceID string) (bool, *core.CaveatExpression) {
	if ms == nil {
		return false, nil
	}

	caveat, ok := ms.membersByID[resourceID]
	return ok, caveat
}

// Size returns the number of elements in the membership set.
func (ms *MembershipSet) Size() int {
	if ms == nil {
		return 0
	}

	return len(ms.membersByID)
}

// IsEmpty returns true if the set is empty.
func (ms *MembershipSet) IsEmpty() bool {
	if ms == nil {
		return true
	}

	return len(ms.membersByID) == 0
}

// HasDeterminedMember returns whether there exists at least one non-caveated member of the set.
func (ms *MembershipSet) HasDeterminedMember() bool {
	if ms == nil {
		return false
	}

	return ms.hasDeterminedMember
}

// AsCheckResultsMap converts the membership set back into a CheckResultsMap for placement into
// a DispatchCheckResult.
func (ms *MembershipSet) AsCheckResultsMap() CheckResultsMap {
	resultsMap := make(CheckResultsMap, len(ms.membersByID))
	for resourceID, caveat := range ms.membersByID {
		membership := v1.ResourceCheckResult_MEMBER
		if caveat != nil {
			membership = v1.ResourceCheckResult_CAVEATED_MEMBER
		}

		resultsMap[resourceID] = &v1.ResourceCheckResult{
			Membership: membership,
			Expression: caveat,
		}
	}

	return resultsMap
}
