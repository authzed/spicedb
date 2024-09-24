package datasets

import (
	"golang.org/x/exp/maps"

	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	caveatAnd        = caveats.And
	caveatOr         = caveats.Or
	caveatInvert     = caveats.Invert
	shortcircuitedOr = caveats.ShortcircuitedOr
)

// Subject is a subject that can be placed into a BaseSubjectSet. It is defined in a generic
// manner to allow implementations that wrap BaseSubjectSet to add their own additional bookkeeping
// to the base implementation.
type Subject[T any] interface {
	// GetSubjectId returns the ID of the subject. For wildcards, this should be `*`.
	GetSubjectId() string

	// GetCaveatExpression returns the caveat expression for this subject, if it is conditional.
	GetCaveatExpression() *core.CaveatExpression

	// GetExcludedSubjects returns the list of subjects excluded. Must only have values
	// for wildcards and must never be nested.
	GetExcludedSubjects() []T
}

// BaseSubjectSet defines a set that tracks accessible subjects, their exclusions (if wildcards),
// and all conditional expressions applied due to caveats.
//
// It is generic to allow other implementations to define the kind of tracking information
// associated with each subject.
//
// NOTE: Unlike a traditional set, unions between wildcards and a concrete subject will result
// in *both* being present in the set, to maintain the proper set semantics around wildcards.
type BaseSubjectSet[T Subject[T]] struct {
	constructor constructor[T]
	concrete    map[string]T
	wildcard    *handle[T]
}

// NewBaseSubjectSet creates a new base subject set for use underneath well-typed implementation.
//
// The constructor function returns a new instance of type T for a particular subject ID.
func NewBaseSubjectSet[T Subject[T]](constructor constructor[T]) BaseSubjectSet[T] {
	return BaseSubjectSet[T]{
		constructor: constructor,
		concrete:    map[string]T{},
		wildcard:    newHandle[T](),
	}
}

// constructor defines a function for constructing a new instance of the Subject type T for
// a subject ID, its (optional) conditional expression, any excluded subjects, and any sources
// for bookkeeping. The sources are those other subjects that were combined to create the current
// subject.
type constructor[T Subject[T]] func(subjectID string, conditionalExpression *core.CaveatExpression, excludedSubjects []T, sources ...T) T

// MustAdd adds the found subject to the set. This is equivalent to a Union operation between the
// existing set of subjects and a set containing the single subject, but modifies the set
// *in place*.
func (bss BaseSubjectSet[T]) MustAdd(foundSubject T) {
	err := bss.Add(foundSubject)
	if err != nil {
		panic(err)
	}
}

// Add adds the found subject to the set. This is equivalent to a Union operation between the
// existing set of subjects and a set containing the single subject, but modifies the set
// *in place*.
func (bss BaseSubjectSet[T]) Add(foundSubject T) error {
	if foundSubject.GetSubjectId() == tuple.PublicWildcard {
		existing := bss.wildcard.getOrNil()
		updated, err := unionWildcardWithWildcard(existing, foundSubject, bss.constructor)
		if err != nil {
			return err
		}

		bss.wildcard.setOrNil(updated)

		for _, concrete := range bss.concrete {
			updated = unionWildcardWithConcrete(updated, concrete, bss.constructor)
		}
		bss.wildcard.setOrNil(updated)
		return nil
	}

	var updatedOrNil *T
	if updated, ok := bss.concrete[foundSubject.GetSubjectId()]; ok {
		updatedOrNil = &updated
	}
	bss.setConcrete(foundSubject.GetSubjectId(), unionConcreteWithConcrete(updatedOrNil, &foundSubject, bss.constructor))

	wildcard := bss.wildcard.getOrNil()
	wildcard = unionWildcardWithConcrete(wildcard, foundSubject, bss.constructor)
	bss.wildcard.setOrNil(wildcard)
	return nil
}

func (bss BaseSubjectSet[T]) setConcrete(subjectID string, subjectOrNil *T) {
	if subjectOrNil == nil {
		delete(bss.concrete, subjectID)
		return
	}

	subject := *subjectOrNil
	bss.concrete[subject.GetSubjectId()] = subject
}

// Subtract subtracts the given subject found the set.
func (bss BaseSubjectSet[T]) Subtract(toRemove T) {
	if toRemove.GetSubjectId() == tuple.PublicWildcard {
		for _, concrete := range bss.concrete {
			bss.setConcrete(concrete.GetSubjectId(), subtractWildcardFromConcrete(concrete, toRemove, bss.constructor))
		}

		existing := bss.wildcard.getOrNil()
		updatedWildcard, concretesToAdd := subtractWildcardFromWildcard(existing, toRemove, bss.constructor)
		bss.wildcard.setOrNil(updatedWildcard)
		for _, concrete := range concretesToAdd {
			concrete := concrete
			bss.setConcrete(concrete.GetSubjectId(), &concrete)
		}
		return
	}

	if existing, ok := bss.concrete[toRemove.GetSubjectId()]; ok {
		bss.setConcrete(toRemove.GetSubjectId(), subtractConcreteFromConcrete(existing, toRemove, bss.constructor))
	}

	wildcard, ok := bss.wildcard.get()
	if ok {
		bss.wildcard.setOrNil(subtractConcreteFromWildcard(wildcard, toRemove, bss.constructor))
	}
}

// SubtractAll subtracts the other set of subjects from this set of subtracts, modifying this
// set *in place*.
func (bss BaseSubjectSet[T]) SubtractAll(other BaseSubjectSet[T]) {
	for _, otherSubject := range other.AsSlice() {
		bss.Subtract(otherSubject)
	}
}

// MustIntersectionDifference performs an intersection between this set and the other set, modifying
// this set *in place*.
func (bss BaseSubjectSet[T]) MustIntersectionDifference(other BaseSubjectSet[T]) {
	err := bss.IntersectionDifference(other)
	if err != nil {
		panic(err)
	}
}

// IntersectionDifference performs an intersection between this set and the other set, modifying
// this set *in place*.
func (bss BaseSubjectSet[T]) IntersectionDifference(other BaseSubjectSet[T]) error {
	// Intersect the wildcards of the sets, if any.
	existingWildcard := bss.wildcard.getOrNil()
	otherWildcard := other.wildcard.getOrNil()

	intersection, err := intersectWildcardWithWildcard(existingWildcard, otherWildcard, bss.constructor)
	if err != nil {
		return err
	}

	bss.wildcard.setOrNil(intersection)

	// Intersect the concretes of each set, as well as with the wildcards.
	updatedConcretes := make(map[string]T, len(bss.concrete))

	for _, concreteSubject := range bss.concrete {
		var otherConcreteOrNil *T
		if otherConcrete, ok := other.concrete[concreteSubject.GetSubjectId()]; ok {
			otherConcreteOrNil = &otherConcrete
		}

		concreteIntersected := intersectConcreteWithConcrete(concreteSubject, otherConcreteOrNil, bss.constructor)
		otherWildcardIntersected, err := intersectConcreteWithWildcard(concreteSubject, otherWildcard, bss.constructor)
		if err != nil {
			return err
		}

		result := unionConcreteWithConcrete(concreteIntersected, otherWildcardIntersected, bss.constructor)
		if result != nil {
			updatedConcretes[concreteSubject.GetSubjectId()] = *result
		}
	}

	if existingWildcard != nil {
		for _, otherSubject := range other.concrete {
			existingWildcardIntersect, err := intersectConcreteWithWildcard(otherSubject, existingWildcard, bss.constructor)
			if err != nil {
				return err
			}

			if existingUpdated, ok := updatedConcretes[otherSubject.GetSubjectId()]; ok {
				result := unionConcreteWithConcrete(&existingUpdated, existingWildcardIntersect, bss.constructor)
				updatedConcretes[otherSubject.GetSubjectId()] = *result
			} else if existingWildcardIntersect != nil {
				updatedConcretes[otherSubject.GetSubjectId()] = *existingWildcardIntersect
			}
		}
	}

	clear(bss.concrete)
	maps.Copy(bss.concrete, updatedConcretes)
	return nil
}

// UnionWith adds the given subjects to this set, via a union call.
func (bss BaseSubjectSet[T]) UnionWith(foundSubjects []T) error {
	for _, fs := range foundSubjects {
		err := bss.Add(fs)
		if err != nil {
			return err
		}
	}
	return nil
}

// UnionWithSet performs a union operation between this set and the other set, modifying this
// set *in place*.
func (bss BaseSubjectSet[T]) UnionWithSet(other BaseSubjectSet[T]) error {
	return bss.UnionWith(other.AsSlice())
}

// MustUnionWithSet performs a union operation between this set and the other set, modifying this
// set *in place*.
func (bss BaseSubjectSet[T]) MustUnionWithSet(other BaseSubjectSet[T]) {
	err := bss.UnionWithSet(other)
	if err != nil {
		panic(err)
	}
}

// Get returns the found subject with the given ID in the set, if any.
func (bss BaseSubjectSet[T]) Get(id string) (T, bool) {
	if id == tuple.PublicWildcard {
		return bss.wildcard.get()
	}

	found, ok := bss.concrete[id]
	return found, ok
}

// IsEmpty returns whether the subject set is empty.
func (bss BaseSubjectSet[T]) IsEmpty() bool {
	return bss.wildcard.getOrNil() == nil && len(bss.concrete) == 0
}

// AsSlice returns the contents of the subject set as a slice of found subjects.
func (bss BaseSubjectSet[T]) AsSlice() []T {
	values := maps.Values(bss.concrete)
	if wildcard, ok := bss.wildcard.get(); ok {
		values = append(values, wildcard)
	}
	return values
}

// Clone returns a clone of this subject set. Note that this is a shallow clone.
// NOTE: Should only be used when performance is not a concern.
func (bss BaseSubjectSet[T]) Clone() BaseSubjectSet[T] {
	return BaseSubjectSet[T]{
		constructor: bss.constructor,
		concrete:    maps.Clone(bss.concrete),
		wildcard:    bss.wildcard.clone(),
	}
}

// UnsafeRemoveExact removes the *exact* matching subject, with no wildcard handling.
// This should ONLY be used for testing.
func (bss BaseSubjectSet[T]) UnsafeRemoveExact(foundSubject T) {
	if foundSubject.GetSubjectId() == tuple.PublicWildcard {
		bss.wildcard.clear()
		return
	}

	delete(bss.concrete, foundSubject.GetSubjectId())
}

// WithParentCaveatExpression returns a copy of the subject set with the parent caveat expression applied
// to all members of this set.
func (bss BaseSubjectSet[T]) WithParentCaveatExpression(parentCaveatExpr *core.CaveatExpression) BaseSubjectSet[T] {
	clone := bss.Clone()

	// Apply the parent caveat expression to the wildcard, if any.
	if wildcard, ok := clone.wildcard.get(); ok {
		constructed := bss.constructor(
			tuple.PublicWildcard,
			caveatAnd(parentCaveatExpr, wildcard.GetCaveatExpression()),
			wildcard.GetExcludedSubjects(),
			wildcard,
		)
		clone.wildcard.setOrNil(&constructed)
	}

	// Apply the parent caveat expression to each concrete.
	for subjectID, concrete := range clone.concrete {
		clone.concrete[subjectID] = bss.constructor(
			subjectID,
			caveatAnd(parentCaveatExpr, concrete.GetCaveatExpression()),
			nil,
			concrete,
		)
	}

	return clone
}

// unionWildcardWithWildcard performs a union operation over two wildcards, returning the updated
// wildcard (if any).
func unionWildcardWithWildcard[T Subject[T]](existing *T, adding T, constructor constructor[T]) (*T, error) {
	// If there is no existing wildcard, return the added one.
	if existing == nil {
		return &adding, nil
	}

	// Otherwise, union together the conditionals for the wildcards and *intersect* their exclusion
	// sets.
	existingWildcard := *existing
	expression := shortcircuitedOr(existingWildcard.GetCaveatExpression(), adding.GetCaveatExpression())

	// Exclusion sets are intersected because if an exclusion is missing from one wildcard
	// but not the other, the missing element will be, by definition, in that other wildcard.
	//
	// Examples:
	//
	//	{*} + {*} => {*}
	//	{* - {user:tom}} + {*} => {*}
	//	{* - {user:tom}} + {* - {user:sarah}} => {*}
	//	{* - {user:tom, user:sarah}} + {* - {user:sarah}} => {* - {user:sarah}}
	//	{*}[c1] + {*} => {*}
	//	{*}[c1] + {*}[c2] => {*}[c1 || c2]

	// NOTE: since we're only using concretes here, it is safe to reuse the BaseSubjectSet itself.
	exisingConcreteExclusions := NewBaseSubjectSet(constructor)
	for _, excludedSubject := range existingWildcard.GetExcludedSubjects() {
		if excludedSubject.GetSubjectId() == tuple.PublicWildcard {
			return nil, spiceerrors.MustBugf("wildcards are not allowed in exclusions")
		}

		err := exisingConcreteExclusions.Add(excludedSubject)
		if err != nil {
			return nil, err
		}
	}

	foundConcreteExclusions := NewBaseSubjectSet(constructor)
	for _, excludedSubject := range adding.GetExcludedSubjects() {
		if excludedSubject.GetSubjectId() == tuple.PublicWildcard {
			return nil, spiceerrors.MustBugf("wildcards are not allowed in exclusions")
		}

		err := foundConcreteExclusions.Add(excludedSubject)
		if err != nil {
			return nil, err
		}
	}

	err := exisingConcreteExclusions.IntersectionDifference(foundConcreteExclusions)
	if err != nil {
		return nil, err
	}

	constructed := constructor(
		tuple.PublicWildcard,
		expression,
		exisingConcreteExclusions.AsSlice(),
		*existing,
		adding)
	return &constructed, nil
}

// unionWildcardWithConcrete performs a union operation between a wildcard and a concrete subject
// being added to the set, returning the updated wildcard (if applciable).
func unionWildcardWithConcrete[T Subject[T]](existing *T, adding T, constructor constructor[T]) *T {
	// If there is no existing wildcard, nothing more to do.
	if existing == nil {
		return nil
	}

	// If the concrete is in the exclusion set, remove it if not conditional. Otherwise, mark
	// it as conditional.
	//
	// Examples:
	//  {*} | {user:tom} => {*} (and user:tom in the concrete)
	//  {* - {user:tom}} | {user:tom} => {*} (and user:tom in the concrete)
	//  {* - {user:tom}[c1]} | {user:tom}[c2] => {* - {user:tom}[c1 && !c2]} (and user:tom in the concrete)
	existingWildcard := *existing
	updatedExclusions := make([]T, 0, len(existingWildcard.GetExcludedSubjects()))
	for _, existingExclusion := range existingWildcard.GetExcludedSubjects() {
		if existingExclusion.GetSubjectId() == adding.GetSubjectId() {
			// If the conditional on the concrete is empty, then the concrete is always present, so
			// we remove the exclusion entirely.
			if adding.GetCaveatExpression() == nil {
				continue
			}

			// Otherwise, the conditional expression for the new exclusion is the existing expression &&
			// the *inversion* of the concrete's expression, as the exclusion will only apply if the
			// concrete subject is not present and the exclusion's expression is true.
			exclusionConditionalExpression := caveatAnd(
				existingExclusion.GetCaveatExpression(),
				caveatInvert(adding.GetCaveatExpression()),
			)

			updatedExclusions = append(updatedExclusions, constructor(
				adding.GetSubjectId(),
				exclusionConditionalExpression,
				nil,
				existingExclusion,
				adding),
			)
		} else {
			updatedExclusions = append(updatedExclusions, existingExclusion)
		}
	}

	constructed := constructor(
		tuple.PublicWildcard,
		existingWildcard.GetCaveatExpression(),
		updatedExclusions,
		existingWildcard)
	return &constructed
}

// unionConcreteWithConcrete performs a union operation between two concrete subjects and returns
// the concrete subject produced, if any.
func unionConcreteWithConcrete[T Subject[T]](existing *T, adding *T, constructor constructor[T]) *T {
	// Check for union with other concretes.
	if existing == nil {
		return adding
	}

	if adding == nil {
		return existing
	}

	existingConcrete := *existing
	addingConcrete := *adding

	// A union of a concrete subjects has the conditionals of each concrete merged.
	constructed := constructor(
		existingConcrete.GetSubjectId(),
		shortcircuitedOr(
			existingConcrete.GetCaveatExpression(),
			addingConcrete.GetCaveatExpression(),
		),
		nil,
		existingConcrete, addingConcrete)
	return &constructed
}

// subtractWildcardFromWildcard performs a subtraction operation of wildcard from another, returning
// the updated wildcard (if any), as well as any concrete subjects produced by the subtraction
// operation due to exclusions.
func subtractWildcardFromWildcard[T Subject[T]](existing *T, toRemove T, constructor constructor[T]) (*T, []T) {
	// If there is no existing wildcard, nothing more to do.
	if existing == nil {
		return nil, nil
	}

	// If there is no condition on the wildcard and the new wildcard has no exclusions, then this wildcard goes away.
	// Example: {*} - {*} => {}
	if toRemove.GetCaveatExpression() == nil && len(toRemove.GetExcludedSubjects()) == 0 {
		return nil, nil
	}

	// Otherwise, we construct a new wildcard and return any concrete subjects that might result from this subtraction.
	existingWildcard := *existing
	existingExclusions := exclusionsMapFor(existingWildcard)

	// Calculate the exclusions which turn into concrete subjects.
	// This occurs when a wildcard with exclusions is subtracted from a wildcard
	// (with, or without *matching* exclusions).
	//
	// Example:
	// Given the two wildcards `* - {user:sarah}` and `* - {user:tom, user:amy, user:sarah}`,
	// the resulting concrete subjects are {user:tom, user:amy} because the first set contains
	// `tom` and `amy` (but not `sarah`) and the second set contains all three.
	resultingConcreteSubjects := make([]T, 0, len(toRemove.GetExcludedSubjects()))
	for _, excludedSubject := range toRemove.GetExcludedSubjects() {
		if existingExclusion, isExistingExclusion := existingExclusions[excludedSubject.GetSubjectId()]; !isExistingExclusion || existingExclusion.GetCaveatExpression() != nil {
			// The conditional expression for the now-concrete subject type is the conditional on the provided exclusion
			// itself.
			//
			// As an example, subtracting the wildcards
			// {*[caveat1] - {user:tom}}
			// -
			// {*[caveat3] - {user:sarah[caveat4]}}
			//
			// the resulting expression to produce a *concrete* `user:sarah` is
			// `caveat1 && caveat3 && caveat4`, because the concrete subject only appears if the first
			// wildcard applies, the *second* wildcard applies and its exclusion applies.
			exclusionConditionalExpression := caveatAnd(
				caveatAnd(
					existingWildcard.GetCaveatExpression(),
					toRemove.GetCaveatExpression(),
				),
				excludedSubject.GetCaveatExpression(),
			)

			// If there is an existing exclusion, then its caveat expression is added as well, but inverted.
			//
			// As an example, subtracting the wildcards
			// {*[caveat1] - {user:tom[caveat2]}}
			// -
			// {*[caveat3] - {user:sarah[caveat4]}}
			//
			// the resulting expression to produce a *concrete* `user:sarah` is
			// `caveat1 && !caveat2 && caveat3 && caveat4`, because the concrete subject only appears
			// if the first wildcard applies, the *second* wildcard applies, the first exclusion
			// does *not* apply (ensuring the concrete is in the first wildcard) and the second exclusion
			// *does* apply (ensuring it is not in the second wildcard).
			if existingExclusion.GetCaveatExpression() != nil {
				exclusionConditionalExpression = caveatAnd(
					caveatAnd(
						caveatAnd(
							existingWildcard.GetCaveatExpression(),
							toRemove.GetCaveatExpression(),
						),
						caveatInvert(existingExclusion.GetCaveatExpression()),
					),
					excludedSubject.GetCaveatExpression(),
				)
			}

			resultingConcreteSubjects = append(resultingConcreteSubjects, constructor(
				excludedSubject.GetSubjectId(),
				exclusionConditionalExpression,
				nil, excludedSubject))
		}
	}

	// Create the combined conditional: the wildcard can only exist when it is present and the other wildcard is not.
	combinedConditionalExpression := caveatAnd(existingWildcard.GetCaveatExpression(), caveatInvert(toRemove.GetCaveatExpression()))
	if combinedConditionalExpression != nil {
		constructed := constructor(
			tuple.PublicWildcard,
			combinedConditionalExpression,
			existingWildcard.GetExcludedSubjects(),
			existingWildcard,
			toRemove)
		return &constructed, resultingConcreteSubjects
	}

	return nil, resultingConcreteSubjects
}

// subtractWildcardFromConcrete subtracts a wildcard from a concrete element, returning the updated
// concrete subject, if any.
func subtractWildcardFromConcrete[T Subject[T]](existingConcrete T, wildcardToRemove T, constructor constructor[T]) *T {
	// Subtraction of a wildcard removes *all* elements of the concrete set, except those that
	// are found in the excluded list. If the wildcard *itself* is conditional, then instead of
	// items being removed, they are made conditional on the inversion of the wildcard's expression,
	// and the exclusion's conditional, if any.
	//
	// Examples:
	//  {user:sarah, user:tom} - {*} => {}
	//  {user:sarah, user:tom} - {*[somecaveat]} => {user:sarah[!somecaveat], user:tom[!somecaveat]}
	//  {user:sarah, user:tom} - {* - {user:tom}} => {user:tom}
	//  {user:sarah, user:tom} - {*[somecaveat] - {user:tom}} => {user:sarah[!somecaveat], user:tom}
	//  {user:sarah, user:tom} - {* - {user:tom[c2]}}[somecaveat] => {user:sarah[!somecaveat], user:tom[c2]}
	//  {user:sarah[c1], user:tom} - {*[somecaveat] - {user:tom}} => {user:sarah[c1 && !somecaveat], user:tom}
	exclusions := exclusionsMapFor(wildcardToRemove)
	exclusion, isExcluded := exclusions[existingConcrete.GetSubjectId()]
	if !isExcluded {
		// If the subject was not excluded within the wildcard, it is either removed directly
		// (in the case where the wildcard is not conditional), or has its condition updated to
		// reflect that it is only present when the condition for the wildcard is *false*.
		if wildcardToRemove.GetCaveatExpression() == nil {
			return nil
		}

		constructed := constructor(
			existingConcrete.GetSubjectId(),
			caveatAnd(existingConcrete.GetCaveatExpression(), caveatInvert(wildcardToRemove.GetCaveatExpression())),
			nil,
			existingConcrete)
		return &constructed
	}

	// If the exclusion is not conditional, then the subject is always present.
	if exclusion.GetCaveatExpression() == nil {
		return &existingConcrete
	}

	// The conditional of the exclusion is that of the exclusion itself OR the caveatInverted case of
	// the wildcard, which would mean the wildcard itself does not apply.
	exclusionConditional := caveatOr(caveatInvert(wildcardToRemove.GetCaveatExpression()), exclusion.GetCaveatExpression())

	constructed := constructor(
		existingConcrete.GetSubjectId(),
		caveatAnd(existingConcrete.GetCaveatExpression(), exclusionConditional),
		nil,
		existingConcrete)
	return &constructed
}

// subtractConcreteFromConcrete subtracts a concrete subject from another concrete subject.
func subtractConcreteFromConcrete[T Subject[T]](existingConcrete T, toRemove T, constructor constructor[T]) *T {
	// Subtraction of a concrete type removes the entry from the concrete list
	// *unless* the subtraction is conditional, in which case the conditional is updated
	// to remove the element when it is true.
	//
	// Examples:
	//  {user:sarah} - {user:tom} => {user:sarah}
	//  {user:tom} - {user:tom} => {}
	//  {user:tom[c1]} - {user:tom} => {user:tom}
	//  {user:tom} - {user:tom[c2]} => {user:tom[!c2]}
	//  {user:tom[c1]} - {user:tom[c2]} => {user:tom[c1 && !c2]}
	if toRemove.GetCaveatExpression() == nil {
		return nil
	}

	// Otherwise, adjust the conditional of the existing item to remove it if it is true.
	expression := caveatAnd(
		existingConcrete.GetCaveatExpression(),
		caveatInvert(
			toRemove.GetCaveatExpression(),
		),
	)

	constructed := constructor(
		existingConcrete.GetSubjectId(),
		expression,
		nil,
		existingConcrete, toRemove)
	return &constructed
}

// subtractConcreteFromWildcard subtracts a concrete element from a wildcard.
func subtractConcreteFromWildcard[T Subject[T]](wildcard T, concreteToRemove T, constructor constructor[T]) *T {
	// Subtracting a concrete type from a wildcard adds the concrete to the exclusions for the wildcard.
	// Examples:
	//  {*} - {user:tom} => {* - {user:tom}}
	//  {*} - {user:tom[c1]} => {* - {user:tom[c1]}}
	//  {* - {user:tom[c1]}} - {user:tom} => {* - {user:tom}}
	//  {* - {user:tom[c1]}} - {user:tom[c2]} => {* - {user:tom[c1 || c2]}}
	updatedExclusions := make([]T, 0, len(wildcard.GetExcludedSubjects())+1)
	wasFound := false
	for _, existingExclusion := range wildcard.GetExcludedSubjects() {
		if existingExclusion.GetSubjectId() == concreteToRemove.GetSubjectId() {
			// The conditional expression for the exclusion is a combination on the existing exclusion or
			// the new expression. The caveat is short-circuited here because if either the exclusion or
			// the concrete is non-caveated, then the whole exclusion is non-caveated.
			exclusionConditionalExpression := shortcircuitedOr(
				existingExclusion.GetCaveatExpression(),
				concreteToRemove.GetCaveatExpression(),
			)

			updatedExclusions = append(updatedExclusions, constructor(
				concreteToRemove.GetSubjectId(),
				exclusionConditionalExpression,
				nil,
				existingExclusion,
				concreteToRemove),
			)
			wasFound = true
		} else {
			updatedExclusions = append(updatedExclusions, existingExclusion)
		}
	}

	if !wasFound {
		updatedExclusions = append(updatedExclusions, concreteToRemove)
	}

	constructed := constructor(
		tuple.PublicWildcard,
		wildcard.GetCaveatExpression(),
		updatedExclusions,
		wildcard)
	return &constructed
}

// intersectConcreteWithConcrete performs intersection between two concrete subjects, returning the
// resolved concrete subject, if any.
func intersectConcreteWithConcrete[T Subject[T]](first T, second *T, constructor constructor[T]) *T {
	// Intersection of concrete subjects is a standard intersection operation, where subjects
	// must be in both sets, with a combination of the two elements into one for conditionals.
	// Otherwise, `and` together conditionals.
	if second == nil {
		return nil
	}

	secondConcrete := *second
	constructed := constructor(
		first.GetSubjectId(),
		caveatAnd(first.GetCaveatExpression(), secondConcrete.GetCaveatExpression()),
		nil,
		first,
		secondConcrete)

	return &constructed
}

// intersectWildcardWithWildcard performs intersection between two wildcards, returning the resolved
// wildcard subject, if any.
func intersectWildcardWithWildcard[T Subject[T]](first *T, second *T, constructor constructor[T]) (*T, error) {
	// If either wildcard does not exist, then no wildcard is placed into the resulting set.
	if first == nil || second == nil {
		return nil, nil
	}

	// If the other wildcard exists, then the intersection between the two wildcards is an && of
	// their conditionals, and a *union* of their exclusions.
	firstWildcard := *first
	secondWildcard := *second

	concreteExclusions := NewBaseSubjectSet(constructor)
	for _, excludedSubject := range firstWildcard.GetExcludedSubjects() {
		if excludedSubject.GetSubjectId() == tuple.PublicWildcard {
			return nil, spiceerrors.MustBugf("wildcards are not allowed in exclusions")
		}

		err := concreteExclusions.Add(excludedSubject)
		if err != nil {
			return nil, err
		}
	}

	for _, excludedSubject := range secondWildcard.GetExcludedSubjects() {
		if excludedSubject.GetSubjectId() == tuple.PublicWildcard {
			return nil, spiceerrors.MustBugf("wildcards are not allowed in exclusions")
		}

		err := concreteExclusions.Add(excludedSubject)
		if err != nil {
			return nil, err
		}
	}

	constructed := constructor(
		tuple.PublicWildcard,
		caveatAnd(firstWildcard.GetCaveatExpression(), secondWildcard.GetCaveatExpression()),
		concreteExclusions.AsSlice(),
		firstWildcard,
		secondWildcard)
	return &constructed, nil
}

// intersectConcreteWithWildcard performs intersection between a concrete subject and a wildcard
// subject, returning the concrete, if any.
func intersectConcreteWithWildcard[T Subject[T]](concrete T, wildcard *T, constructor constructor[T]) (*T, error) {
	// If no wildcard exists, then the concrete cannot exist (for this branch)
	if wildcard == nil {
		return nil, nil
	}

	wildcardToIntersect := *wildcard
	exclusionsMap := exclusionsMapFor(wildcardToIntersect)
	exclusion, isExcluded := exclusionsMap[concrete.GetSubjectId()]

	// Cases:
	// - The concrete subject is not excluded and the wildcard is not conditional => concrete is kept
	// - The concrete subject is excluded and the wildcard is not conditional but the exclusion *is* conditional => concrete is made conditional
	// - The concrete subject is excluded and the wildcard is not conditional => concrete is removed
	// - The concrete subject is not excluded but the wildcard is conditional => concrete is kept, but made conditional
	// - The concrete subject is excluded and the wildcard is conditional => concrete is removed, since it is always excluded
	// - The concrete subject is excluded and the wildcard is conditional and the exclusion is conditional => combined conditional
	switch {
	case !isExcluded && wildcardToIntersect.GetCaveatExpression() == nil:
		// If the concrete is not excluded and the wildcard conditional is empty, then the concrete is always found.
		// Example: {user:tom} & {*} => {user:tom}
		return &concrete, nil

	case !isExcluded && wildcardToIntersect.GetCaveatExpression() != nil:
		// The concrete subject is only included if the wildcard's caveat is true.
		// Example: {user:tom}[acaveat] & {* - user:tom}[somecaveat] => {user:tom}[acaveat && somecaveat]
		constructed := constructor(
			concrete.GetSubjectId(),
			caveatAnd(concrete.GetCaveatExpression(), wildcardToIntersect.GetCaveatExpression()),
			nil,
			concrete,
			wildcardToIntersect)
		return &constructed, nil

	case isExcluded && exclusion.GetCaveatExpression() == nil:
		// If the concrete is excluded and the exclusion is not conditional, then the concrete can never show up,
		// regardless of whether the wildcard is conditional.
		// Example: {user:tom} & {* - user:tom}[somecaveat] => {}
		return nil, nil

	case isExcluded && exclusion.GetCaveatExpression() != nil:
		// NOTE: whether the wildcard is itself conditional or not is handled within the expression combinators below.
		// The concrete subject is included if the wildcard's caveat is true and the exclusion's caveat is *false*.
		// Example: {user:tom}[acaveat] & {* - user:tom[ecaveat]}[wcaveat] => {user:tom[acaveat && wcaveat && !ecaveat]}
		constructed := constructor(
			concrete.GetSubjectId(),
			caveatAnd(
				concrete.GetCaveatExpression(),
				caveatAnd(
					wildcardToIntersect.GetCaveatExpression(),
					caveatInvert(exclusion.GetCaveatExpression()),
				)),
			nil,
			concrete,
			wildcardToIntersect,
			exclusion)
		return &constructed, nil

	default:
		return nil, spiceerrors.MustBugf("unhandled case in basesubjectset intersectConcreteWithWildcard: %v & %v", concrete, wildcardToIntersect)
	}
}

type handle[T any] struct {
	value *T
}

func newHandle[T any]() *handle[T] {
	return &handle[T]{}
}

func (h *handle[T]) getOrNil() *T {
	return h.value
}

func (h *handle[T]) setOrNil(value *T) {
	h.value = value
}

func (h *handle[T]) get() (T, bool) {
	if h.value != nil {
		return *h.value, true
	}

	return *new(T), false
}

func (h *handle[T]) clear() {
	h.value = nil
}

func (h *handle[T]) clone() *handle[T] {
	return &handle[T]{
		value: h.value,
	}
}

// exclusionsMapFor creates a map of all the exclusions on a wildcard, by subject ID.
func exclusionsMapFor[T Subject[T]](wildcard T) map[string]T {
	exclusions := make(map[string]T, len(wildcard.GetExcludedSubjects()))
	for _, excludedSubject := range wildcard.GetExcludedSubjects() {
		exclusions[excludedSubject.GetSubjectId()] = excludedSubject
	}
	return exclusions
}
