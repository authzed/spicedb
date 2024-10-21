package developmentmembership

import (
	"sort"
	"strings"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/tuple"
)

// NewFoundSubject creates a new FoundSubject for a subject and a set of its resources.
func NewFoundSubject(subject *core.DirectSubject, resources ...tuple.ObjectAndRelation) FoundSubject {
	return FoundSubject{tuple.FromCoreObjectAndRelation(subject.Subject), nil, subject.CaveatExpression, NewONRSet(resources...)}
}

// FoundSubject contains a single found subject and all the relationships in which that subject
// is a member which were found via the ONRs expansion.
type FoundSubject struct {
	// subject is the subject found.
	subject tuple.ObjectAndRelation

	// excludedSubjects are any subjects excluded. Only should be set if subject is a wildcard.
	excludedSubjects []FoundSubject

	// caveatExpression is the conditional expression on the found subject.
	caveatExpression *core.CaveatExpression

	// resources are the resources under which the subject lives that informed the locating
	// of this subject for the root ONR.
	resources ONRSet
}

// GetSubjectId is named to match the Subject interface for the BaseSubjectSet.
//
//nolint:all
func (fs FoundSubject) GetSubjectId() string {
	return fs.subject.ObjectID
}

func (fs FoundSubject) GetCaveatExpression() *core.CaveatExpression {
	return fs.caveatExpression
}

func (fs FoundSubject) GetExcludedSubjects() []FoundSubject {
	return fs.excludedSubjects
}

// Subject returns the Subject of the FoundSubject.
func (fs FoundSubject) Subject() tuple.ObjectAndRelation {
	return fs.subject
}

// WildcardType returns the object type for the wildcard subject, if this is a wildcard subject.
func (fs FoundSubject) WildcardType() (string, bool) {
	if fs.subject.ObjectID == tuple.PublicWildcard {
		return fs.subject.ObjectType, true
	}

	return "", false
}

// ExcludedSubjectsFromWildcard returns those subjects excluded from the wildcard subject.
// If not a wildcard subject, returns false.
func (fs FoundSubject) ExcludedSubjectsFromWildcard() ([]FoundSubject, bool) {
	if fs.subject.ObjectID == tuple.PublicWildcard {
		return fs.excludedSubjects, true
	}

	return nil, false
}

func (fs FoundSubject) excludedSubjectStrings() []string {
	excludedStrings := make([]string, 0, len(fs.excludedSubjects))
	for _, excludedSubject := range fs.excludedSubjects {
		excludedSubjectString := tuple.StringONR(excludedSubject.subject)
		if excludedSubject.GetCaveatExpression() != nil {
			excludedSubjectString += "[...]"
		}
		excludedStrings = append(excludedStrings, excludedSubjectString)
	}

	sort.Strings(excludedStrings)
	return excludedStrings
}

// ToValidationString returns the FoundSubject in a format that is consumable by the validationfile
// package.
func (fs FoundSubject) ToValidationString() string {
	onrString := tuple.StringONR(fs.Subject())
	validationString := onrString
	if fs.caveatExpression != nil {
		validationString = validationString + "[...]"
	}

	excluded, isWildcard := fs.ExcludedSubjectsFromWildcard()
	if isWildcard && len(excluded) > 0 {
		validationString = validationString + " - {" + strings.Join(fs.excludedSubjectStrings(), ", ") + "}"
	}

	return validationString
}

func (fs FoundSubject) String() string {
	return fs.ToValidationString()
}

// ParentResources returns all the resources in which the subject was found as per the expand.
func (fs FoundSubject) ParentResources() []tuple.ObjectAndRelation {
	return fs.resources.AsSlice()
}

// FoundSubjects contains the subjects found for a specific ONR.
type FoundSubjects struct {
	// subjects is a map from the Subject ONR (as a string) to the FoundSubject information.
	subjects *TrackingSubjectSet
}

// ListFound returns a slice of all the FoundSubject's.
func (fs FoundSubjects) ListFound() []FoundSubject {
	return fs.subjects.ToSlice()
}

// LookupSubject returns the FoundSubject for a matching subject, if any.
func (fs FoundSubjects) LookupSubject(subject tuple.ObjectAndRelation) (FoundSubject, bool) {
	return fs.subjects.Get(subject)
}
