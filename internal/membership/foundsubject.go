package membership

import (
	"fmt"
	"sort"
	"strings"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/tuple"
)

// NewFoundSubject creates a new FoundSubject for a subject and a set of its resources.
func NewFoundSubject(subject *core.ObjectAndRelation, resources ...*core.ObjectAndRelation) FoundSubject {
	return FoundSubject{subject, nil, tuple.NewONRSet(resources...)}
}

// FoundSubject contains a single found subject and all the relationships in which that subject
// is a member which were found via the ONRs expansion.
type FoundSubject struct {
	// subject is the subject found.
	subject *core.ObjectAndRelation

	// excludedSubjects are any subjects excluded. Only should be set if subject is a wildcard.
	excludedSubjectIds []string

	// relations are the relations under which the subject lives that informed the locating
	// of this subject for the root ONR.
	relationships *tuple.ONRSet
}

// GetSubjectId is named to match the Subject interface for the BaseSubjectSet.
//
//nolint:all
func (fs FoundSubject) GetSubjectId() string {
	return fs.subject.ObjectId
}

func (fs FoundSubject) GetExcludedSubjectIds() []string {
	return fs.excludedSubjectIds
}

// Subject returns the Subject of the FoundSubject.
func (fs FoundSubject) Subject() *core.ObjectAndRelation {
	return fs.subject
}

// WildcardType returns the object type for the wildcard subject, if this is a wildcard subject.
func (fs FoundSubject) WildcardType() (string, bool) {
	if fs.subject.ObjectId == tuple.PublicWildcard {
		return fs.subject.Namespace, true
	}

	return "", false
}

// ExcludedSubjectsFromWildcard returns those subjects excluded from the wildcard subject.
// If not a wildcard subject, returns false.
func (fs FoundSubject) ExcludedSubjectsFromWildcard() ([]*core.ObjectAndRelation, bool) {
	if fs.subject.ObjectId == tuple.PublicWildcard {
		excludedSubjects := make([]*core.ObjectAndRelation, 0, len(fs.excludedSubjectIds))
		for _, excludedID := range fs.excludedSubjectIds {
			excludedSubjects = append(excludedSubjects, &core.ObjectAndRelation{
				Namespace: fs.subject.Namespace,
				ObjectId:  excludedID,
				Relation:  fs.subject.Relation,
			})
		}

		return excludedSubjects, true
	}

	return []*core.ObjectAndRelation{}, false
}

// Relationships returns all the relationships in which the subject was found as per the expand.
func (fs FoundSubject) Relationships() []*core.ObjectAndRelation {
	return fs.relationships.AsSlice()
}

// ToValidationString returns the FoundSubject in a format that is consumable by the validationfile
// package.
func (fs FoundSubject) ToValidationString() string {
	onrString := tuple.StringONR(fs.Subject())
	excluded, isWildcard := fs.ExcludedSubjectsFromWildcard()
	if isWildcard && len(excluded) > 0 {
		excludedONRStrings := make([]string, 0, len(excluded))
		for _, excludedONR := range excluded {
			excludedONRStrings = append(excludedONRStrings, tuple.StringONR(excludedONR))
		}

		sort.Strings(excludedONRStrings)
		return fmt.Sprintf("%s - {%s}", onrString, strings.Join(excludedONRStrings, ", "))
	}

	return onrString
}

// FoundSubjects contains the subjects found for a specific ONR.
type FoundSubjects struct {
	// subjects is a map from the Subject ONR (as a string) to the FoundSubject information.
	subjects TrackingSubjectSet
}

// ListFound returns a slice of all the FoundSubject's.
func (fs FoundSubjects) ListFound() []FoundSubject {
	return fs.subjects.ToSlice()
}

// LookupSubject returns the FoundSubject for a matching subject, if any.
func (fs FoundSubjects) LookupSubject(subject *core.ObjectAndRelation) (FoundSubject, bool) {
	return fs.subjects.Get(subject)
}
