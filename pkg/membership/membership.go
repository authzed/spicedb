package membership

import (
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	//"github.com/authzed/spicedb/pkg/graph"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Set represents the set of membership for one or more ONRs, based on expansion
// trees.
type Set struct {
	// objectsAndRelations is a map from an ONR (as a string) to the subjects found for that ONR.
	objectsAndRelations map[string]FoundSubjects
}

// SubjectsByONR returns a map from ONR (as a string) to the FoundSubjects for that ONR.
func (ms *Set) SubjectsByONR() map[string]FoundSubjects {
	return ms.objectsAndRelations
}

// FoundSubjects contains the subjects found for a specific ONR.
type FoundSubjects struct {
	// subjects is a map from the Subject ONR (as a string) to the FoundSubject information.
	subjects map[string]FoundSubject
}

// ListFound returns a slice of all the FoundSubject's.
func (fs FoundSubjects) ListFound() []FoundSubject {
	found := []FoundSubject{}
	for _, sub := range fs.subjects {
		found = append(found, sub)
	}
	return found
}

// LookupSubject returns the FoundSubject for a matching subject, if any.
func (fs FoundSubjects) LookupSubject(subject *v0.ObjectAndRelation) (FoundSubject, bool) {
	found, ok := fs.subjects[toKey(subject)]
	return found, ok
}

// FoundSubject contains a single found subject and all the relationships in which that subject
// is a member which were found via the ONRs expansion.
type FoundSubject struct {
	// subject is the subject found.
	subject *v0.ObjectAndRelation

	// relations are the relations under which the subject lives that informed the locating
	// of this subject for the root ONR.
	relationships *tuple.ONRSet
}

// Subject returns the Subject of the FoundSubject.
func (fs FoundSubject) Subject() *v0.ObjectAndRelation {
	return fs.subject
}

// Relationships returns all the relationships in which the subject was found as per the expand.
func (fs FoundSubject) Relationships() []*v0.ObjectAndRelation {
	return fs.relationships.AsSlice()
}

// NewMembershipSet constructs a new membership set.
//
// NOTE: This is designed solely for the developer API and should *not* be used in any performance
// sensitive code.
func NewMembershipSet() *Set {
	return &Set{
		objectsAndRelations: map[string]FoundSubjects{},
	}
}

// AddExpansion adds the expansion of an ONR to the membership set. Returns false if the ONR was already added.
//
// NOTE: The expansion tree *should* be the fully recursive expansion.
func (ms *Set) AddExpansion(onr *v0.ObjectAndRelation, expansion *v0.RelationTupleTreeNode) (FoundSubjects, bool, error) {
	onrString := tuple.StringONR(onr)
	existing, ok := ms.objectsAndRelations[onrString]
	if ok {
		return existing, false, nil
	}

	tss, err := populateFoundSubjects(onr, expansion)
	if err != nil {
		return FoundSubjects{}, false, err
	}

	fs := tss.toFoundSubjects()
	ms.objectsAndRelations[onrString] = fs
	return fs, true, nil
}

func populateFoundSubjects(rootONR *v0.ObjectAndRelation, treeNode *v0.RelationTupleTreeNode) (trackingSubjectSet, error) {
	resource := rootONR
	if treeNode.Expanded != nil {
		resource = treeNode.Expanded
	}

	switch typed := treeNode.NodeType.(type) {
	case *v0.RelationTupleTreeNode_IntermediateNode:
		switch typed.IntermediateNode.Operation {
		case v0.SetOperationUserset_UNION:
			toReturn := newTrackingSubjectSet()
			for _, child := range typed.IntermediateNode.ChildNodes {
				tss, err := populateFoundSubjects(resource, child)
				if err != nil {
					return nil, err
				}

				toReturn.addFrom(tss)
			}
			return toReturn, nil

		case v0.SetOperationUserset_INTERSECTION:
			if len(typed.IntermediateNode.ChildNodes) == 0 {
				return nil, fmt.Errorf("Found intersection with no children")
			}

			firstChildSet, err := populateFoundSubjects(rootONR, typed.IntermediateNode.ChildNodes[0])
			if err != nil {
				return nil, err
			}

			toReturn := newTrackingSubjectSet()
			toReturn.addFrom(firstChildSet)

			for _, child := range typed.IntermediateNode.ChildNodes[1:] {
				childSet, err := populateFoundSubjects(rootONR, child)
				if err != nil {
					return nil, err
				}
				toReturn = toReturn.intersect(childSet)
			}
			return toReturn, nil

		case v0.SetOperationUserset_EXCLUSION:
			if len(typed.IntermediateNode.ChildNodes) == 0 {
				return nil, fmt.Errorf("Found exclusion with no children")
			}

			firstChildSet, err := populateFoundSubjects(rootONR, typed.IntermediateNode.ChildNodes[0])
			if err != nil {
				return nil, err
			}

			toReturn := newTrackingSubjectSet()
			toReturn.addFrom(firstChildSet)

			for _, child := range typed.IntermediateNode.ChildNodes[1:] {
				childSet, err := populateFoundSubjects(rootONR, child)
				if err != nil {
					return nil, err
				}
				toReturn = toReturn.exclude(childSet)
			}

			return toReturn, nil

		default:
			panic("unknown expand operation")
		}

	case *v0.RelationTupleTreeNode_LeafNode:
		toReturn := newTrackingSubjectSet()
		for _, user := range typed.LeafNode.Users {
			fs := FoundSubject{
				subject:       user.GetUserset(),
				relationships: tuple.NewONRSet(),
			}
			toReturn.add(fs)
			fs.relationships.Add(resource)
		}
		return toReturn, nil

	default:
		panic("unknown TreeNode type")
	}
}

// TODO(jschorr): Combine with SubjectSet in graph package once we're on a Go version with
// stable generics.
func isWildcard(subject *v0.ObjectAndRelation) bool {
	return subject.ObjectId == tuple.PublicWildcard
}

type trackingSubjectSet map[string]FoundSubject

func newTrackingSubjectSet(subjects ...FoundSubject) trackingSubjectSet {
	var toReturn trackingSubjectSet = make(map[string]FoundSubject)
	toReturn.add(subjects...)
	return toReturn
}

func (tss trackingSubjectSet) addFrom(otherSet trackingSubjectSet) {
	for _, value := range otherSet {
		tss.add(value)
	}
}

func (tss trackingSubjectSet) removeFrom(otherSet trackingSubjectSet) {
	for _, otherSAR := range otherSet {
		tss.remove(otherSAR.subject)
	}
}

func (tss trackingSubjectSet) add(subjectsAndResources ...FoundSubject) {
	for _, sar := range subjectsAndResources {
		key := toKey(sar.subject)
		existing, ok := tss[key]
		if ok {
			tss[key] = FoundSubject{
				subject:       sar.subject,
				relationships: existing.relationships.Union(sar.relationships),
			}
		} else {
			tss[key] = sar
		}
	}
}

func (tss trackingSubjectSet) addWithResources(subjectsAndResources []FoundSubject, additionalResources *tuple.ONRSet) {
	for _, sar := range subjectsAndResources {
		tss.add(sar)

		key := toKey(sar.subject)
		entry := tss[key]
		tss[key] = FoundSubject{
			subject:       entry.subject,
			relationships: entry.relationships.Union(additionalResources),
		}
	}
}

func (tss trackingSubjectSet) get(subject *v0.ObjectAndRelation) (FoundSubject, bool) {
	found, ok := tss[toKey(subject)]
	return found, ok
}

func (tss trackingSubjectSet) remove(subjects ...*v0.ObjectAndRelation) {
	for _, subject := range subjects {
		delete(tss, toKey(subject))

		// Delete any entries matching the wildcard, if applicable.
		if isWildcard(subject) {
			// remove any matching types.
			for key := range tss {
				current := fromKey(key)
				if current.Namespace == subject.Namespace {
					delete(tss, key)
				}
			}
		}
	}
}

func (tss trackingSubjectSet) withType(objectType string) []FoundSubject {
	toReturn := make([]FoundSubject, 0, len(tss))
	for _, current := range tss {
		if current.subject.Namespace == objectType {
			toReturn = append(toReturn, current)
		}
	}
	return toReturn
}

func (tss trackingSubjectSet) exclude(otherSet trackingSubjectSet) trackingSubjectSet {
	newSet := newTrackingSubjectSet()
	newSet.addFrom(tss)
	newSet.removeFrom(otherSet)
	return newSet
}

func (tss trackingSubjectSet) intersect(otherSet trackingSubjectSet) trackingSubjectSet {
	newSet := newTrackingSubjectSet()
	for _, current := range tss {
		// Add directly if shared by both.
		other, ok := otherSet.get(current.subject)
		if ok {
			// NOTE: we add *both*, to ensure that we get the resources for both.
			newSet.add(current)
			newSet.add(other)
		}

		// If the current is a wildcard, Add any matching.
		if isWildcard(current.subject) {
			newSet.addWithResources(otherSet.withType(current.subject.Namespace), current.relationships)
		}
	}

	for _, current := range otherSet {
		// If the current is a wildcard, Add any matching.
		if isWildcard(current.subject) {
			newSet.addWithResources(tss.withType(current.subject.Namespace), current.relationships)
		}
	}

	return newSet
}

func (tss trackingSubjectSet) toSlice() []FoundSubject {
	toReturn := make([]FoundSubject, 0, len(tss))
	for _, current := range tss {
		toReturn = append(toReturn, current)
	}
	return toReturn
}

func (tss trackingSubjectSet) toFoundSubjects() FoundSubjects {
	return FoundSubjects{tss}
}

func toKey(subject *v0.ObjectAndRelation) string {
	return fmt.Sprintf("%s %s %s", subject.Namespace, subject.ObjectId, subject.Relation)
}

func fromKey(key string) *v0.ObjectAndRelation {
	subject := &v0.ObjectAndRelation{}
	fmt.Sscanf(key, "%s %s %s", &subject.Namespace, &subject.ObjectId, &subject.Relation)
	return subject
}
