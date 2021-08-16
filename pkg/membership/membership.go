package membership

import (
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	"github.com/authzed/spicedb/pkg/tuple"
)

// MembershipSet represents the set of membership for one or more ONRs, based on expansion
// trees.
type MembershipSet struct {
	// objectsAndRelations is a map from an ONR (as a string) to the subjects found for that ONR.
	objectsAndRelations map[string]FoundSubjects
}

// SubjectsByONR returns a map from ONR (as a string) to the FoundSubjects for that ONR.
func (ms *MembershipSet) SubjectsByONR() map[string]FoundSubjects {
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
	onrString := tuple.StringONR(subject)
	found, ok := fs.subjects[onrString]
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
func NewMembershipSet() *MembershipSet {
	return &MembershipSet{
		objectsAndRelations: map[string]FoundSubjects{},
	}
}

// AddExpansion adds the expansion of an ONR to the membership set. Returns false if the ONR was already added.
//
// NOTE: The expansion tree *should* be the fully recursive expansion.
func (ms *MembershipSet) AddExpansion(onr *v0.ObjectAndRelation, expansion *v0.RelationTupleTreeNode) (FoundSubjects, bool, error) {
	onrString := tuple.StringONR(onr)
	existing, ok := ms.objectsAndRelations[onrString]
	if ok {
		return existing, false, nil
	}

	foundSubjectsMap := map[string]FoundSubject{}
	err := populateFoundSubjects(foundSubjectsMap, onr, expansion)
	if err != nil {
		return FoundSubjects{}, false, err
	}

	fs := FoundSubjects{
		subjects: foundSubjectsMap,
	}
	ms.objectsAndRelations[onrString] = fs
	return fs, true, nil
}

func populateFoundSubjects(foundSubjectsMap map[string]FoundSubject, rootONR *v0.ObjectAndRelation, treeNode *v0.RelationTupleTreeNode) error {
	relationship := rootONR
	if treeNode.Expanded != nil {
		relationship = treeNode.Expanded
	}

	switch typed := treeNode.NodeType.(type) {
	case *v0.RelationTupleTreeNode_IntermediateNode:
		switch typed.IntermediateNode.Operation {
		case v0.SetOperationUserset_UNION:
			for _, child := range typed.IntermediateNode.ChildNodes {
				err := populateFoundSubjects(foundSubjectsMap, rootONR, child)
				if err != nil {
					return err
				}
			}

		case v0.SetOperationUserset_INTERSECTION:
			if len(typed.IntermediateNode.ChildNodes) == 0 {
				return fmt.Errorf("Found intersection with no children")
			}

			fsm := map[string]FoundSubject{}
			populateFoundSubjects(fsm, rootONR, typed.IntermediateNode.ChildNodes[0])

			subjectset := newSubjectSet()
			subjectset.union(fsm)

			for _, child := range typed.IntermediateNode.ChildNodes[1:] {
				fsm := map[string]FoundSubject{}
				populateFoundSubjects(fsm, rootONR, child)
				subjectset.intersect(fsm)
			}

			subjectset.populate(foundSubjectsMap)

		case v0.SetOperationUserset_EXCLUSION:
			if len(typed.IntermediateNode.ChildNodes) == 0 {
				return fmt.Errorf("Found exclusion with no children")
			}

			fsm := map[string]FoundSubject{}
			populateFoundSubjects(fsm, rootONR, typed.IntermediateNode.ChildNodes[0])

			subjectset := newSubjectSet()
			subjectset.union(fsm)

			for _, child := range typed.IntermediateNode.ChildNodes[1:] {
				fsm := map[string]FoundSubject{}
				populateFoundSubjects(fsm, rootONR, child)
				subjectset.exclude(fsm)
			}

			subjectset.populate(foundSubjectsMap)

		default:
			panic("unknown expand operation")
		}

	case *v0.RelationTupleTreeNode_LeafNode:
		for _, user := range typed.LeafNode.Users {
			subjectONRString := tuple.StringONR(user.GetUserset())
			_, ok := foundSubjectsMap[subjectONRString]
			if !ok {
				foundSubjectsMap[subjectONRString] = FoundSubject{
					subject:       user.GetUserset(),
					relationships: tuple.NewONRSet(),
				}
			}

			foundSubjectsMap[subjectONRString].relationships.Add(relationship)
		}
	default:
		panic("unknown TreeNode type")
	}

	return nil
}

type subjectSet struct {
	subjectsMap map[string]FoundSubject
}

func newSubjectSet() *subjectSet {
	return &subjectSet{
		subjectsMap: map[string]FoundSubject{},
	}
}

func (ss *subjectSet) populate(outgoingSubjectsMap map[string]FoundSubject) {
	for key, fs := range ss.subjectsMap {
		existing, ok := outgoingSubjectsMap[key]
		if ok {
			existing.relationships.UpdateFrom(fs.relationships)
		} else {
			outgoingSubjectsMap[key] = fs
		}
	}
}

func (ss *subjectSet) union(subjectsMap map[string]FoundSubject) {
	for key, fs := range subjectsMap {
		existing, ok := ss.subjectsMap[key]
		if ok {
			existing.relationships.UpdateFrom(fs.relationships)
		} else {
			ss.subjectsMap[key] = fs
		}
	}
}

func (ss *subjectSet) intersect(subjectsMap map[string]FoundSubject) {
	for key, fs := range ss.subjectsMap {
		other, ok := subjectsMap[key]
		if ok {
			fs.relationships.UpdateFrom(other.relationships)
		} else {
			delete(ss.subjectsMap, key)
		}
	}
}

func (ss *subjectSet) exclude(subjectsMap map[string]FoundSubject) {
	for key := range ss.subjectsMap {
		_, ok := subjectsMap[key]
		if ok {
			delete(ss.subjectsMap, key)
		}
	}
}
