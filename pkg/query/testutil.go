package query

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/tuple"
)

// createRelation is a helper function to create a relation with the given parameters
func createRelation(resourceType, resourceID, resourceRel, subjectType, subjectID, subjectRel string) tuple.Relationship {
	return tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ObjectAndRelation{
				ObjectType: resourceType,
				ObjectID:   resourceID,
				Relation:   resourceRel,
			},
			Subject: tuple.ObjectAndRelation{
				ObjectType: subjectType,
				ObjectID:   subjectID,
				Relation:   subjectRel,
			},
		},
	}
}

// NewDocumentAccessFixedIterator creates a FixedIterator with typical document access patterns
func NewDocumentAccessFixedIterator() *FixedIterator {
	relations := []tuple.Relationship{
		// Document viewers
		createRelation("document", "doc1", "viewer", "user", "alice", "..."),
		createRelation("document", "doc1", "viewer", "user", "bob", "..."),
		createRelation("document", "doc2", "viewer", "user", "alice", "..."),
		createRelation("document", "doc3", "viewer", "user", "charlie", "..."),

		// Document editors
		createRelation("document", "doc1", "editor", "user", "alice", "..."),
		createRelation("document", "doc2", "editor", "user", "bob", "..."),
		createRelation("document", "doc4", "editor", "user", "diana", "..."),

		// Document owners
		createRelation("document", "doc1", "owner", "user", "alice", "..."),
		createRelation("document", "doc2", "owner", "user", "bob", "..."),
		createRelation("document", "doc3", "owner", "user", "charlie", "..."),
		createRelation("document", "doc4", "owner", "user", "diana", "..."),

		// Group-based access
		createRelation("document", "doc5", "viewer", "group", "engineers", "member"),
		createRelation("document", "doc5", "editor", "group", "leads", "member"),
		createRelation("document", "doc6", "viewer", "group", "all_staff", "member"),
	}

	paths := make([]*Path, len(relations))
	for i, rel := range relations {
		paths[i] = FromRelationship(rel)
	}
	return NewFixedIterator(paths...)
}

// NewFolderHierarchyFixedIterator creates a FixedIterator with folder hierarchy relations
func NewFolderHierarchyFixedIterator() *FixedIterator {
	relations := []tuple.Relationship{
		// Folder structure: root -> projects -> project1, project2
		createRelation("folder", "root", "viewer", "user", "admin", "..."),
		createRelation("folder", "projects", "parent", "folder", "root", "..."),
		createRelation("folder", "project1", "parent", "folder", "projects", "..."),
		createRelation("folder", "project2", "parent", "folder", "projects", "..."),

		// Users with access to different levels
		createRelation("folder", "root", "viewer", "user", "admin", "..."),
		createRelation("folder", "projects", "viewer", "user", "manager", "..."),
		createRelation("folder", "project1", "viewer", "user", "alice", "..."),
		createRelation("folder", "project1", "editor", "user", "bob", "..."),
		createRelation("folder", "project2", "viewer", "user", "charlie", "..."),
		createRelation("folder", "project2", "editor", "user", "diana", "..."),

		// Documents within folders
		createRelation("document", "spec1", "parent", "folder", "project1", "..."),
		createRelation("document", "spec2", "parent", "folder", "project2", "..."),
		createRelation("document", "readme", "parent", "folder", "root", "..."),
	}

	paths := make([]*Path, len(relations))
	for i, rel := range relations {
		paths[i] = FromRelationship(rel)
	}
	return NewFixedIterator(paths...)
}

// NewMultiRoleFixedIterator creates a FixedIterator where users have multiple roles on the same resources
func NewMultiRoleFixedIterator() *FixedIterator {
	relations := []tuple.Relationship{
		// Alice has multiple roles on doc1
		createRelation("document", "doc1", "viewer", "user", "alice", "..."),
		createRelation("document", "doc1", "editor", "user", "alice", "..."),
		createRelation("document", "doc1", "owner", "user", "alice", "..."),

		// Bob has viewer and editor on doc2
		createRelation("document", "doc2", "viewer", "user", "bob", "..."),
		createRelation("document", "doc2", "editor", "user", "bob", "..."),

		// Charlie only has viewer on multiple docs
		createRelation("document", "doc1", "viewer", "user", "charlie", "..."),
		createRelation("document", "doc2", "viewer", "user", "charlie", "..."),
		createRelation("document", "doc3", "viewer", "user", "charlie", "..."),

		// Mixed permissions
		createRelation("document", "doc3", "editor", "user", "diana", "..."),
		createRelation("document", "doc4", "owner", "user", "diana", "..."),
	}

	paths := make([]*Path, len(relations))
	for i, rel := range relations {
		paths[i] = FromRelationship(rel)
	}
	return NewFixedIterator(paths...)
}

// NewGroupMembershipFixedIterator creates a FixedIterator with group membership and nested groups
func NewGroupMembershipFixedIterator() *FixedIterator {
	relations := []tuple.Relationship{
		// Direct group memberships
		createRelation("group", "engineers", "member", "user", "alice", "..."),
		createRelation("group", "engineers", "member", "user", "bob", "..."),
		createRelation("group", "designers", "member", "user", "charlie", "..."),
		createRelation("group", "designers", "member", "user", "diana", "..."),

		// Nested group relationships
		createRelation("group", "all_staff", "member", "group", "engineers", "..."),
		createRelation("group", "all_staff", "member", "group", "designers", "..."),
		createRelation("group", "leads", "member", "user", "alice", "..."),
		createRelation("group", "leads", "member", "user", "charlie", "..."),

		// Resource permissions through groups
		createRelation("document", "handbook", "viewer", "group", "all_staff", "member"),
		createRelation("document", "roadmap", "viewer", "group", "leads", "member"),
		createRelation("document", "code_review", "editor", "group", "engineers", "member"),
		createRelation("document", "design_guide", "editor", "group", "designers", "member"),
	}

	paths := make([]*Path, len(relations))
	for i, rel := range relations {
		paths[i] = FromRelationship(rel)
	}
	return NewFixedIterator(paths...)
}

// NewSingleUserFixedIterator creates a FixedIterator with relations for a single user across multiple resources
func NewSingleUserFixedIterator(userID string) *FixedIterator {
	relations := []tuple.Relationship{
		createRelation("document", "personal1", "owner", "user", userID, "..."),
		createRelation("document", "personal2", "owner", "user", userID, "..."),
		createRelation("document", "shared1", "viewer", "user", userID, "..."),
		createRelation("document", "shared2", "editor", "user", userID, "..."),
		createRelation("folder", "my_folder", "owner", "user", userID, "..."),
		createRelation("folder", "shared_folder", "viewer", "user", userID, "..."),
	}

	paths := make([]*Path, len(relations))
	for i, rel := range relations {
		paths[i] = FromRelationship(rel)
	}
	return NewFixedIterator(paths...)
}

// NewEmptyFixedIterator creates an empty FixedIterator for testing edge cases
func NewEmptyFixedIterator() *FixedIterator {
	return NewFixedIterator()
}

// NewLargeFixedIterator creates a FixedIterator with many relations for performance testing
func NewLargeFixedIterator() *FixedIterator {
	var relations []tuple.Relationship

	// Create 100 users with various permissions on 50 documents
	for i := 0; i < 100; i++ {
		userID := fmt.Sprintf("user%d", i)

		// Each user gets viewer access to multiple documents
		for j := 0; j < 10; j++ {
			docID := fmt.Sprintf("doc%d", j)
			relations = append(relations, createRelation("document", docID, "viewer", "user", userID, "..."))
		}

		// Some users get editor access
		if i%5 == 0 {
			for j := 0; j < 5; j++ {
				docID := fmt.Sprintf("doc%d", j)
				relations = append(relations, createRelation("document", docID, "editor", "user", userID, "..."))
			}
		}

		// Few users get owner access
		if i%10 == 0 {
			docID := fmt.Sprintf("doc%d", i/10)
			relations = append(relations, createRelation("document", docID, "owner", "user", userID, "..."))
		}
	}

	paths := make([]*Path, len(relations))
	for i, rel := range relations {
		paths[i] = FromRelationship(rel)
	}
	return NewFixedIterator(paths...)
}

// NewConflictingPermissionsFixedIterator creates a FixedIterator with potential permission conflicts for testing
func NewConflictingPermissionsFixedIterator() *FixedIterator {
	relations := []tuple.Relationship{
		// Same user with different permission levels on same resource
		createRelation("document", "conflicted", "viewer", "user", "alice", "..."),
		createRelation("document", "conflicted", "editor", "user", "alice", "..."),
		createRelation("document", "conflicted", "owner", "user", "alice", "..."),

		// Different relation types for same resource-subject pair
		createRelation("document", "mixed", "viewer", "user", "bob", "..."),
		createRelation("document", "mixed", "parent", "user", "bob", "..."),

		// Group and direct permissions on same resource
		createRelation("document", "group_direct", "viewer", "group", "team", "member"),
		createRelation("document", "group_direct", "editor", "user", "charlie", "..."),
		createRelation("group", "team", "member", "user", "charlie", "..."),
	}

	paths := make([]*Path, len(relations))
	for i, rel := range relations {
		paths[i] = FromRelationship(rel)
	}
	return NewFixedIterator(paths...)
}

// FaultyIterator is a test helper that simulates iterator errors
type FaultyIterator struct {
	shouldFailOnCheck   bool
	shouldFailOnCollect bool
}

var _ Iterator = &FaultyIterator{}

func (f *FaultyIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	if f.shouldFailOnCheck {
		return nil, fmt.Errorf("faulty iterator error")
	}
	// Return a sequence that will fail during collection
	if f.shouldFailOnCollect {
		return func(yield func(*Path, error) bool) {
			yield(nil, fmt.Errorf("faulty iterator collection error"))
		}, nil
	}
	// Return empty sequence
	return func(yield func(*Path, error) bool) {}, nil
}

func (f *FaultyIterator) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	if f.shouldFailOnCheck {
		return nil, fmt.Errorf("faulty iterator error")
	}
	// Return a sequence that will fail during collection
	if f.shouldFailOnCollect {
		return func(yield func(*Path, error) bool) {
			yield(nil, fmt.Errorf("faulty iterator collection error"))
		}, nil
	}
	// Return empty sequence
	return func(yield func(*Path, error) bool) {}, nil
}

func (f *FaultyIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	if f.shouldFailOnCheck {
		return nil, fmt.Errorf("faulty iterator error")
	}
	// Return a sequence that will fail during collection
	if f.shouldFailOnCollect {
		return func(yield func(*Path, error) bool) {
			yield(nil, fmt.Errorf("faulty iterator collection error"))
		}, nil
	}
	// Return empty sequence
	return func(yield func(*Path, error) bool) {}, nil
}

func (f *FaultyIterator) Clone() Iterator {
	return &FaultyIterator{
		shouldFailOnCheck:   f.shouldFailOnCheck,
		shouldFailOnCollect: f.shouldFailOnCollect,
	}
}

func (f *FaultyIterator) Explain() Explain {
	return Explain{Info: "FaultyIterator"}
}

// NewFaultyIterator creates a new FaultyIterator for testing error conditions
func NewFaultyIterator(shouldFailOnCheck, shouldFailOnCollect bool) *FaultyIterator {
	return &FaultyIterator{
		shouldFailOnCheck:   shouldFailOnCheck,
		shouldFailOnCollect: shouldFailOnCollect,
	}
}
