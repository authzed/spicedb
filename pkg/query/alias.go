package query

import (
	"github.com/google/uuid"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/google/uuid"
)

// Alias is an iterator that rewrites the Resource's Relation field of all paths
// streamed from the sub-iterator to a specified alias relation.
type Alias struct {
	id       string
	relation string
	subIt    Iterator
}

var _ Iterator = &Alias{}

// NewAlias creates a new Alias iterator that rewrites paths from the sub-iterator
// to use the specified relation name.
func NewAlias(relation string, subIt Iterator) *Alias {
	return &Alias{
		id:       uuid.NewString(),
		relation: relation,
		subIt:    subIt,
	}
}

// maybePrependSelfEdge checks if a self-edge should be added and combines it with the given sequence.
// A self-edge is added when the resource with the alias relation matches the subject.
func (a *Alias) maybePrependSelfEdge(resource Object, subSeq PathSeq, shouldAddSelfEdge bool) PathSeq {
	if !shouldAddSelfEdge {
		// No self-edge, just rewrite paths from sub-iterator
		return func(yield func(Path, error) bool) {
			for path, err := range subSeq {
				if err != nil {
					yield(Path{}, err)
					return
				}

				path.Relation = a.relation
				if !yield(path, nil) {
					return
				}
			}
		}
	}

	// Create a self-edge path
	selfPath := Path{
		Resource: resource,
		Relation: a.relation,
		Subject: ObjectAndRelation{
			ObjectType: resource.ObjectType,
			ObjectID:   resource.ObjectID,
			Relation:   a.relation,
		},
		Metadata: make(map[string]any),
	}

	// Combine self-edge with paths from sub-iterator
	combined := func(yield func(Path, error) bool) {
		// Yield the self-edge first
		if !yield(selfPath, nil) {
			return
		}

		// Then yield rewritten paths from sub-iterator
		for path, err := range subSeq {
			if err != nil {
				yield(Path{}, err)
				return
			}

			path.Relation = a.relation
			if !yield(path, nil) {
				return
			}
		}
	}

	// Wrap with deduplication to handle duplicate paths
	return DeduplicatePathSeq(combined)
}

func (a *Alias) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	// Get relations from sub-iterator
	subSeq, err := ctx.Check(a.subIt, resources, subject)
	if err != nil {
		return nil, err
	}

	// Check for self-edge: if any resource with the alias relation matches the subject
	for _, resource := range resources {
		resourceWithAlias := resource.WithRelation(a.relation)
		if resourceWithAlias.ObjectID == subject.ObjectID &&
			resourceWithAlias.ObjectType == subject.ObjectType &&
			resourceWithAlias.Relation == subject.Relation {
			return a.maybePrependSelfEdge(GetObject(resourceWithAlias), subSeq, true), nil
		}
	}

	// No self-edge detected, just rewrite paths from sub-iterator
	return DeduplicatePathSeq(a.maybePrependSelfEdge(Object{}, subSeq, false)), nil
}

func (a *Alias) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	subSeq, err := ctx.IterSubjects(a.subIt, resource, filterSubjectType)
	if err != nil {
		return nil, err
	}

	// Check if we should add a self-edge by testing if the resource (as a subject)
	// exists in the datastore. We do this by calling IterResources on the sub-iterator
	// with the resource as the subject. If it returns anything, the resource exists
	// as a subject and we should add the self-edge.
	shouldAddSelfEdge := false

	// Only check if the filter allows this resource type as a subject
	typeMatches := filterSubjectType.Type == "" || filterSubjectType.Type == resource.ObjectType
	relationMatches := filterSubjectType.Subrelation == "" || filterSubjectType.Subrelation == a.relation

	if typeMatches && relationMatches && ctx.Reader != nil {
		// Test if resource#relation exists as a subject by querying the datastore directly
		// We check if there are ANY relationships where resource#relation appears as the subject
		filter := datastore.RelationshipsFilter{
			OptionalSubjectsSelectors: []datastore.SubjectsSelector{{
				OptionalSubjectType: resource.ObjectType,
				OptionalSubjectIds:  []string{resource.ObjectID},
				RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation(a.relation),
			}},
		}

		iter, err := ctx.Reader.QueryRelationships(ctx, filter, options.WithLimit(options.LimitOne))
		if err != nil {
			return nil, err
		}

		// If the query returns any relationship, the resource exists as a subject
		for _, err := range iter {
			if err != nil {
				return nil, err
			}
			shouldAddSelfEdge = true
			break
		}
	}

	// Use the helper method that prepends self-edge if needed
	return a.maybePrependSelfEdge(resource, subSeq, shouldAddSelfEdge), nil
}

func (a *Alias) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	subSeq, err := ctx.IterResources(a.subIt, subject, filterResourceType)
	if err != nil {
		return nil, err
	}

	// If the relation on the alias iterator is the same as the subject relation,
	// we have a self-edge and should yield it
	shouldAddSelfEdge := a.relation == subject.Relation

	return a.maybePrependSelfEdge(GetObject(subject), subSeq, shouldAddSelfEdge), nil
}

func (a *Alias) Clone() Iterator {
	return &Alias{
		id:       uuid.NewString(),
		relation: a.relation,
		subIt:    a.subIt.Clone(),
	}
}

func (a *Alias) Explain() Explain {
	return Explain{
		Name:       "Alias",
		Info:       "Alias(" + a.relation + ")",
		SubExplain: []Explain{a.subIt.Explain()},
	}
}

func (a *Alias) Subiterators() []Iterator {
	return []Iterator{a.subIt}
}

func (a *Alias) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &Alias{id: uuid.NewString(), relation: a.relation, subIt: newSubs[0]}, nil
}

func (a *Alias) ID() string {
	return a.id
}

func (a *Alias) ResourceType() (ObjectType, error) {
	return a.subIt.ResourceType()
}

func (a *Alias) SubjectTypes() ([]ObjectType, error) {
	return a.subIt.SubjectTypes()
}
