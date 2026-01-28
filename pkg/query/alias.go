package query

import (
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

func (a *Alias) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	// First, check for self-edge: if the object with internal relation matches the subject
	for _, resource := range resources {
		resourceWithAlias := resource.WithRelation(a.relation)
		if resourceWithAlias.ObjectID == subject.ObjectID &&
			resourceWithAlias.ObjectType == subject.ObjectType &&
			resourceWithAlias.Relation == subject.Relation {
			// Return the self-edge path first
			selfPath := Path{
				Resource: GetObject(resourceWithAlias),
				Relation: resourceWithAlias.Relation,
				Subject:  subject,
				Metadata: make(map[string]any),
			}

			// Also get relations from sub-iterator
			subSeq, err := ctx.Check(a.subIt, resources, subject)
			if err != nil {
				return nil, err
			}

			// Create combined sequence with self-edge and rewritten paths
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

			// Wrap with deduplication to handle duplicate paths after rewriting
			return DeduplicatePathSeq(combined), nil
		}
	}

	// No self-edge detected, just rewrite paths from sub-iterator
	subSeq, err := ctx.Check(a.subIt, resources, subject)
	if err != nil {
		return nil, err
	}

	rewritten := func(yield func(Path, error) bool) {
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

	// Wrap with deduplication to handle duplicate paths after rewriting
	return DeduplicatePathSeq(rewritten), nil
}

func (a *Alias) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	subSeq, err := ctx.IterSubjects(a.subIt, resource)
	if err != nil {
		return nil, err
	}

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
	}, nil
}

func (a *Alias) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	subSeq, err := ctx.IterResources(a.subIt, subject)
	if err != nil {
		return nil, err
	}

	// If the relation on the alias iterator is the same
	// as the subject relation on the subject, we have
	// a self edge and want to yield it accordingly.
	if a.relation == subject.Relation {
		selfPath := Path{
			Resource: GetObject(subject),
			Relation: a.relation,
			Subject:  subject,
		}

		combined := func(yield func(Path, error) bool) {
			// Yield the self path first
			if !yield(selfPath, nil) {
				return
			}

			for subPath, err := range subSeq {
				if err != nil {
					yield(Path{}, err)
					return
				}

				// Then yield remaining paths with the rewrite
				subPath.Relation = a.relation
				if !yield(subPath, nil) {
					return
				}
			}
		}

		return DeduplicatePathSeq(combined), nil
	}

	// else we don't have a subpath and do the normal logic
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
	}, nil
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
