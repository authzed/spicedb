package query

// Alias is an iterator that rewrites the Resource's Relation field of all paths
// streamed from the sub-iterator to a specified alias relation.
type Alias struct {
	relation string
	subIt    Iterator
}

var _ Iterator = &Alias{}

// NewAlias creates a new Alias iterator that rewrites paths from the sub-iterator
// to use the specified relation name.
func NewAlias(relation string, subIt Iterator) *Alias {
	return &Alias{
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
			subSeq, err := a.subIt.CheckImpl(ctx, resources, subject)
			if err != nil {
				return nil, err
			}

			return func(yield func(Path, error) bool) {
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
			}, nil
		}
	}

	// No self-edge detected, just rewrite paths from sub-iterator
	subSeq, err := a.subIt.CheckImpl(ctx, resources, subject)
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

func (a *Alias) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	subSeq, err := a.subIt.IterSubjectsImpl(ctx, resource)
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
	subSeq, err := a.subIt.IterResourcesImpl(ctx, subject)
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

func (a *Alias) Clone() Iterator {
	return &Alias{
		relation: a.relation,
		subIt:    a.subIt.Clone(),
	}
}

func (a *Alias) Explain() Explain {
	return Explain{
		Info:       "Alias(" + a.relation + ")",
		SubExplain: []Explain{a.subIt.Explain()},
	}
}
