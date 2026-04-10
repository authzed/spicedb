package query

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// DatastoreIterator is a common leaf iterator. It represents the set of all
// relationships of the given schema.BaseRelation, ie, relations that have a
// known resource and subject type and may contain caveats or expiration.
//
// The DatastoreIterator, being the leaf, generates this set by calling the datastore.
type DatastoreIterator struct {
	base         *schema.BaseRelation
	canonicalKey CanonicalKey
}

var _ Iterator = &DatastoreIterator{}

func NewDatastoreIterator(base *schema.BaseRelation) *DatastoreIterator {
	return &DatastoreIterator{
		base: base,
	}
}

func (r *DatastoreIterator) CheckImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	// For subrelations, we need to allow type mismatches because the subrelation might bridge different types
	// For example, group:member -> group:member should find group:everyone#member@group:engineering#member
	// and then that relationship should be used by the Arrow to check group:engineering#member for user subjects
	// However, wildcard relations and ellipsis relations should always enforce strict type checking
	// Ellipsis (...) means "any relation on the same type", not "bridging to a different type"
	if subject.ObjectType != r.base.Type() && r.base.Subrelation() != "" && r.base.Subrelation() != tuple.Ellipsis && !r.base.Wildcard() {
		// For non-wildcard, non-ellipsis subrelations, we proceed with the query even if types don't match
		// This allows finding intermediate relationships that bridge type gaps
		if ctx.shouldTrace() {
			ctx.TraceStep(r, "subject type %s doesn't match base type %s, but proceeding due to subrelation %s",
				subject.ObjectType, r.base.Type(), r.base.Subrelation())
		}
	} else if subject.ObjectType != r.base.Type() {
		// For non-subrelations, ellipsis, and all wildcard relations, strict type checking applies
		if ctx.shouldTrace() {
			ctx.TraceStep(r, "subject type %s doesn't match base type %s, returning empty", subject.ObjectType, r.base.Type())
		}
		return nil, nil
	}

	if r.base.Wildcard() {
		return r.checkWildcardImpl(ctx, resource, subject)
	}
	return r.checkNormalImpl(ctx, resource, subject)
}

func (r *DatastoreIterator) checkNormalImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(r, "querying datastore for %s:%s with resource=%s:%s", r.base.Type(), r.base.RelationName(), resource.ObjectType, resource.ObjectID)
	}

	resourceType := ObjectType{Type: r.base.DefinitionName()}
	pathSeq, err := ctx.Reader.CheckRelationships(ctx,
		resourceType,
		resource.ObjectID,
		r.base.RelationName(),
		subject,
		r.base.Caveat() != "", r.base.Expiration(),
	)
	if err != nil {
		return nil, err
	}

	// Collect results and return the first (there is at most one for a single resource).
	// Eagerly collecting also terminates the database query immediately.
	paths, err := CollectAll(pathSeq)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, nil
	}
	return paths[0], nil
}

func (r *DatastoreIterator) checkWildcardImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	// Invariant: wildcard subjects in the datastore are always stored with the ellipsis
	// relation. The "*" is only ever an ObjectID; "type:*#relation" is syntactically
	// invalid and cannot be written. Any caller passing a non-ellipsis relation here
	// would cause us to query with the wrong relation filter and return a false negative.
	if subject.Relation != tuple.Ellipsis {
		return nil, spiceerrors.MustBugf("checkWildcardImpl called with non-ellipsis subject relation %q for subject %s:%s; wildcard subjects are always stored with ellipsis relation", subject.Relation, subject.ObjectType, subject.ObjectID)
	}

	// Query the datastore for wildcard relationships (subject ObjectID = "*")
	wildcardSubject := ObjectAndRelation{
		ObjectType: subject.ObjectType,
		ObjectID:   WildcardObjectID,
		Relation:   tuple.Ellipsis,
	}

	resourceType := ObjectType{Type: r.base.DefinitionName()}
	pathSeq, err := ctx.Reader.CheckRelationships(ctx,
		resourceType,
		resource.ObjectID,
		r.base.RelationName(),
		wildcardSubject,
		r.base.Caveat() != "", r.base.Expiration(),
	)
	if err != nil {
		return nil, err
	}

	// Rewrite subjects from wildcard back to the actual subject, collect, return first.
	pathSeq = RewriteSubject(pathSeq, subject)
	paths, err := CollectAll(pathSeq)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, nil
	}
	return paths[0], nil
}

func (r *DatastoreIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	if r.base.Wildcard() {
		return r.iterSubjectsWildcardImpl(ctx, resource)
	}
	return r.iterSubjectsNormalImpl(ctx, resource)
}

func (r *DatastoreIterator) iterSubjectsNormalImpl(ctx *Context, resource Object) (PathSeq, error) {
	subjectType := ObjectType{
		Type:        r.base.Type(),
		Subrelation: r.base.Subrelation(),
	}

	// If pagination is not configured, do the simple eager collection
	if ctx.PaginationLimit == nil {
		pathSeq, err := ctx.Reader.QuerySubjects(ctx,
			resource,
			r.base.RelationName(),
			subjectType,
			r.base.Caveat() != "", r.base.Expiration(),
			QueryPage{},
		)
		if err != nil {
			return nil, err
		}

		// Eagerly collect (wildcards propagate through the tree and are stripped at the top level)
		paths, err := CollectAll(pathSeq)
		if err != nil {
			return nil, err
		}
		return PathSeqFromSlice(paths), nil
	}

	// Pagination is configured - return a PathSeq that fetches pages as needed
	return func(yield func(*Path, error) bool) {
		iteratorID := fmt.Sprintf("%016x:iter_subjects", r.CanonicalKey().Hash())
		cursor := ctx.GetPaginationCursor(iteratorID)

		for {
			pathSeq, err := ctx.Reader.QuerySubjects(ctx,
				resource,
				r.base.RelationName(),
				subjectType,
				r.base.Caveat() != "", r.base.Expiration(),
				QueryPage{Limit: ctx.PaginationLimit, Cursor: cursor},
			)
			if err != nil {
				yield(nil, err)
				return
			}

			paths, err := CollectAll(pathSeq)
			if err != nil {
				yield(nil, err)
				return
			}

			if len(paths) == 0 {
				return
			}

			lastPath := paths[len(paths)-1]
			if rel, err := lastPath.ToRelationship(); err == nil {
				cursor = &rel
				ctx.SetPaginationCursor(iteratorID, cursor)
			}

			for _, path := range paths {
				if !yield(path, nil) {
					return
				}
			}

			if uint64(len(paths)) < *ctx.PaginationLimit {
				return
			}
		}
	}, nil
}

func (r *DatastoreIterator) iterSubjectsWildcardImpl(ctx *Context, resource Object) (PathSeq, error) {
	// When a relation contains a wildcard (e.g., user:*[caveat]), it means "all subjects of that
	// type are (conditionally) in this set". Rather than enumerating every concrete subject of
	// that type across the entire store, we return the wildcard subject itself (user:*) as the
	// found path. This matches the traditional LookupSubjects behavior and avoids a degenerate
	// store-wide query with no resource filters.
	//
	// The IntersectionIterator and ExclusionIterator have wildcard-aware set operations that
	// handle the expansion of wildcards when combined with concrete subjects from other branches.

	wildcardSubject := ObjectAndRelation{
		ObjectType: r.base.Type(),
		ObjectID:   WildcardObjectID,
		Relation:   r.base.Subrelation(),
	}

	resourceType := ObjectType{Type: r.base.DefinitionName()}
	return ctx.Reader.CheckRelationships(ctx,
		resourceType,
		resource.ObjectID,
		r.base.RelationName(),
		wildcardSubject,
		r.base.Caveat() != "", r.base.Expiration(),
	)
}

func (r *DatastoreIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	// If the types don't match, we don't even have to go to the datastore.
	if subject.ObjectType != r.base.Type() {
		return EmptyPathSeq(), nil
	}

	// Handle wildcards first - they don't have subrelations and match any query relation
	if r.base.Wildcard() {
		return r.iterResourcesWildcardImpl(ctx, subject)
	}

	// An empty Relation is always a bug in the caller: it must be either tuple.Ellipsis
	// for direct membership or a specific subrelation string.
	if subject.Relation == "" {
		return nil, spiceerrors.MustBugf("IterResources called with empty subject.Relation for %s:%s; caller must use tuple.Ellipsis or a specific subrelation", subject.ObjectType, subject.ObjectID)
	}

	// Check if subject relation matches what this iterator expects.
	// When the schema uses ellipsis ("..."), callers should pass tuple.Ellipsis — the
	// MustBugf above ensures "" never reaches here — but we match on the schema value directly.
	if r.base.Subrelation() != subject.Relation {
		return EmptyPathSeq(), nil
	}

	if ctx.PaginationLimit == nil {
		pathSeq, err := ctx.Reader.QueryResources(ctx,
			r.base.DefinitionName(),
			r.base.RelationName(),
			subject,
			r.base.Caveat() != "", r.base.Expiration(),
			QueryPage{},
		)
		if err != nil {
			return nil, err
		}

		paths, err := CollectAll(pathSeq)
		if err != nil {
			return nil, err
		}
		return PathSeqFromSlice(paths), nil
	}

	return func(yield func(*Path, error) bool) {
		iteratorID := fmt.Sprintf("%016x:iter_resources", r.CanonicalKey().Hash())
		cursor := ctx.GetPaginationCursor(iteratorID)

		for {
			pathSeq, err := ctx.Reader.QueryResources(ctx,
				r.base.DefinitionName(),
				r.base.RelationName(),
				subject,
				r.base.Caveat() != "", r.base.Expiration(),
				QueryPage{Limit: ctx.PaginationLimit, Cursor: cursor},
			)
			if err != nil {
				yield(nil, err)
				return
			}

			paths, err := CollectAll(pathSeq)
			if err != nil {
				yield(nil, err)
				return
			}

			if len(paths) == 0 {
				return
			}

			lastPath := paths[len(paths)-1]
			if rel, err := lastPath.ToRelationship(); err == nil {
				cursor = &rel
				ctx.SetPaginationCursor(iteratorID, cursor)
			}

			for _, path := range paths {
				if !yield(path, nil) {
					return
				}
			}

			if uint64(len(paths)) < *ctx.PaginationLimit {
				return
			}
		}
	}, nil
}

func (r *DatastoreIterator) iterResourcesWildcardImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	// Invariant: wildcard subjects in the datastore are always stored with the ellipsis
	// relation. The "*" is only ever an ObjectID; "type:*#relation" is syntactically
	// invalid and cannot be written. Any caller passing a non-ellipsis relation here
	// would cause us to query with the wrong relation filter and return a false negative.
	if subject.Relation != tuple.Ellipsis {
		return nil, spiceerrors.MustBugf("iterResourcesWildcardImpl called with non-ellipsis subject relation %q for subject %s:%s; wildcard subjects are always stored with ellipsis relation", subject.Relation, subject.ObjectType, subject.ObjectID)
	}

	wildcardSubject := ObjectAndRelation{
		ObjectType: subject.ObjectType,
		ObjectID:   WildcardObjectID,
		Relation:   tuple.Ellipsis,
	}

	if ctx.PaginationLimit == nil {
		pathSeq, err := ctx.Reader.QueryResources(ctx,
			r.base.DefinitionName(),
			r.base.RelationName(),
			wildcardSubject,
			r.base.Caveat() != "", r.base.Expiration(),
			QueryPage{},
		)
		if err != nil {
			return nil, err
		}

		pathSeq = RewriteSubject(pathSeq, subject)
		paths, err := CollectAll(pathSeq)
		if err != nil {
			return nil, err
		}
		return PathSeqFromSlice(paths), nil
	}

	return func(yield func(*Path, error) bool) {
		iteratorID := fmt.Sprintf("%016x:iter_resources_wildcard", r.CanonicalKey().Hash())
		cursor := ctx.GetPaginationCursor(iteratorID)

		for {
			pathSeq, err := ctx.Reader.QueryResources(ctx,
				r.base.DefinitionName(),
				r.base.RelationName(),
				wildcardSubject,
				r.base.Caveat() != "", r.base.Expiration(),
				QueryPage{Limit: ctx.PaginationLimit, Cursor: cursor},
			)
			if err != nil {
				yield(nil, err)
				return
			}

			pathSeq = RewriteSubject(pathSeq, subject)
			paths, err := CollectAll(pathSeq)
			if err != nil {
				yield(nil, err)
				return
			}

			if len(paths) == 0 {
				return
			}

			lastPath := paths[len(paths)-1]
			if rel, err := lastPath.ToRelationship(); err == nil {
				cursor = &rel
				ctx.SetPaginationCursor(iteratorID, cursor)
			}

			for _, path := range paths {
				if !yield(path, nil) {
					return
				}
			}

			if uint64(len(paths)) < *ctx.PaginationLimit {
				return
			}
		}
	}, nil
}

func (r *DatastoreIterator) Clone() Iterator {
	return &DatastoreIterator{
		canonicalKey: r.canonicalKey,
		base:         r.base,
	}
}

func (r *DatastoreIterator) Explain() Explain {
	relationName := r.base.Subrelation()
	if r.base.Wildcard() {
		relationName = "*"
	}
	return Explain{
		Info: fmt.Sprintf("Datastore(%s:%s -> %s:%s, caveat: %v, expiration: %v)",
			r.base.DefinitionName(), r.base.RelationName(), r.base.Type(), relationName,
			r.base.Caveat() != "", r.base.Expiration()),
	}
}

func (r *DatastoreIterator) Subiterators() []Iterator {
	return nil
}

func (r *DatastoreIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return nil, spiceerrors.MustBugf("Trying to replace a leaf DatastoreIterator's subiterators")
}

func (r *DatastoreIterator) CanonicalKey() CanonicalKey {
	return r.canonicalKey
}

func (r *DatastoreIterator) ResourceType() ([]ObjectType, error) {
	return []ObjectType{{
		Type:        r.base.DefinitionName(),
		Subrelation: tuple.Ellipsis,
	}}, nil
}

func (r *DatastoreIterator) SubjectTypes() ([]ObjectType, error) {
	// For wildcards, return the base type with no subrelation
	if r.base.Wildcard() {
		return []ObjectType{{
			Type:        r.base.Type(),
			Subrelation: "",
		}}, nil
	}

	// For ellipsis, preserve the ellipsis subrelation so callers that construct
	// ObjectAndRelation values from SubjectTypes get the correct relation to query with.
	if r.base.Subrelation() == tuple.Ellipsis {
		return []ObjectType{{
			Type:        r.base.Type(),
			Subrelation: tuple.Ellipsis,
		}}, nil
	}

	// For regular subrelations, return the specific type and subrelation
	return []ObjectType{{
		Type:        r.base.Type(),
		Subrelation: r.base.Subrelation(),
	}}, nil
}
