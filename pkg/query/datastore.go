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

func (r *DatastoreIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	// For subrelations, we need to allow type mismatches because the subrelation might bridge different types
	// For example, group:member -> group:member should find group:everyone#member@group:engineering#member
	// and then that relationship should be used by the Arrow to check group:engineering#member for user subjects
	// However, wildcard relations and ellipsis relations should always enforce strict type checking
	// Ellipsis (...) means "any relation on the same type", not "bridging to a different type"
	if subject.ObjectType != r.base.Type() && r.base.Subrelation() != "" && r.base.Subrelation() != tuple.Ellipsis && !r.base.Wildcard() {
		// For non-wildcard, non-ellipsis subrelations, we proceed with the query even if types don't match
		// This allows finding intermediate relationships that bridge type gaps
		ctx.TraceStep(r, "subject type %s doesn't match base type %s, but proceeding due to subrelation %s",
			subject.ObjectType, r.base.Type(), r.base.Subrelation())
	} else if subject.ObjectType != r.base.Type() {
		// For non-subrelations, ellipsis, and all wildcard relations, strict type checking applies
		ctx.TraceStep(r, "subject type %s doesn't match base type %s, returning empty", subject.ObjectType, r.base.Type())
		return EmptyPathSeq(), nil
	}

	if r.base.Wildcard() {
		return r.checkWildcardImpl(ctx, resources, subject)
	}
	return r.checkNormalImpl(ctx, resources, subject)
}

func (r *DatastoreIterator) checkNormalImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	ctx.TraceStep(r, "querying datastore for %s:%s with resources=%v", r.base.Type(), r.base.RelationName(), resourceIDs(resources))

	pathSeq, err := ctx.Reader.CheckRelationships(ctx,
		resources,
		r.base.RelationName(),
		subject,
		r.base.Caveat() != "", r.base.Expiration(),
	)
	if err != nil {
		return nil, err
	}

	// Eagerly collect all results to terminate the database query immediately
	paths, err := CollectAll(pathSeq)
	if err != nil {
		return nil, err
	}
	return PathSeqFromSlice(paths), nil
}

func (r *DatastoreIterator) checkWildcardImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	// Query the datastore for wildcard relationships (subject ObjectID = "*")
	wildcardSubject := ObjectAndRelation{
		ObjectType: subject.ObjectType,
		ObjectID:   WildcardObjectID,
		Relation:   subject.Relation,
	}

	pathSeq, err := ctx.Reader.CheckRelationships(ctx,
		resources,
		r.base.RelationName(),
		wildcardSubject,
		r.base.Caveat() != "", r.base.Expiration(),
	)
	if err != nil {
		return nil, err
	}

	// Rewrite subjects from wildcard back to the actual subject
	pathSeq = RewriteSubject(pathSeq, subject)

	// Eagerly collect all results to terminate the database query immediately
	paths, err := CollectAll(pathSeq)
	if err != nil {
		return nil, err
	}
	return PathSeqFromSlice(paths), nil
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

		// Filter out wildcard subjects and eagerly collect
		paths, err := CollectAll(FilterWildcardSubjects(pathSeq))
		if err != nil {
			return nil, err
		}
		return PathSeqFromSlice(paths), nil
	}

	// Pagination is configured - return a PathSeq that fetches pages as needed
	return func(yield func(Path, error) bool) {
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
				yield(Path{}, err)
				return
			}

			paths, err := CollectAll(FilterWildcardSubjects(pathSeq))
			if err != nil {
				yield(Path{}, err)
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
	// When a relation contains a wildcard (e.g., user:*), it means "all subjects of that type"
	// that have ANY relationship with this resource. We enumerate concrete subjects by:
	// 1. First checking if a wildcard relationship actually exists for this resource
	// 2. If yes, querying for all concrete subjects with relationships to this resource
	//
	// This avoids doing a full subject enumeration when no wildcard exists (the common case).

	// First, check if there's actually a wildcard relationship for this resource
	// by doing a CheckRelationships probe with subject ObjectID = "*".
	subjectType := ObjectType{
		Type:        r.base.Type(),
		Subrelation: r.base.Subrelation(),
	}

	wildcardSubject := ObjectAndRelation{
		ObjectType: r.base.Type(),
		ObjectID:   WildcardObjectID,
		Relation:   r.base.Subrelation(),
	}

	wildcardPathSeq, err := ctx.Reader.CheckRelationships(ctx,
		[]Object{resource},
		r.base.RelationName(),
		wildcardSubject,
		r.base.Caveat() != "", r.base.Expiration(),
	)
	if err != nil {
		return nil, err
	}

	hasWildcard := false
	for _, err := range wildcardPathSeq {
		if err != nil {
			return nil, err
		}
		hasWildcard = true
		break
	}

	if !hasWildcard {
		return EmptyPathSeq(), nil
	}

	// Wildcard exists — enumerate all concrete subjects of the appropriate type.
	// Empty Object{} and empty resourceRelation means no resource constraints at all.
	allSubjectsResource := Object{}
	const noResourceRelation = ""

	if ctx.PaginationLimit == nil {
		pathSeq, err := ctx.Reader.QuerySubjects(ctx,
			allSubjectsResource,
			noResourceRelation,
			subjectType,
			r.base.Caveat() != "", r.base.Expiration(),
			QueryPage{},
		)
		if err != nil {
			return nil, err
		}

		paths, err := CollectAll(FilterWildcardSubjects(pathSeq))
		if err != nil {
			return nil, err
		}
		return PathSeqFromSlice(paths), nil
	}

	// Pagination is configured
	return func(yield func(Path, error) bool) {
		iteratorID := fmt.Sprintf("%016x:iter_subjects_wildcard", r.CanonicalKey().Hash())
		cursor := ctx.GetPaginationCursor(iteratorID)

		for {
			pathSeq, err := ctx.Reader.QuerySubjects(ctx,
				allSubjectsResource,
				noResourceRelation,
				subjectType,
				r.base.Caveat() != "", r.base.Expiration(),
				QueryPage{Limit: ctx.PaginationLimit, Cursor: cursor},
			)
			if err != nil {
				yield(Path{}, err)
				return
			}

			paths, err := CollectAll(FilterWildcardSubjects(pathSeq))
			if err != nil {
				yield(Path{}, err)
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

	return func(yield func(Path, error) bool) {
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
				yield(Path{}, err)
				return
			}

			paths, err := CollectAll(pathSeq)
			if err != nil {
				yield(Path{}, err)
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
	wildcardSubject := ObjectAndRelation{
		ObjectType: subject.ObjectType,
		ObjectID:   WildcardObjectID,
		Relation:   subject.Relation,
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

	return func(yield func(Path, error) bool) {
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
				yield(Path{}, err)
				return
			}

			pathSeq = RewriteSubject(pathSeq, subject)
			paths, err := CollectAll(pathSeq)
			if err != nil {
				yield(Path{}, err)
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

// resourceIDs extracts the ObjectID strings from a slice of Objects.
func resourceIDs(resources []Object) []string {
	ids := make([]string, len(resources))
	for i, r := range resources {
		ids[i] = r.ObjectID
	}
	return ids
}
