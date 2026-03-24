package query

import (
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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
		if ctx.shouldTrace() {
			ctx.TraceStep(r, "subject type %s doesn't match base type %s, but proceeding due to subrelation %s",
				subject.ObjectType, r.base.Type(), r.base.Subrelation())
		}
	} else if subject.ObjectType != r.base.Type() {
		// For non-subrelations, ellipsis, and all wildcard relations, strict type checking applies
		if ctx.shouldTrace() {
			ctx.TraceStep(r, "subject type %s doesn't match base type %s, returning empty", subject.ObjectType, r.base.Type())
		}
		return EmptyPathSeq(), nil
	}

	if r.base.Wildcard() {
		return r.checkWildcardImpl(ctx, resources, subject)
	}
	return r.checkNormalImpl(ctx, resources, subject)
}

func (r *DatastoreIterator) checkNormalImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	ids := resourceIDs(resources)
	if ctx.shouldTrace() {
		ctx.TraceStep(r, "querying datastore for %s:%s with resources=%v", r.base.Type(), r.base.RelationName(), ids)
	}

	resourceType := ObjectType{Type: r.base.DefinitionName()}
	pathSeq, err := ctx.Reader.CheckRelationships(ctx,
		resourceType,
		ids,
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
		resourceIDs(resources),
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

			paths, err := CollectAll(FilterWildcardSubjects(pathSeq))
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
	// type are (conditionally) in this set". We enumerate concrete subjects by:
	// 1. Fetching the wildcard relationship(s) for this resource to get the wildcard caveat.
	// 2. Enumerating all known concrete subjects of the appropriate type.
	// 3. Synthesizing a path for each subject that carries the wildcard's caveat.
	//
	// IMPORTANT: the enumerated subjects must carry the wildcard's caveat, not the caveats
	// from their own individual relationship rows. For example, for
	//   document:firstdoc#banned@user:*[some_caveat]
	// every user is conditionally banned; a user visible via another relation (e.g. viewer)
	// must still appear as caveated-banned, not as unconditionally-banned.

	subjectType := ObjectType{
		Type:        r.base.Type(),
		Subrelation: r.base.Subrelation(),
	}

	wildcardSubject := ObjectAndRelation{
		ObjectType: r.base.Type(),
		ObjectID:   WildcardObjectID,
		Relation:   r.base.Subrelation(),
	}

	resourceType := ObjectType{Type: r.base.DefinitionName()}
	wildcardPathSeq, err := ctx.Reader.CheckRelationships(ctx,
		resourceType,
		[]string{resource.ObjectID},
		r.base.RelationName(),
		wildcardSubject,
		r.base.Caveat() != "", r.base.Expiration(),
	)
	if err != nil {
		return nil, err
	}

	// Collect the wildcard path(s) to obtain the caveat expression from the wildcard row.
	// In practice there is at most one wildcard row per resource × relation.
	var wildcardCaveat *core.CaveatExpression
	hasWildcard := false
	for wildcardPath, err := range wildcardPathSeq {
		if err != nil {
			return nil, err
		}
		hasWildcard = true
		wildcardCaveat = wildcardPath.Caveat // may be nil for uncaveated wildcard
		break
	}

	if !hasWildcard {
		return EmptyPathSeq(), nil
	}

	// wildcardCaveat is the caveat expression that came from the wildcard row itself
	// (e.g. some_caveat for user:*[some_caveat], or nil for an uncaveated user:*).
	// All synthesized subject paths inherit this caveat.

	// Enumerate all concrete subjects of the appropriate type across the whole store.
	// We use an empty resource / empty relation so that the datastore returns every
	// distinct (subjectType, subjectID) pair it knows about.
	allSubjectsResource := Object{}
	const noResourceRelation = ""

	// synthesizeWildcardPaths takes a raw PathSeq from the datastore and replaces each
	// path's caveat with the wildcard caveat so that callers see the correct conditionality.
	synthesizeWildcardPaths := func(raw []*Path, wildcardCav *core.CaveatExpression) []*Path {
		seen := make(map[string]struct{}, len(raw))
		result := make([]*Path, 0, len(raw))
		for _, p := range raw {
			key := ObjectAndRelationKey(p.Subject)
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			// Synthesize a path: resource stays as-is from wildcard expansion, subject
			// keeps its identity, but the caveat is the wildcard's caveat.
			synthesized := &Path{
				Resource: resource,
				Relation: r.base.RelationName(),
				Subject: ObjectAndRelation{
					ObjectType: p.Subject.ObjectType,
					ObjectID:   p.Subject.ObjectID,
					Relation:   p.Subject.Relation,
				},
				Caveat: wildcardCav,
			}
			result = append(result, synthesized)
		}
		return result
	}

	if ctx.PaginationLimit == nil {
		pathSeq, err := ctx.Reader.QuerySubjects(ctx,
			allSubjectsResource,
			noResourceRelation,
			subjectType,
			false, r.base.Expiration(),
			QueryPage{},
		)
		if err != nil {
			return nil, err
		}

		raw, err := CollectAll(FilterWildcardSubjects(pathSeq))
		if err != nil {
			return nil, err
		}
		paths := synthesizeWildcardPaths(raw, wildcardCaveat)
		return PathSeqFromSlice(paths), nil
	}

	// Pagination is configured
	return func(yield func(*Path, error) bool) {
		iteratorID := fmt.Sprintf("%016x:iter_subjects_wildcard", r.CanonicalKey().Hash())
		cursor := ctx.GetPaginationCursor(iteratorID)

		for {
			pathSeq, err := ctx.Reader.QuerySubjects(ctx,
				allSubjectsResource,
				noResourceRelation,
				subjectType,
				false, r.base.Expiration(),
				QueryPage{Limit: ctx.PaginationLimit, Cursor: cursor},
			)
			if err != nil {
				yield(nil, err)
				return
			}

			raw, err := CollectAll(FilterWildcardSubjects(pathSeq))
			if err != nil {
				yield(nil, err)
				return
			}

			if len(raw) == 0 {
				return
			}

			lastRaw := raw[len(raw)-1]
			if rel, err := lastRaw.ToRelationship(); err == nil {
				cursor = &rel
				ctx.SetPaginationCursor(iteratorID, cursor)
			}

			paths := synthesizeWildcardPaths(raw, wildcardCaveat)
			for _, path := range paths {
				if !yield(path, nil) {
					return
				}
			}

			if uint64(len(raw)) < *ctx.PaginationLimit {
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

// resourceIDs extracts the ObjectID strings from a slice of Objects.
func resourceIDs(resources []Object) []string {
	ids := make([]string, len(resources))
	for i, r := range resources {
		ids[i] = r.ObjectID
	}
	return ids
}
