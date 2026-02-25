package query

import (
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

// AliasIterator is an iterator that rewrites the Resource's Relation field of all paths
// streamed from the sub-iterator to a specified alias relation.
type AliasIterator struct {
	relation     string
	subIt        Iterator
	canonicalKey CanonicalKey
}

var _ Iterator = &AliasIterator{}

// NewAliasIterator creates a new Alias iterator that rewrites paths from the sub-iterator
// to use the specified relation name.
func NewAliasIterator(relation string, subIt Iterator) *AliasIterator {
	return &AliasIterator{
		relation: relation,
		subIt:    subIt,
	}
}

// maybePrependSelfEdge checks if a self-edge should be added and combines it with the given sequence.
// A self-edge is added when the resource with the alias relation matches the subject.
func (a *AliasIterator) maybePrependSelfEdge(resource Object, subSeq PathSeq, shouldAddSelfEdge bool) PathSeq {
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

func (a *AliasIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
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

func (a *AliasIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	subSeq, err := ctx.IterSubjects(a.subIt, resource, filterSubjectType)
	if err != nil {
		return nil, err
	}

	// Check if we should add a self-edge based on identity semantics.
	// The dispatcher Check includes an identity check (see filterForFoundMemberResource
	// in internal/graph/check.go): if the resource (with relation) matches the subject
	// exactly, it returns MEMBER. This only applies if the resource actually appears
	// as a subject in the data and the filter allows it.
	shouldAddSelfEdge := a.shouldIncludeSelfEdge(ctx, resource, filterSubjectType)

	return a.maybePrependSelfEdge(resource, subSeq, shouldAddSelfEdge), nil
}

// shouldIncludeSelfEdge checks if a self-edge should be included for the given resource.
// This matches the dispatcher's identity check behavior: if resource#relation appears as
// a subject anywhere in the datastore (expired or not), and the filter allows it, we
// include a self-edge in the results.
func (a *AliasIterator) shouldIncludeSelfEdge(ctx *Context, resource Object, filterSubjectType ObjectType) bool {
	// First check: does the filter allow this resource type as a subject?
	typeMatches := filterSubjectType.Type == "" || filterSubjectType.Type == resource.ObjectType
	relationMatches := filterSubjectType.Subrelation == "" || filterSubjectType.Subrelation == a.relation
	if !typeMatches || !relationMatches || ctx.Reader == nil {
		return false
	}

	// Second check: does the resource actually appear as a subject in the data?
	// We check for ANY relationships (expired or not) because the dispatcher's
	// identity check applies regardless of expiration.
	exists, err := a.resourceExistsAsSubject(ctx, resource)
	if err != nil {
		// On error, conservatively return false rather than failing the entire operation
		return false
	}
	return exists
}

// resourceExistsAsSubject queries the datastore to check if the given resource appears
// as a subject in any relationship, including expired relationships.
func (a *AliasIterator) resourceExistsAsSubject(ctx *Context, resource Object) (bool, error) {
	filter := datastore.RelationshipsFilter{
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{{
			OptionalSubjectType: resource.ObjectType,
			OptionalSubjectIds:  []string{resource.ObjectID},
			RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation(a.relation),
		}},
		OptionalExpirationOption: datastore.ExpirationFilterOptionNone,
	}

	iter, err := ctx.Reader.QueryRelationships(ctx, filter,
		options.WithLimit(options.LimitOne),
		options.WithSkipExpiration(true)) // Include expired relationships
	if err != nil {
		return false, err
	}

	// Check if any relationship exists
	for _, err := range iter {
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func (a *AliasIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	subSeq, err := ctx.IterResources(a.subIt, subject, filterResourceType)
	if err != nil {
		return nil, err
	}

	// Check if we should add a self-edge (identity check: permission always grants access to itself)
	// This matches LookupResources3's logic where subject type+relation must match resource type+relation
	shouldAddSelfEdge := false
	if a.relation == subject.Relation {
		// Get the resource types from the iterator
		resourceTypes, err := a.ResourceType()
		if err != nil {
			return nil, err
		}

		// Add self-edge if:
		// - No resource types defined (empty/unconstrained iterator), OR
		// - Subject type matches one of the possible resource types
		// This allows self-edges for empty iterators while preventing them in nested contexts
		// where the types don't match
		if len(resourceTypes) == 0 {
			shouldAddSelfEdge = true
		} else {
			for _, rt := range resourceTypes {
				if rt.Type == subject.ObjectType {
					shouldAddSelfEdge = true
					break
				}
			}
		}
	}

	return a.maybePrependSelfEdge(GetObject(subject), subSeq, shouldAddSelfEdge), nil
}

func (a *AliasIterator) Clone() Iterator {
	return &AliasIterator{
		canonicalKey: a.canonicalKey,
		relation:     a.relation,
		subIt:        a.subIt.Clone(),
	}
}

func (a *AliasIterator) Explain() Explain {
	return Explain{
		Name:       "Alias",
		Info:       "Alias(" + a.relation + ")",
		SubExplain: []Explain{a.subIt.Explain()},
	}
}

func (a *AliasIterator) Subiterators() []Iterator {
	return []Iterator{a.subIt}
}

func (a *AliasIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &AliasIterator{canonicalKey: a.canonicalKey, relation: a.relation, subIt: newSubs[0]}, nil
}

func (a *AliasIterator) CanonicalKey() CanonicalKey {
	return a.canonicalKey
}

func (a *AliasIterator) ResourceType() ([]ObjectType, error) {
	return a.subIt.ResourceType()
}

func (a *AliasIterator) SubjectTypes() ([]ObjectType, error) {
	return a.subIt.SubjectTypes()
}
