package query

import "strings"

// AliasIterator is an iterator that rewrites the Resource's Relation field of all paths
// streamed from the sub-iterator.
//
// The relation field is the alias's underlying identity — the relation produced
// by the immediate sub-iterator. The optional aliasedAs slice, when populated by
// the alias-chain-collapse optimizer, holds the names of additional outer aliases
// that were collapsed into this node, in inner-to-outer order. The outermost name
// (the last entry, when aliasedAs is non-empty) is what emitted paths are rewritten
// to. The self-edge fires when the subject's relation matches any name in the chain
// — relation itself or any entry in aliasedAs — preserving the multi-level self-edge
// semantics of an uncollapsed chain. The underlying identity (relation) is kept so
// the canonical key for caching reflects the data the iterator actually reads.
type AliasIterator struct {
	definitionName string
	relation       string
	aliasedAs      []string
	subIt          Iterator
	canonicalKey   CanonicalKey
}

var _ Iterator = &AliasIterator{}

// NewAliasIterator creates a new Alias iterator that rewrites paths from the sub-iterator
// to use the specified relation name. The definitionName identifies the schema definition
// to which the alias belongs and is used by the dispatch layer to identify the
// (definition, relation) boundary represented by this alias.
func NewAliasIterator(definitionName, relation string, subIt Iterator) *AliasIterator {
	return &AliasIterator{
		definitionName: definitionName,
		relation:       relation,
		subIt:          subIt,
	}
}

// NewAliasIteratorWithChain creates an Alias iterator whose underlying identity is
// `relation` but whose emitted paths use the outermost name and whose self-edge
// fires for any name in the alias chain. `aliasedAs` lists the collapsed outer
// names in inner-to-outer order. This is produced by alias-chain-collapse:
// collapsing Alias("perm")(Alias("view")(Alias("viewer")(...))) preserves "viewer"
// as the identity (cache key, datastore-facing name) and records ["view","perm"]
// in aliasedAs so the iterator emits paths labeled "perm" and the self-edge
// matches subject relations in {"viewer","view","perm"}.
func NewAliasIteratorWithChain(definitionName, relation string, aliasedAs []string, subIt Iterator) *AliasIterator {
	return &AliasIterator{
		definitionName: definitionName,
		relation:       relation,
		aliasedAs:      append([]string(nil), aliasedAs...),
		subIt:          subIt,
	}
}

// effectiveRelation returns the outermost name in the alias chain, which is what
// emitted paths are rewritten to. When no chain is set, this is just the underlying
// relation.
func (a *AliasIterator) effectiveRelation() string {
	if n := len(a.aliasedAs); n > 0 {
		return a.aliasedAs[n-1]
	}
	return a.relation
}

// DefinitionName returns the schema definition this alias belongs to.
func (a *AliasIterator) DefinitionName() string {
	return a.definitionName
}

// Relation returns the outermost name in the alias chain — what emitted paths
// are rewritten to and what callers asked the alias to compute.
func (a *AliasIterator) Relation() string {
	return a.effectiveRelation()
}

// matchesSelfEdgeRelation reports whether the given relation name would trigger
// the alias's self-edge. In an uncollapsed chain, every Alias level fires its own
// self-edge for its relation name; after collapse, the merged node has to honor
// every name in the original chain.
func (a *AliasIterator) matchesSelfEdgeRelation(name string) bool {
	if name == a.relation {
		return true
	}
	for _, n := range a.aliasedAs {
		if n == name {
			return true
		}
	}
	return false
}

// maybePrependSelfEdge checks if a self-edge should be added and combines it with the given sequence.
// A self-edge is added when the resource with the effective relation matches the subject.
func (a *AliasIterator) maybePrependSelfEdge(resource Object, subSeq PathSeq, shouldAddSelfEdge bool) PathSeq {
	rel := a.effectiveRelation()
	if !shouldAddSelfEdge {
		// No self-edge, just rewrite paths from sub-iterator
		return func(yield func(*Path, error) bool) {
			for path, err := range subSeq {
				if err != nil {
					yield(nil, err)
					return
				}

				path.Relation = rel
				if !yield(path, nil) {
					return
				}
			}
		}
	}

	// Create a self-edge path
	selfPath := &Path{
		Resource: resource,
		Relation: rel,
		Subject: ObjectAndRelation{
			ObjectType: resource.ObjectType,
			ObjectID:   resource.ObjectID,
			Relation:   rel,
		},
		Metadata: make(map[string]any),
	}

	// Combine self-edge with paths from sub-iterator
	combined := func(yield func(*Path, error) bool) {
		// Yield the self-edge first
		if !yield(selfPath, nil) {
			return
		}

		// Then yield rewritten paths from sub-iterator
		for path, err := range subSeq {
			if err != nil {
				yield(nil, err)
				return
			}

			path.Relation = rel
			if !yield(path, nil) {
				return
			}
		}
	}

	// Wrap with deduplication to handle duplicate paths
	return DeduplicatePathSeq(combined)
}

func (a *AliasIterator) CheckImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	// Get path from sub-iterator
	subPath, err := ctx.Check(a.subIt, resource, subject)
	if err != nil {
		return nil, err
	}

	rel := a.effectiveRelation()
	if subPath != nil {
		// We have a path! Even if it's caveated, rewrite it and return.
		subPath.Relation = rel
		return subPath, nil
	}

	// We have no sub-path. Check for the self edge: the resource matches the subject
	// (same type+id) and the subject's relation is one of the names in the alias chain.
	// In an uncollapsed chain each Alias level fires its own self-edge; the collapsed
	// node must honor every name in the chain to preserve those semantics.
	if resource.ObjectID != subject.ObjectID || resource.ObjectType != subject.ObjectType {
		return nil, nil
	}
	if !a.matchesSelfEdgeRelation(subject.Relation) {
		return nil, nil
	}

	// Build the synthetic self-edge path. The resource is labeled with the outermost
	// name (what the user asked about); the subject is the user's subject as-is.
	selfPath := &Path{
		Resource: GetObject(resource.WithRelation(rel)),
		Relation: rel,
		Subject:  subject,
		Metadata: make(map[string]any),
	}

	return selfPath, nil
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
	if ctx.TopLevelOperation != OperationIterSubjects {
		return false
	}
	rel := a.effectiveRelation()
	typeMatches := filterSubjectType.Type == "" || filterSubjectType.Type == resource.ObjectType
	relationMatches := filterSubjectType.Subrelation == "" || filterSubjectType.Subrelation == rel
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
	return ctx.Reader.SubjectExistsAsRelationship(ctx, resource, a.effectiveRelation())
}

func (a *AliasIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	subSeq, err := ctx.IterResources(a.subIt, subject, filterResourceType)
	if err != nil {
		return nil, err
	}

	// Check if we should add a self-edge (identity check: permission always grants access to itself).
	// This matches LookupResources3's logic where subject type+relation must match resource type+relation.
	// In a collapsed chain any name in the chain can match.
	shouldAddSelfEdge := false
	if a.matchesSelfEdgeRelation(subject.Relation) {
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
		canonicalKey:   a.canonicalKey,
		definitionName: a.definitionName,
		relation:       a.relation,
		aliasedAs:      append([]string(nil), a.aliasedAs...),
		subIt:          a.subIt.Clone(),
	}
}

func (a *AliasIterator) Explain() Explain {
	info := "Alias(" + a.relation + ")"
	if len(a.aliasedAs) > 0 {
		info = "Alias(" + a.relation + "→" + strings.Join(a.aliasedAs, "→") + ")"
	}
	return Explain{
		Name:       "Alias",
		Info:       info,
		SubExplain: []Explain{a.subIt.Explain()},
	}
}

func (a *AliasIterator) Subiterators() []Iterator {
	return []Iterator{a.subIt}
}

func (a *AliasIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &AliasIterator{
		canonicalKey:   a.canonicalKey,
		definitionName: a.definitionName,
		relation:       a.relation,
		aliasedAs:      append([]string(nil), a.aliasedAs...),
		subIt:          newSubs[0],
	}, nil
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
