package query

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// WildcardIterator represents a wildcard relation that matches any subject of the specified type.
// This is used when a BaseRelation has Wildcard: true, indicating that any subject of the
// specified type should be considered valid for the relation.
type WildcardIterator struct {
	base *schema.BaseRelation
}

var _ Iterator = &WildcardIterator{}

// NewWildcardIterator creates a new wildcard iterator for the given BaseRelation.
// The BaseRelation should have Wildcard: true.
func NewWildcardIterator(base *schema.BaseRelation) *WildcardIterator {
	return &WildcardIterator{
		base: base,
	}
}

func (w *WildcardIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (RelationSeq, error) {
	// For wildcard relations, we match if the subject type matches the wildcard type
	if subject.ObjectType != w.base.Type {
		// Subject type doesn't match, no relations
		return func(yield func(Relation, error) bool) {
			// Empty sequence
		}, nil
	}

	// Query the datastore for wildcard relationships (subject ObjectID = "*")
	resourceIDs := make([]string, len(resources))
	for i, res := range resources {
		resourceIDs[i] = res.ObjectID
	}

	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     w.base.DefinitionName(),
		OptionalResourceIds:      resourceIDs,
		OptionalResourceRelation: w.base.RelationName(),
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: w.base.Type,
				OptionalSubjectIds:  []string{tuple.PublicWildcard}, // Look for "*" subjects
				RelationFilter:      w.buildSubjectRelationFilter(),
			},
		},
	}

	reader := ctx.Datastore.SnapshotReader(ctx.Revision)

	relIter, err := reader.QueryRelationships(ctx, filter,
		options.WithSkipCaveats(w.base.Caveat == ""),
		options.WithSkipExpiration(!w.base.Expiration),
		options.WithQueryShape(queryshape.CheckPermissionSelectDirectSubjects),
	)
	if err != nil {
		return nil, err
	}

	// Transform the wildcard relationships to use the concrete subject
	return func(yield func(Relation, error) bool) {
		for rel, err := range relIter {
			if err != nil {
				if !yield(rel, err) {
					return
				}
				continue
			}

			// Replace the wildcard subject with the concrete subject
			concreteRel := rel
			concreteRel.Subject = subject

			if !yield(concreteRel, nil) {
				return
			}
		}
	}, nil
}

func (w *WildcardIterator) buildSubjectRelationFilter() datastore.SubjectRelationFilter {
	if w.base.Subrelation == tuple.Ellipsis {
		return datastore.SubjectRelationFilter{}.WithEllipsisRelation()
	}
	return datastore.SubjectRelationFilter{}.WithNonEllipsisRelation(w.base.Subrelation)
}

func (w *WildcardIterator) IterSubjectsImpl(ctx *Context, resource Object) (RelationSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (w *WildcardIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (RelationSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (w *WildcardIterator) Clone() Iterator {
	return &WildcardIterator{
		base: w.base,
	}
}

func (w *WildcardIterator) Explain() Explain {
	return Explain{
		Info: fmt.Sprintf("Wildcard(%s:%s -> %s:*, caveat: %v, expiration: %v)",
			w.base.DefinitionName(), w.base.RelationName(), w.base.Type,
			w.base.Caveat != "", w.base.Expiration),
	}
}
