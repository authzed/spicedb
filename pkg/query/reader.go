package query

import (
	"context"
	"iter"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/tuple"
)

// WildcardObjectID is the subject ID representing a public wildcard ("*").
const WildcardObjectID = tuple.PublicWildcard

// limitOne is used for existence-probe queries that only need to know if
// at least one row exists.
var limitOne uint64 = 1

// QueryPage bundles pagination parameters for QuerySubjects and QueryResources.
type QueryPage struct {
	Limit  *uint64
	Cursor *tuple.Relationship
}

// QueryDatastoreReader is the minimal datastore interface used by pkg/query.
// It exposes only the four logical operations actually performed by this package,
// returning PathSeq values directly so callers never touch raw relationship iterators.
type QueryDatastoreReader interface {
	// CheckRelationships finds paths for specific resource objects matched against
	// a subject. subject.ObjectID may be WildcardObjectID for wildcard checks.
	// All resource IDs must be of the same resourceType.
	CheckRelationships(
		ctx context.Context,
		resourceType ObjectType,
		resourceIDs []string,
		resourceRelation string,
		subject ObjectAndRelation,
		withCaveats, withExpiration bool,
	) (PathSeq, error)

	// QuerySubjects finds all subject paths for a resource.
	// If resource.ObjectID is empty, no resource ID filter is applied (wildcard expansion).
	// subjectType.Subrelation drives the ellipsis-vs-non-ellipsis filter.
	QuerySubjects(
		ctx context.Context,
		resource Object,
		resourceRelation string,
		subjectType ObjectType,
		withCaveats, withExpiration bool,
		page QueryPage,
	) (PathSeq, error)

	// QueryResources finds all resource paths for a subject.
	// subject.ObjectID may be WildcardObjectID for wildcard resource queries.
	QueryResources(
		ctx context.Context,
		resourceType string,
		resourceRelation string,
		subject ObjectAndRelation,
		withCaveats, withExpiration bool,
		page QueryPage,
	) (PathSeq, error)

	// SubjectExistsAsRelationship is an existence probe used by AliasIterator.
	// It includes expired relationships and returns true if any relationship
	// has the given subject with the specified non-ellipsis relation.
	SubjectExistsAsRelationship(
		ctx context.Context,
		subject Object,
		nonEllipsisRelation string,
	) (bool, error)

	// LookupCaveatDefinition fetches a single caveat definition by name.
	// Implementations are expected to cache results.
	LookupCaveatDefinition(
		ctx context.Context,
		name string,
	) (datastore.CaveatDefinition, error)
}

// NewQueryDatastoreReader wraps a datalayer.RevisionedReader as a QueryDatastoreReader.
func NewQueryDatastoreReader(r datalayer.RevisionedReader) QueryDatastoreReader {
	return &datalayerQueryDatastoreReader{inner: r}
}

type datalayerQueryDatastoreReader struct {
	inner datalayer.RevisionedReader
}

// convertRelationSeqToPathSeq converts an iter.Seq2[tuple.Relationship, error] from
// the datastore into a PathSeq by transforming each Relationship into a Path.
func convertRelationSeqToPathSeq(relSeq iter.Seq2[tuple.Relationship, error]) PathSeq {
	return func(yield func(*Path, error) bool) {
		for rel, err := range relSeq {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			if !yield(FromRelationship(rel), nil) {
				return
			}
		}
	}
}

// buildSubjectRelationFilter returns the appropriate SubjectRelationFilter for a
// given subrelation string: ellipsis → WithEllipsisRelation, otherwise → WithNonEllipsisRelation.
func buildSubjectRelationFilter(subrelation string) datastore.SubjectRelationFilter {
	if subrelation == tuple.Ellipsis {
		return datastore.SubjectRelationFilter{}.WithEllipsisRelation()
	}
	return datastore.SubjectRelationFilter{}.WithNonEllipsisRelation(subrelation)
}

func (r *datalayerQueryDatastoreReader) CheckRelationships(
	ctx context.Context,
	resourceType ObjectType,
	resourceIDs []string,
	resourceRelation string,
	subject ObjectAndRelation,
	withCaveats, withExpiration bool,
) (PathSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     resourceType.Type,
		OptionalResourceIds:      resourceIDs,
		OptionalResourceRelation: resourceRelation,
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: subject.ObjectType,
				OptionalSubjectIds:  []string{subject.ObjectID},
				RelationFilter:      buildSubjectRelationFilter(subject.Relation),
			},
		},
	}

	relIter, err := r.inner.QueryRelationships(ctx, filter,
		options.WithSkipCaveats(!withCaveats),
		options.WithSkipExpiration(!withExpiration),
		options.WithQueryShape(queryshape.CheckPermissionSelectDirectSubjects),
	)
	if err != nil {
		return nil, err
	}
	return convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter)), nil
}

func (r *datalayerQueryDatastoreReader) QuerySubjects(
	ctx context.Context,
	resource Object,
	resourceRelation string,
	subjectType ObjectType,
	withCaveats, withExpiration bool,
	page QueryPage,
) (PathSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: subjectType.Type,
				RelationFilter:      buildSubjectRelationFilter(subjectType.Subrelation),
			},
		},
	}
	// Non-empty fields constrain the query; empty means no constraint on that axis.
	if resource.ObjectType != "" {
		filter.OptionalResourceType = resource.ObjectType
	}
	if resource.ObjectID != "" {
		filter.OptionalResourceIds = []string{resource.ObjectID}
	}
	if resourceRelation != "" {
		filter.OptionalResourceRelation = resourceRelation
	}

	queryOpts := []options.QueryOptionsOption{
		options.WithSkipCaveats(!withCaveats),
		options.WithSkipExpiration(!withExpiration),
		options.WithQueryShape(queryshape.AllSubjectsForResources),
	}
	if page.Limit != nil {
		queryOpts = append(queryOpts,
			options.WithLimit(page.Limit),
			options.WithSort(options.ChooseEfficient),
		)
	}
	if page.Cursor != nil {
		queryOpts = append(queryOpts, options.WithAfter(options.ToCursor(*page.Cursor)))
	}

	relIter, err := r.inner.QueryRelationships(ctx, filter, queryOpts...)
	if err != nil {
		return nil, err
	}
	return convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter)), nil
}

func (r *datalayerQueryDatastoreReader) QueryResources(
	ctx context.Context,
	resourceType string,
	resourceRelation string,
	subject ObjectAndRelation,
	withCaveats, withExpiration bool,
	page QueryPage,
) (PathSeq, error) {
	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     resourceType,
		OptionalResourceRelation: resourceRelation,
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: subject.ObjectType,
				OptionalSubjectIds:  []string{subject.ObjectID},
				RelationFilter:      buildSubjectRelationFilter(subject.Relation),
			},
		},
	}

	queryOpts := []options.QueryOptionsOption{
		options.WithSkipCaveats(!withCaveats),
		options.WithSkipExpiration(!withExpiration),
		options.WithQueryShape(queryshape.MatchingResourcesForSubject),
	}
	if page.Limit != nil {
		queryOpts = append(queryOpts,
			options.WithLimit(page.Limit),
			options.WithSort(options.ChooseEfficient),
		)
	}
	if page.Cursor != nil {
		queryOpts = append(queryOpts, options.WithAfter(options.ToCursor(*page.Cursor)))
	}

	relIter, err := r.inner.QueryRelationships(ctx, filter, queryOpts...)
	if err != nil {
		return nil, err
	}
	return convertRelationSeqToPathSeq(iter.Seq2[tuple.Relationship, error](relIter)), nil
}

func (r *datalayerQueryDatastoreReader) SubjectExistsAsRelationship(
	ctx context.Context,
	subject Object,
	nonEllipsisRelation string,
) (bool, error) {
	filter := datastore.RelationshipsFilter{
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: subject.ObjectType,
				OptionalSubjectIds:  []string{subject.ObjectID},
				RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation(nonEllipsisRelation),
			},
		},
		OptionalExpirationOption: datastore.ExpirationFilterOptionNone,
	}

	relIter, err := r.inner.QueryRelationships(ctx, filter,
		options.WithLimit(&limitOne),
		options.WithSkipExpiration(true),
	)
	if err != nil {
		return false, err
	}

	for _, err := range relIter {
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (r *datalayerQueryDatastoreReader) LookupCaveatDefinition(
	ctx context.Context,
	name string,
) (datastore.CaveatDefinition, error) {
	sr, err := r.inner.ReadSchema(ctx)
	if err != nil {
		return nil, err
	}
	defs, err := sr.LookupCaveatDefinitionsByNames(ctx, []string{name})
	if err != nil {
		return nil, err
	}
	def, ok := defs[name]
	if !ok {
		return nil, datastore.NewCaveatNameNotFoundErr(name)
	}
	return def, nil
}
