package v1

import (
	"context"
	"errors"
	"slices"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	implv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// BulkExport implements the BulkExportRelationships API functionality. Given a datastore.Datastore, it will
// export stream via the sender all relationships matched by the incoming request.
// If no cursor is provided, it will fallback to the provided revision.
func BulkExport(ctx context.Context, ds datastore.Datastore, batchSize uint64, req *v1.BulkExportRelationshipsRequest, fallbackRevision datastore.Revision, sender func(response *v1.BulkExportRelationshipsResponse) error) error {
	atRevision := fallbackRevision
	var curNamespace string
	var cur dsoptions.Cursor
	if req.OptionalCursor != nil {
		var err error
		atRevision, curNamespace, cur, err = decodeCursor(ds, req.OptionalCursor)
		if err != nil {
			return shared.RewriteErrorWithoutConfig(ctx, err)
		}
	}

	reader := ds.SnapshotReader(atRevision)

	namespaces, err := reader.ListAllNamespaces(ctx)
	if err != nil {
		return shared.RewriteErrorWithoutConfig(ctx, err)
	}

	// Make sure the namespaces are always in a stable order
	slices.SortFunc(namespaces, func(
		lhs datastore.RevisionedDefinition[*core.NamespaceDefinition],
		rhs datastore.RevisionedDefinition[*core.NamespaceDefinition],
	) int {
		return strings.Compare(lhs.Definition.Name, rhs.Definition.Name)
	})

	// Skip the namespaces that are already fully returned
	for cur != nil && len(namespaces) > 0 && namespaces[0].Definition.Name < curNamespace {
		namespaces = namespaces[1:]
	}

	limit := batchSize
	if req.OptionalLimit > 0 {
		limit = uint64(req.OptionalLimit)
	}

	// Pre-allocate all of the relationships that we might need in order to
	// make export easier and faster for the garbage collector.
	relsArray := make([]v1.Relationship, limit)
	objArray := make([]v1.ObjectReference, limit)
	subArray := make([]v1.SubjectReference, limit)
	subObjArray := make([]v1.ObjectReference, limit)
	caveatArray := make([]v1.ContextualizedCaveat, limit)
	for i := range relsArray {
		relsArray[i].Resource = &objArray[i]
		relsArray[i].Subject = &subArray[i]
		relsArray[i].Subject.Object = &subObjArray[i]
	}

	emptyRels := make([]*v1.Relationship, limit)
	for _, ns := range namespaces {
		rels := emptyRels

		// Reset the cursor between namespaces.
		if ns.Definition.Name != curNamespace {
			cur = nil
		}

		// Skip this namespace if a resource type filter was specified.
		if req.OptionalRelationshipFilter != nil && req.OptionalRelationshipFilter.ResourceType != "" {
			if ns.Definition.Name != req.OptionalRelationshipFilter.ResourceType {
				continue
			}
		}

		// Setup the filter to use for the relationships.
		relationshipFilter := datastore.RelationshipsFilter{OptionalResourceType: ns.Definition.Name}
		if req.OptionalRelationshipFilter != nil {
			rf, err := datastore.RelationshipsFilterFromPublicFilter(req.OptionalRelationshipFilter)
			if err != nil {
				return shared.RewriteErrorWithoutConfig(ctx, err)
			}

			// Overload the namespace name with the one from the request, because each iteration is for a different namespace.
			rf.OptionalResourceType = ns.Definition.Name
			relationshipFilter = rf
		}

		// We want to keep iterating as long as we're sending full batches.
		// To bootstrap this loop, we enter the first time with a full rels
		// slice of dummy rels that were never sent.
		for uint64(len(rels)) == limit {
			// Lop off any rels we've already sent
			rels = rels[:0]

			tplFn := func(tpl *core.RelationTuple) {
				offset := len(rels)
				rels = append(rels, &relsArray[offset]) // nozero
				tuple.CopyRelationTupleToRelationship(tpl, &relsArray[offset], &caveatArray[offset])
			}

			cur, err = queryForEach(
				ctx,
				reader,
				relationshipFilter,
				tplFn,
				dsoptions.WithLimit(&limit),
				dsoptions.WithAfter(cur),
				dsoptions.WithSort(dsoptions.ByResource),
			)
			if err != nil {
				return shared.RewriteErrorWithoutConfig(ctx, err)
			}

			if len(rels) == 0 {
				continue
			}

			encoded, err := cursor.Encode(&implv1.DecodedCursor{
				VersionOneof: &implv1.DecodedCursor_V1{
					V1: &implv1.V1Cursor{
						Revision: atRevision.String(),
						Sections: []string{
							ns.Definition.Name,
							tuple.MustString(cur),
						},
					},
				},
			})
			if err != nil {
				return shared.RewriteErrorWithoutConfig(ctx, err)
			}

			if err := sender(&v1.BulkExportRelationshipsResponse{
				AfterResultCursor: encoded,
				Relationships:     rels,
			}); err != nil {
				return shared.RewriteErrorWithoutConfig(ctx, err)
			}
		}
	}
	return nil
}

func decodeCursor(ds datastore.Datastore, encoded *v1.Cursor) (datastore.Revision, string, *core.RelationTuple, error) {
	decoded, err := cursor.Decode(encoded)
	if err != nil {
		return datastore.NoRevision, "", nil, err
	}

	if decoded.GetV1() == nil {
		return datastore.NoRevision, "", nil, errors.New("malformed cursor: no V1 in OneOf")
	}

	if len(decoded.GetV1().Sections) != 2 {
		return datastore.NoRevision, "", nil, errors.New("malformed cursor: wrong number of components")
	}

	atRevision, err := ds.RevisionFromString(decoded.GetV1().Revision)
	if err != nil {
		return datastore.NoRevision, "", nil, err
	}

	cur := tuple.Parse(decoded.GetV1().GetSections()[1])
	if cur == nil {
		return datastore.NoRevision, "", nil, errors.New("malformed cursor: invalid encoded relation tuple")
	}

	// Returns the current namespace and the cursor.
	return atRevision, decoded.GetV1().GetSections()[0], cur, nil
}

func queryForEach(
	ctx context.Context,
	reader datastore.Reader,
	filter datastore.RelationshipsFilter,
	fn func(tpl *core.RelationTuple),
	opts ...dsoptions.QueryOptionsOption,
) (*core.RelationTuple, error) {
	iter, err := reader.QueryRelationships(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var hadTuples bool
	for tpl := iter.Next(); tpl != nil; tpl = iter.Next() {
		fn(tpl)
		hadTuples = true
	}
	if iter.Err() != nil {
		return nil, err
	}

	var cur *core.RelationTuple
	if hadTuples {
		cur, err = iter.Cursor()
		iter.Close()
		if err != nil {
			return nil, err
		}
	}

	return cur, nil
}
