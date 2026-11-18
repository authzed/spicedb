package query

import (
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

// typedScanKey identifies source rows within one branch's fixed reader. It does
// not contain the requested Check subject: all subjects of the source relation
// are enumerated before the appropriate type/relation group is consumed.
type typedScanKey struct {
	reader   *datalayerQueryDatastoreReader
	resource Object
	relation string
	indirect bool
}

// hasMultipleScanForms recognizes native-compatible broad scans. Tuplesets
// contain only concrete ellipsis targets; indirect scans ignore direct forms.
// A single form keeps the existing path and does not allocate a scan cache.
func (r *DatastoreIterator) hasMultipleScanForms(indirect bool) bool {
	parent, ok := r.base.Parent().(*schema.Relation)
	if !ok || r.base.Wildcard() {
		return false
	}

	first := ""
	multiple := false

	for _, br := range parent.BaseRelations() {
		nonEllipsis := br.Subrelation() != "" && br.Subrelation() != tuple.Ellipsis
		if !indirect && (br.Wildcard() || nonEllipsis) {
			return false
		}

		if indirect && (br.Wildcard() || !nonEllipsis) {
			continue
		}

		form := br.Type() + "#" + br.Subrelation()

		if first == "" {
			first = form
		} else if first != form {
			multiple = true
		}
	}

	return multiple
}

// sharedTypedSubjects materializes a native broad source scan once, then returns
// owned paths for this leaf's type/relation. Cached entries contain immutable
// rows, belong to this execution branch, and are copied by forkForCheck. No
// partial, failed or canceled scan becomes a completed empty group.
func sharedTypedSubjects(ctx *Context, leaf *DatastoreIterator, resources []Object) ([][]*Path, bool, error) {
	reader, ok := ctx.Reader.(*datalayerQueryDatastoreReader)
	if !ok || ctx.PaginationLimit != nil || leaf.base.Wildcard() {
		return nil, false, nil
	}

	indirect := leaf.base.Subrelation() != "" && leaf.base.Subrelation() != tuple.Ellipsis
	enabled := ctx.checkExecution.UnfilteredSingleTypeReads

	if indirect {
		enabled = ctx.checkExecution.BroadSingleUsersetReads
	}

	if !enabled || !leaf.hasMultipleScanForms(indirect) {
		return nil, false, nil
	}

	if err := ctx.Err(); err != nil {
		return nil, true, err
	}

	if ctx.typedScans == nil {
		ctx.typedScans = map[typedScanKey][]*Path{}
	}

	missing := map[Object]bool{}
	ids := []string{}

	for _, resource := range resources {
		if resource.ObjectType != leaf.base.DefinitionName() || resource.ObjectID == "" {
			return nil, false, nil
		}

		key := typedScanKey{reader, resource, leaf.base.RelationName(), indirect}

		if _, found := ctx.typedScans[key]; !found && !missing[resource] {
			missing[resource] = true
			ids = append(ids, resource.ObjectID)
		}
	}

	if len(ids) > 0 {
		filter := datastore.RelationshipsFilter{
			OptionalResourceType:     leaf.base.DefinitionName(),
			OptionalResourceIds:      ids,
			OptionalResourceRelation: leaf.base.RelationName(),
		}
		shape := queryshape.AllSubjectsForResources

		if indirect {
			filter.OptionalSubjectsSelectors = []datastore.SubjectsSelector{
				{RelationFilter: datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations()},
			}
			shape = queryshape.CheckPermissionSelectIndirectSubjects
		}

		seq, err := reader.inner.QueryRelationships(ctx, filter,
			options.WithSkipCaveats(!leaf.relationHasCaveats()),
			options.WithSkipExpiration(!leaf.relationHasExpiration()),
			options.WithQueryShape(shape))
		if err != nil {
			return nil, true, err
		}

		rows := map[Object][]*Path{}

		for rel, err := range seq {
			if err != nil {
				return nil, true, err
			}

			path := FromRelationship(rel)
			rows[path.Resource] = append(rows[path.Resource], path)
		}

		if err := ctx.Err(); err != nil {
			return nil, true, err
		}

		for resource := range missing {
			ctx.typedScans[typedScanKey{reader, resource, leaf.base.RelationName(), indirect}] = rows[resource]
		}
	}

	out := make([][]*Path, len(resources))

	for i, resource := range resources {
		for _, path := range ctx.typedScans[typedScanKey{reader, resource, leaf.base.RelationName(), indirect}] {
			if path.Subject.ObjectType == leaf.base.Type() && path.Subject.Relation == leaf.base.Subrelation() {
				out[i] = append(out[i], ownedPath(path))
			}
		}
	}

	return out, true, nil
}
