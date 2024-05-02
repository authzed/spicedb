package development

import (
	"context"
	"fmt"
	"strings"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/typesystem"
)

var lintRelationReferencesParentType = func(
	ctx context.Context,
	relation *corev1.Relation,
	ts *typesystem.TypeSystem,
) (*devinterface.DeveloperWarning, error) {
	parentDef := ts.Namespace()
	if strings.HasSuffix(relation.Name, parentDef.Name) {
		if ts.IsPermission(relation.Name) {
			return warningForMetadata(
				fmt.Sprintf("Permission %q references parent type %q in its name; it is recommended to drop the suffix", relation.Name, parentDef.Name),
				relation,
			), nil
		}

		return warningForMetadata(
			fmt.Sprintf("Relation %q references parent type %q in its name; it is recommended to drop the suffix", relation.Name, parentDef.Name),
			relation,
		), nil
	}

	return nil, nil
}

var lintPermissionReferencingItself = func(
	ctx context.Context,
	computedUserset *corev1.ComputedUserset,
	sourcePosition *corev1.SourcePosition,
	ts *typesystem.TypeSystem,
) (*devinterface.DeveloperWarning, error) {
	parentRelation := ctx.Value(relationKey).(*corev1.Relation)
	permName := parentRelation.Name
	if computedUserset.Relation == permName {
		return warningForPosition(
			fmt.Sprintf("Permission %q references itself, which will cause an error to be raised due to infinite recursion", permName),
			sourcePosition,
		), nil
	}

	return nil, nil
}

var lintArrowReferencingUnreachable = func(
	ctx context.Context,
	ttu *corev1.TupleToUserset,
	sourcePosition *corev1.SourcePosition,
	ts *typesystem.TypeSystem,
) (*devinterface.DeveloperWarning, error) {
	parentRelation := ctx.Value(relationKey).(*corev1.Relation)

	referencedRelation, ok := ts.GetRelation(ttu.Tupleset.Relation)
	if !ok {
		return nil, nil
	}

	allowedSubjectTypes, err := ts.AllowedSubjectRelations(referencedRelation.Name)
	if err != nil {
		return nil, err
	}

	wasFound := false
	for _, subjectType := range allowedSubjectTypes {
		nts, err := ts.TypeSystemForNamespace(ctx, subjectType.Namespace)
		if err != nil {
			return nil, err
		}

		_, ok := nts.GetRelation(ttu.ComputedUserset.Relation)
		if ok {
			wasFound = true
		}
	}

	if !wasFound {
		return warningForPosition(
			fmt.Sprintf(
				"Arrow `%s->%s` under permission %q references relation/permission %q that does not exist on any subject types of relation %q",
				ttu.Tupleset.Relation,
				ttu.ComputedUserset.Relation,
				parentRelation.Name,
				ttu.ComputedUserset.Relation,
				ttu.Tupleset.Relation,
			),
			sourcePosition,
		), nil
	}

	return nil, nil
}

var lintArrowOverSubRelation = func(
	ctx context.Context,
	ttu *corev1.TupleToUserset,
	sourcePosition *corev1.SourcePosition,
	ts *typesystem.TypeSystem,
) (*devinterface.DeveloperWarning, error) {
	parentRelation := ctx.Value(relationKey).(*corev1.Relation)

	referencedRelation, ok := ts.GetRelation(ttu.Tupleset.Relation)
	if !ok {
		return nil, nil
	}

	allowedSubjectTypes, err := ts.AllowedSubjectRelations(referencedRelation.Name)
	if err != nil {
		return nil, err
	}

	for _, subjectType := range allowedSubjectTypes {
		if subjectType.Relation != tuple.Ellipsis {
			return warningForPosition(
				fmt.Sprintf(
					"Arrow `%s->%s` under permission %q references relation %q that has relation %q on subject %q: *the subject relation will be ignored for the arrow*",
					ttu.Tupleset.Relation,
					ttu.ComputedUserset.Relation,
					parentRelation.Name,
					ttu.Tupleset.Relation,
					subjectType.Relation,
					subjectType.Namespace,
				),
				sourcePosition,
			), nil
		}
	}

	return nil, nil
}

var lintArrowReferencingRelation = func(
	ctx context.Context,
	ttu *corev1.TupleToUserset,
	sourcePosition *corev1.SourcePosition,
	ts *typesystem.TypeSystem,
) (*devinterface.DeveloperWarning, error) {
	parentRelation := ctx.Value(relationKey).(*corev1.Relation)

	referencedRelation, ok := ts.GetRelation(ttu.Tupleset.Relation)
	if !ok {
		return nil, nil
	}

	// For each subject type of the referenced relation, check if the referenced permission
	// is, in fact, a relation.
	allowedSubjectTypes, err := ts.AllowedSubjectRelations(referencedRelation.Name)
	if err != nil {
		return nil, err
	}

	for _, subjectType := range allowedSubjectTypes {
		nts, err := ts.TypeSystemForNamespace(ctx, subjectType.Namespace)
		if err != nil {
			return nil, err
		}

		targetRelation, ok := nts.GetRelation(ttu.ComputedUserset.Relation)
		if !ok {
			continue
		}

		if !nts.IsPermission(targetRelation.Name) {
			return warningForPosition(
				fmt.Sprintf(
					"Arrow `%s->%s` under permission %q references relation %q on definition %q; it is recommended to point to a permission",
					ttu.Tupleset.Relation,
					ttu.ComputedUserset.Relation,
					parentRelation.Name,
					targetRelation.Name,
					subjectType.Namespace,
				),
				sourcePosition,
			), nil
		}
	}

	return nil, nil
}
