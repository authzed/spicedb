package development

import (
	"context"
	"fmt"
	"strings"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/tuple"
)

var lintRelationReferencesParentType = relationCheck{
	"relation-name-references-parent",
	func(
		ctx context.Context,
		relation *corev1.Relation,
		def *schema.Definition,
	) (*devinterface.DeveloperWarning, error) {
		parentDef := def.Namespace()
		if strings.HasSuffix(relation.Name, parentDef.Name) {
			if def.IsPermission(relation.Name) {
				return warningForMetadata(
					"relation-name-references-parent",
					fmt.Sprintf("Permission %q references parent type %q in its name; it is recommended to drop the suffix", relation.Name, parentDef.Name),
					relation.Name,
					relation,
				), nil
			}

			return warningForMetadata(
				"relation-name-references-parent",
				fmt.Sprintf("Relation %q references parent type %q in its name; it is recommended to drop the suffix", relation.Name, parentDef.Name),
				relation.Name,
				relation,
			), nil
		}

		return nil, nil
	},
}

var lintPermissionReferencingItself = computedUsersetCheck{
	"permission-references-itself",
	func(
		ctx context.Context,
		computedUserset *corev1.ComputedUserset,
		sourcePosition *corev1.SourcePosition,
		def *schema.Definition,
	) (*devinterface.DeveloperWarning, error) {
		parentRelation := ctx.Value(relationKey).(*corev1.Relation)
		permName := parentRelation.Name
		if computedUserset.GetRelation() == permName {
			return warningForPosition(
				"permission-references-itself",
				fmt.Sprintf("Permission %q references itself, which will cause an error to be raised due to infinite recursion", permName),
				permName,
				sourcePosition,
			), nil
		}

		return nil, nil
	},
}

var lintArrowReferencingUnreachable = ttuCheck{
	"arrow-references-unreachable-relation",
	func(
		ctx context.Context,
		ttu ttu,
		sourcePosition *corev1.SourcePosition,
		def *schema.Definition,
	) (*devinterface.DeveloperWarning, error) {
		parentRelation := ctx.Value(relationKey).(*corev1.Relation)

		referencedRelation, ok := def.GetRelation(ttu.GetTupleset().GetRelation())
		if !ok {
			return nil, nil
		}

		allowedSubjectTypes, err := def.AllowedSubjectRelations(referencedRelation.Name)
		if err != nil {
			return nil, err
		}

		wasFound := false
		for _, subjectType := range allowedSubjectTypes {
			nts, err := def.TypeSystem().GetDefinition(ctx, subjectType.Namespace)
			if err != nil {
				return nil, err
			}

			_, ok := nts.GetRelation(ttu.GetComputedUserset().GetRelation())
			if ok {
				wasFound = true
			}
		}

		if !wasFound {
			arrowString, err := ttu.GetArrowString()
			if err != nil {
				return nil, err
			}

			return warningForPosition(
				"arrow-references-unreachable-relation",
				fmt.Sprintf(
					"Arrow `%s` under permission %q references relation/permission %q that does not exist on any subject types of relation %q",
					arrowString,
					parentRelation.Name,
					ttu.GetComputedUserset().GetRelation(),
					ttu.GetTupleset().GetRelation(),
				),
				arrowString,
				sourcePosition,
			), nil
		}

		return nil, nil
	},
}

var lintArrowOverSubRelation = ttuCheck{
	"arrow-walks-subject-relation",
	func(
		ctx context.Context,
		ttu ttu,
		sourcePosition *corev1.SourcePosition,
		def *schema.Definition,
	) (*devinterface.DeveloperWarning, error) {
		parentRelation := ctx.Value(relationKey).(*corev1.Relation)

		referencedRelation, ok := def.GetRelation(ttu.GetTupleset().GetRelation())
		if !ok {
			return nil, nil
		}

		allowedSubjectTypes, err := def.AllowedSubjectRelations(referencedRelation.Name)
		if err != nil {
			return nil, err
		}

		arrowString, err := ttu.GetArrowString()
		if err != nil {
			return nil, err
		}

		for _, subjectType := range allowedSubjectTypes {
			if subjectType.GetRelation() != tuple.Ellipsis {
				return warningForPosition(
					"arrow-walks-subject-relation",
					fmt.Sprintf(
						"Arrow `%s` under permission %q references relation %q that has relation %q on subject %q: *the subject relation will be ignored for the arrow*",
						arrowString,
						parentRelation.Name,
						ttu.GetTupleset().GetRelation(),
						subjectType.GetRelation(),
						subjectType.Namespace,
					),
					arrowString,
					sourcePosition,
				), nil
			}
		}

		return nil, nil
	},
}

var lintArrowReferencingRelation = ttuCheck{
	"arrow-references-relation",
	func(
		ctx context.Context,
		ttu ttu,
		sourcePosition *corev1.SourcePosition,
		def *schema.Definition,
	) (*devinterface.DeveloperWarning, error) {
		parentRelation := ctx.Value(relationKey).(*corev1.Relation)

		referencedRelation, ok := def.GetRelation(ttu.GetTupleset().GetRelation())
		if !ok {
			return nil, nil
		}

		// For each subject type of the referenced relation, check if the referenced permission
		// is, in fact, a relation.
		allowedSubjectTypes, err := def.AllowedSubjectRelations(referencedRelation.Name)
		if err != nil {
			return nil, err
		}

		arrowString, err := ttu.GetArrowString()
		if err != nil {
			return nil, err
		}

		for _, subjectType := range allowedSubjectTypes {
			// Skip for arrow referencing relations in the same namespace.
			if subjectType.Namespace == def.Namespace().Name {
				continue
			}

			nts, err := def.TypeSystem().GetDefinition(ctx, subjectType.Namespace)
			if err != nil {
				return nil, err
			}

			targetRelation, ok := nts.GetRelation(ttu.GetComputedUserset().GetRelation())
			if !ok {
				continue
			}

			if !nts.IsPermission(targetRelation.Name) {
				return warningForPosition(
					"arrow-references-relation",
					fmt.Sprintf(
						"Arrow `%s` under permission %q references relation %q on definition %q; it is recommended to point to a permission",
						arrowString,
						parentRelation.Name,
						targetRelation.Name,
						subjectType.Namespace,
					),
					arrowString,
					sourcePosition,
				), nil
			}
		}

		return nil, nil
	},
}
