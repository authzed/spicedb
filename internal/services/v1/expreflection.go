package v1

import (
	"fmt"
	"sort"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"golang.org/x/exp/maps"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/diff"
	caveatdiff "github.com/authzed/spicedb/pkg/diff/caveats"
	nsdiff "github.com/authzed/spicedb/pkg/diff/namespace"
	"github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

// convertDiff converts a schema diff into an API response.
func convertDiff(
	diff *diff.SchemaDiff,
	existingSchema *diff.DiffableSchema,
	comparisonSchema *diff.DiffableSchema,
	atRevision datastore.Revision,
) (*v1.ExperimentalSchemaDiffResponse, error) {
	size := len(diff.AddedNamespaces) + len(diff.RemovedNamespaces) + len(diff.AddedCaveats) + len(diff.RemovedCaveats) + len(diff.ChangedNamespaces) + len(diff.ChangedCaveats)
	diffs := make([]*v1.ExpSchemaDiff, 0, size)

	// Add/remove namespaces.
	for _, ns := range diff.AddedNamespaces {
		nsDef, err := namespaceAPIReprForName(ns, comparisonSchema)
		if err != nil {
			return nil, err
		}

		diffs = append(diffs, &v1.ExpSchemaDiff{
			Diff: &v1.ExpSchemaDiff_DefinitionAdded{
				DefinitionAdded: nsDef,
			},
		})
	}

	for _, ns := range diff.RemovedNamespaces {
		nsDef, err := namespaceAPIReprForName(ns, existingSchema)
		if err != nil {
			return nil, err
		}

		diffs = append(diffs, &v1.ExpSchemaDiff{
			Diff: &v1.ExpSchemaDiff_DefinitionRemoved{
				DefinitionRemoved: nsDef,
			},
		})
	}

	// Add/remove caveats.
	for _, caveat := range diff.AddedCaveats {
		caveatDef, err := caveatAPIReprForName(caveat, comparisonSchema)
		if err != nil {
			return nil, err
		}

		diffs = append(diffs, &v1.ExpSchemaDiff{
			Diff: &v1.ExpSchemaDiff_CaveatAdded{
				CaveatAdded: caveatDef,
			},
		})
	}

	for _, caveat := range diff.RemovedCaveats {
		caveatDef, err := caveatAPIReprForName(caveat, existingSchema)
		if err != nil {
			return nil, err
		}

		diffs = append(diffs, &v1.ExpSchemaDiff{
			Diff: &v1.ExpSchemaDiff_CaveatRemoved{
				CaveatRemoved: caveatDef,
			},
		})
	}

	// Changed namespaces.
	for nsName, nsDiff := range diff.ChangedNamespaces {
		for _, delta := range nsDiff.Deltas() {
			switch delta.Type {
			case nsdiff.AddedPermission:
				permission, ok := comparisonSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, fmt.Errorf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				perm, err := permissionAPIRepr(permission, nsName)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_PermissionAdded{
						PermissionAdded: perm,
					},
				})

			case nsdiff.AddedRelation:
				relation, ok := comparisonSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, fmt.Errorf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := relationAPIRepr(relation, nsName)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_RelationAdded{
						RelationAdded: rel,
					},
				})

			case nsdiff.ChangedPermissionComment:
				permission, ok := comparisonSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, fmt.Errorf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				perm, err := permissionAPIRepr(permission, nsName)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_PermissionDocCommentChanged{
						PermissionDocCommentChanged: perm,
					},
				})

			case nsdiff.ChangedPermissionImpl:
				permission, ok := comparisonSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, fmt.Errorf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				perm, err := permissionAPIRepr(permission, nsName)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_PermissionExprChanged{
						PermissionExprChanged: perm,
					},
				})

			case nsdiff.ChangedRelationComment:
				relation, ok := comparisonSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, fmt.Errorf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := relationAPIRepr(relation, nsName)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_RelationDocCommentChanged{
						RelationDocCommentChanged: rel,
					},
				})

			case nsdiff.LegacyChangedRelationImpl:
				return nil, spiceerrors.MustBugf("legacy relation implementation changes are not supported")

			case nsdiff.NamespaceCommentsChanged:
				def, err := namespaceAPIReprForName(nsName, comparisonSchema)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_DefinitionDocCommentChanged{
						DefinitionDocCommentChanged: def,
					},
				})

			case nsdiff.RelationAllowedTypeRemoved:
				relation, ok := comparisonSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, fmt.Errorf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := relationAPIRepr(relation, nsName)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_RelationSubjectTypeRemoved{
						RelationSubjectTypeRemoved: &v1.ExpRelationSubjectTypeChange{
							Relation:           rel,
							ChangedSubjectType: typeAPIRepr(delta.AllowedType),
						},
					},
				})

			case nsdiff.RelationAllowedTypeAdded:
				relation, ok := comparisonSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, fmt.Errorf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := relationAPIRepr(relation, nsName)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_RelationSubjectTypeAdded{
						RelationSubjectTypeAdded: &v1.ExpRelationSubjectTypeChange{
							Relation:           rel,
							ChangedSubjectType: typeAPIRepr(delta.AllowedType),
						},
					},
				})

			case nsdiff.RemovedPermission:
				permission, ok := existingSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, fmt.Errorf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				perm, err := permissionAPIRepr(permission, nsName)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_PermissionRemoved{
						PermissionRemoved: perm,
					},
				})

			case nsdiff.RemovedRelation:
				relation, ok := existingSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, fmt.Errorf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := relationAPIRepr(relation, nsName)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_RelationRemoved{
						RelationRemoved: rel,
					},
				})

			case nsdiff.NamespaceAdded:
				return nil, spiceerrors.MustBugf("should be handled above")

			case nsdiff.NamespaceRemoved:
				return nil, spiceerrors.MustBugf("should be handled above")

			default:
				return nil, spiceerrors.MustBugf("unexpected delta type %v", delta.Type)
			}
		}
	}

	// Changed caveats.
	for caveatName, caveatDiff := range diff.ChangedCaveats {
		for _, delta := range caveatDiff.Deltas() {
			switch delta.Type {
			case caveatdiff.CaveatCommentsChanged:
				caveat, err := caveatAPIReprForName(caveatName, comparisonSchema)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_CaveatDocCommentChanged{
						CaveatDocCommentChanged: caveat,
					},
				})

			case caveatdiff.AddedParameter:
				paramDef, err := caveatAPIParamRepr(delta.ParameterName, caveatName, comparisonSchema)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_CaveatParameterAdded{
						CaveatParameterAdded: paramDef,
					},
				})

			case caveatdiff.RemovedParameter:
				paramDef, err := caveatAPIParamRepr(delta.ParameterName, caveatName, existingSchema)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_CaveatParameterRemoved{
						CaveatParameterRemoved: paramDef,
					},
				})

			case caveatdiff.ParameterTypeChanged:
				previousParamDef, err := caveatAPIParamRepr(delta.ParameterName, caveatName, existingSchema)
				if err != nil {
					return nil, err
				}

				paramDef, err := caveatAPIParamRepr(delta.ParameterName, caveatName, comparisonSchema)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_CaveatParameterTypeChanged{
						CaveatParameterTypeChanged: &v1.ExpCaveatParameterTypeChange{
							Parameter:    paramDef,
							PreviousType: previousParamDef.Type,
						},
					},
				})

			case caveatdiff.CaveatExpressionChanged:
				caveat, err := caveatAPIReprForName(caveatName, comparisonSchema)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_CaveatExprChanged{
						CaveatExprChanged: caveat,
					},
				})

			case caveatdiff.CaveatAdded:
				return nil, spiceerrors.MustBugf("should be handled above")

			case caveatdiff.CaveatRemoved:
				return nil, spiceerrors.MustBugf("should be handled above")

			default:
				return nil, spiceerrors.MustBugf("unexpected delta type %v", delta.Type)
			}
		}
	}

	return &v1.ExperimentalSchemaDiffResponse{
		Diffs:  diffs,
		ReadAt: zedtoken.MustNewFromRevision(atRevision),
	}, nil
}

// namespaceAPIReprForName builds an API representation of a namespace.
func namespaceAPIReprForName(namespaceName string, schema *diff.DiffableSchema) (*v1.ExpDefinition, error) {
	nsDef, ok := schema.GetNamespace(namespaceName)
	if !ok {
		return nil, fmt.Errorf("namespace %q not found in schema", namespaceName)
	}

	return namespaceAPIRepr(nsDef)
}

func namespaceAPIRepr(nsDef *core.NamespaceDefinition) (*v1.ExpDefinition, error) {
	relations := make([]*v1.ExpRelation, 0, len(nsDef.Relation))
	permissions := make([]*v1.ExpPermission, 0, len(nsDef.Relation))

	for _, rel := range nsDef.Relation {
		if namespace.GetRelationKind(rel) == iv1.RelationMetadata_PERMISSION {
			permission, err := permissionAPIRepr(rel, nsDef.Name)
			if err != nil {
				return nil, err
			}

			permissions = append(permissions, permission)
			continue
		}

		relation, err := relationAPIRepr(rel, nsDef.Name)
		if err != nil {
			return nil, err
		}

		relations = append(relations, relation)
	}

	comments := namespace.GetComments(nsDef.Metadata)
	return &v1.ExpDefinition{
		Name:        nsDef.Name,
		Comment:     strings.Join(comments, "\n"),
		Relations:   relations,
		Permissions: permissions,
	}, nil
}

// permissionAPIRepr builds an API representation of a permission.
func permissionAPIRepr(relation *core.Relation, parentDefName string) (*v1.ExpPermission, error) {
	comments := namespace.GetComments(relation.Metadata)
	return &v1.ExpPermission{
		Name:                 relation.Name,
		Comment:              strings.Join(comments, "\n"),
		ParentDefinitionName: parentDefName,
	}, nil
}

// relationAPIRepresentation builds an API representation of a relation.
func relationAPIRepr(relation *core.Relation, parentDefName string) (*v1.ExpRelation, error) {
	comments := namespace.GetComments(relation.Metadata)

	var subjectTypes []*v1.ExpTypeReference
	if relation.TypeInformation != nil {
		subjectTypes = make([]*v1.ExpTypeReference, 0, len(relation.TypeInformation.AllowedDirectRelations))
		for _, subjectType := range relation.TypeInformation.AllowedDirectRelations {
			typeref := typeAPIRepr(subjectType)
			subjectTypes = append(subjectTypes, typeref)
		}
	}

	return &v1.ExpRelation{
		Name:                 relation.Name,
		Comment:              strings.Join(comments, "\n"),
		ParentDefinitionName: parentDefName,
		SubjectTypes:         subjectTypes,
	}, nil
}

// typeAPIRepr builds an API representation of a type.
func typeAPIRepr(subjectType *core.AllowedRelation) *v1.ExpTypeReference {
	typeref := &v1.ExpTypeReference{
		SubjectDefinitionName: subjectType.Namespace,
		Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
	}

	if subjectType.GetRelation() != tuple.Ellipsis {
		typeref.Typeref = &v1.ExpTypeReference_OptionalRelationName{
			OptionalRelationName: subjectType.GetRelation(),
		}
	} else if subjectType.GetPublicWildcard() != nil {
		typeref.Typeref = &v1.ExpTypeReference_IsPublicWildcard{}
	}

	if subjectType.GetRequiredCaveat() != nil {
		typeref.OptionalCaveatName = subjectType.GetRequiredCaveat().CaveatName
	}

	return typeref
}

// caveatAPIReprForName builds an API representation of a caveat.
func caveatAPIReprForName(caveatName string, schema *diff.DiffableSchema) (*v1.ExpCaveat, error) {
	caveatDef, ok := schema.GetCaveat(caveatName)
	if !ok {
		return nil, fmt.Errorf("caveat %q not found in schema", caveatName)
	}

	return caveatAPIRepr(caveatDef)
}

// caveatAPIRepr builds an API representation of a caveat.
func caveatAPIRepr(caveatDef *core.CaveatDefinition) (*v1.ExpCaveat, error) {
	parameters := make([]*v1.ExpCaveatParameter, 0, len(caveatDef.ParameterTypes))
	paramNames := maps.Keys(caveatDef.ParameterTypes)
	sort.Strings(paramNames)

	for _, paramName := range paramNames {
		paramType, ok := caveatDef.ParameterTypes[paramName]
		if !ok {
			return nil, fmt.Errorf("parameter %q not found in caveat %q", paramName, caveatDef.Name)
		}

		decoded, err := caveattypes.DecodeParameterType(paramType)
		if err != nil {
			return nil, fmt.Errorf("invalid parameter type on caveat: %w", err)
		}

		parameters = append(parameters, &v1.ExpCaveatParameter{
			Name:             paramName,
			Type:             decoded.String(),
			ParentCaveatName: caveatDef.Name,
		})
	}

	parameterTypes, err := caveattypes.DecodeParameterTypes(caveatDef.ParameterTypes)
	if err != nil {
		return nil, fmt.Errorf("invalid caveat parameters: %w", err)
	}

	deserializedExpression, err := caveats.DeserializeCaveat(caveatDef.SerializedExpression, parameterTypes)
	if err != nil {
		return nil, fmt.Errorf("invalid caveat expression bytes: %w", err)
	}

	exprString, err := deserializedExpression.ExprString()
	if err != nil {
		return nil, fmt.Errorf("invalid caveat expression: %w", err)
	}

	comments := namespace.GetComments(caveatDef.Metadata)
	return &v1.ExpCaveat{
		Name:       caveatDef.Name,
		Comment:    strings.Join(comments, "\n"),
		Parameters: parameters,
		Expression: exprString,
	}, nil
}

// caveatAPIParamRepresentation builds an API representation of a caveat parameter.
func caveatAPIParamRepr(paramName, parentCaveatName string, schema *diff.DiffableSchema) (*v1.ExpCaveatParameter, error) {
	caveatDef, ok := schema.GetCaveat(parentCaveatName)
	if !ok {
		return nil, fmt.Errorf("caveat %q not found in schema", parentCaveatName)
	}

	paramType, ok := caveatDef.ParameterTypes[paramName]
	if !ok {
		return nil, fmt.Errorf("parameter %q not found in caveat %q", paramName, parentCaveatName)
	}

	decoded, err := caveattypes.DecodeParameterType(paramType)
	if err != nil {
		return nil, fmt.Errorf("invalid parameter type on caveat: %w", err)
	}

	return &v1.ExpCaveatParameter{
		Name:             paramName,
		Type:             decoded.String(),
		ParentCaveatName: parentCaveatName,
	}, nil
}
