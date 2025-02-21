package v1

import (
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

type expSchemaFilters struct {
	filters []*v1.ExpSchemaFilter
}

func newexpSchemaFilters(filters []*v1.ExpSchemaFilter) (*expSchemaFilters, error) {
	for _, filter := range filters {
		if filter.OptionalDefinitionNameFilter != "" {
			if filter.OptionalCaveatNameFilter != "" {
				return nil, NewInvalidFilterErr("cannot filter by both definition and caveat name", filter.String())
			}
		}

		if filter.OptionalRelationNameFilter != "" {
			if filter.OptionalDefinitionNameFilter == "" {
				return nil, NewInvalidFilterErr("relation name match requires definition name match", filter.String())
			}

			if filter.OptionalPermissionNameFilter != "" {
				return nil, NewInvalidFilterErr("cannot filter by both relation and permission name", filter.String())
			}
		}

		if filter.OptionalPermissionNameFilter != "" {
			if filter.OptionalDefinitionNameFilter == "" {
				return nil, NewInvalidFilterErr("permission name match requires definition name match", filter.String())
			}
		}
	}

	return &expSchemaFilters{filters: filters}, nil
}

func (sf *expSchemaFilters) HasNamespaces() bool {
	if len(sf.filters) == 0 {
		return true
	}

	for _, filter := range sf.filters {
		if filter.OptionalDefinitionNameFilter != "" {
			return true
		}
	}

	return false
}

func (sf *expSchemaFilters) HasCaveats() bool {
	if len(sf.filters) == 0 {
		return true
	}

	for _, filter := range sf.filters {
		if filter.OptionalCaveatNameFilter != "" {
			return true
		}
	}

	return false
}

func (sf *expSchemaFilters) HasNamespace(namespaceName string) bool {
	if len(sf.filters) == 0 {
		return true
	}

	hasDefinitionFilter := false
	for _, filter := range sf.filters {
		if filter.OptionalDefinitionNameFilter == "" {
			continue
		}

		hasDefinitionFilter = true
		isMatch := strings.HasPrefix(namespaceName, filter.OptionalDefinitionNameFilter)
		if isMatch {
			return true
		}
	}

	return !hasDefinitionFilter
}

func (sf *expSchemaFilters) HasCaveat(caveatName string) bool {
	if len(sf.filters) == 0 {
		return true
	}

	hasCaveatFilter := false
	for _, filter := range sf.filters {
		if filter.OptionalCaveatNameFilter == "" {
			continue
		}

		hasCaveatFilter = true
		isMatch := strings.HasPrefix(caveatName, filter.OptionalCaveatNameFilter)
		if isMatch {
			return true
		}
	}

	return !hasCaveatFilter
}

func (sf *expSchemaFilters) HasRelation(namespaceName, relationName string) bool {
	if len(sf.filters) == 0 {
		return true
	}

	hasRelationFilter := false
	for _, filter := range sf.filters {
		if filter.OptionalRelationNameFilter == "" {
			continue
		}

		hasRelationFilter = true
		isMatch := strings.HasPrefix(relationName, filter.OptionalRelationNameFilter)
		if !isMatch {
			continue
		}

		isMatch = strings.HasPrefix(namespaceName, filter.OptionalDefinitionNameFilter)
		if isMatch {
			return true
		}
	}

	return !hasRelationFilter
}

func (sf *expSchemaFilters) HasPermission(namespaceName, permissionName string) bool {
	if len(sf.filters) == 0 {
		return true
	}

	hasPermissionFilter := false
	for _, filter := range sf.filters {
		if filter.OptionalPermissionNameFilter == "" {
			continue
		}

		hasPermissionFilter = true
		isMatch := strings.HasPrefix(permissionName, filter.OptionalPermissionNameFilter)
		if !isMatch {
			continue
		}

		isMatch = strings.HasPrefix(namespaceName, filter.OptionalDefinitionNameFilter)
		if isMatch {
			return true
		}
	}

	return !hasPermissionFilter
}

// expConvertDiff converts a schema diff into an API response.
func expConvertDiff(
	diff *diff.SchemaDiff,
	existingSchema *diff.DiffableSchema,
	comparisonSchema *diff.DiffableSchema,
	atRevision datastore.Revision,
) (*v1.ExperimentalDiffSchemaResponse, error) {
	size := len(diff.AddedNamespaces) + len(diff.RemovedNamespaces) + len(diff.AddedCaveats) + len(diff.RemovedCaveats) + len(diff.ChangedNamespaces) + len(diff.ChangedCaveats)
	diffs := make([]*v1.ExpSchemaDiff, 0, size)

	// Add/remove namespaces.
	for _, ns := range diff.AddedNamespaces {
		nsDef, err := expNamespaceAPIReprForName(ns, comparisonSchema)
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
		nsDef, err := expNamespaceAPIReprForName(ns, existingSchema)
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
		caveatDef, err := expCaveatAPIReprForName(caveat, comparisonSchema)
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
		caveatDef, err := expCaveatAPIReprForName(caveat, existingSchema)
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
					return nil, spiceerrors.MustBugf("permission %q not found in namespace %q", delta.RelationName, nsName)
				}

				perm, err := expPermissionAPIRepr(permission, nsName, nil)
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
					return nil, spiceerrors.MustBugf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := expRelationAPIRepr(relation, nsName, nil)
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
					return nil, spiceerrors.MustBugf("permission %q not found in namespace %q", delta.RelationName, nsName)
				}

				perm, err := expPermissionAPIRepr(permission, nsName, nil)
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
					return nil, spiceerrors.MustBugf("permission %q not found in namespace %q", delta.RelationName, nsName)
				}

				perm, err := expPermissionAPIRepr(permission, nsName, nil)
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
					return nil, spiceerrors.MustBugf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := expRelationAPIRepr(relation, nsName, nil)
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
				def, err := expNamespaceAPIReprForName(nsName, comparisonSchema)
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
					return nil, spiceerrors.MustBugf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := expRelationAPIRepr(relation, nsName, nil)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_RelationSubjectTypeRemoved{
						RelationSubjectTypeRemoved: &v1.ExpRelationSubjectTypeChange{
							Relation:           rel,
							ChangedSubjectType: expTypeAPIRepr(delta.AllowedType),
						},
					},
				})

			case nsdiff.RelationAllowedTypeAdded:
				relation, ok := comparisonSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, spiceerrors.MustBugf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := expRelationAPIRepr(relation, nsName, nil)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_RelationSubjectTypeAdded{
						RelationSubjectTypeAdded: &v1.ExpRelationSubjectTypeChange{
							Relation:           rel,
							ChangedSubjectType: expTypeAPIRepr(delta.AllowedType),
						},
					},
				})

			case nsdiff.RemovedPermission:
				permission, ok := existingSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, spiceerrors.MustBugf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				perm, err := expPermissionAPIRepr(permission, nsName, nil)
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
					return nil, spiceerrors.MustBugf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := expRelationAPIRepr(relation, nsName, nil)
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
				caveat, err := expCaveatAPIReprForName(caveatName, comparisonSchema)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_CaveatDocCommentChanged{
						CaveatDocCommentChanged: caveat,
					},
				})

			case caveatdiff.AddedParameter:
				paramDef, err := expCaveatAPIParamRepr(delta.ParameterName, caveatName, comparisonSchema)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_CaveatParameterAdded{
						CaveatParameterAdded: paramDef,
					},
				})

			case caveatdiff.RemovedParameter:
				paramDef, err := expCaveatAPIParamRepr(delta.ParameterName, caveatName, existingSchema)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ExpSchemaDiff{
					Diff: &v1.ExpSchemaDiff_CaveatParameterRemoved{
						CaveatParameterRemoved: paramDef,
					},
				})

			case caveatdiff.ParameterTypeChanged:
				previousParamDef, err := expCaveatAPIParamRepr(delta.ParameterName, caveatName, existingSchema)
				if err != nil {
					return nil, err
				}

				paramDef, err := expCaveatAPIParamRepr(delta.ParameterName, caveatName, comparisonSchema)
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
				caveat, err := expCaveatAPIReprForName(caveatName, comparisonSchema)
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

	return &v1.ExperimentalDiffSchemaResponse{
		Diffs:  diffs,
		ReadAt: zedtoken.MustNewFromRevision(atRevision),
	}, nil
}

// expNamespaceAPIReprForName builds an API representation of a namespace.
func expNamespaceAPIReprForName(namespaceName string, schema *diff.DiffableSchema) (*v1.ExpDefinition, error) {
	nsDef, ok := schema.GetNamespace(namespaceName)
	if !ok {
		return nil, spiceerrors.MustBugf("namespace %q not found in schema", namespaceName)
	}

	return expNamespaceAPIRepr(nsDef, nil)
}

func expNamespaceAPIRepr(nsDef *core.NamespaceDefinition, expSchemaFilters *expSchemaFilters) (*v1.ExpDefinition, error) {
	if expSchemaFilters != nil && !expSchemaFilters.HasNamespace(nsDef.Name) {
		return nil, nil
	}

	relations := make([]*v1.ExpRelation, 0, len(nsDef.Relation))
	permissions := make([]*v1.ExpPermission, 0, len(nsDef.Relation))

	for _, rel := range nsDef.Relation {
		if namespace.GetRelationKind(rel) == iv1.RelationMetadata_PERMISSION {
			permission, err := expPermissionAPIRepr(rel, nsDef.Name, expSchemaFilters)
			if err != nil {
				return nil, err
			}

			if permission != nil {
				permissions = append(permissions, permission)
			}
			continue
		}

		relation, err := expRelationAPIRepr(rel, nsDef.Name, expSchemaFilters)
		if err != nil {
			return nil, err
		}

		if relation != nil {
			relations = append(relations, relation)
		}
	}

	comments := namespace.GetComments(nsDef.Metadata)
	return &v1.ExpDefinition{
		Name:        nsDef.Name,
		Comment:     strings.Join(comments, "\n"),
		Relations:   relations,
		Permissions: permissions,
	}, nil
}

// expPermissionAPIRepr builds an API representation of a permission.
func expPermissionAPIRepr(relation *core.Relation, parentDefName string, expSchemaFilters *expSchemaFilters) (*v1.ExpPermission, error) {
	if expSchemaFilters != nil && !expSchemaFilters.HasPermission(parentDefName, relation.Name) {
		return nil, nil
	}

	comments := namespace.GetComments(relation.Metadata)
	return &v1.ExpPermission{
		Name:                 relation.Name,
		Comment:              strings.Join(comments, "\n"),
		ParentDefinitionName: parentDefName,
	}, nil
}

// expRelationAPIRepr builds an API representation of a relation.
func expRelationAPIRepr(relation *core.Relation, parentDefName string, expSchemaFilters *expSchemaFilters) (*v1.ExpRelation, error) {
	if expSchemaFilters != nil && !expSchemaFilters.HasRelation(parentDefName, relation.Name) {
		return nil, nil
	}

	comments := namespace.GetComments(relation.Metadata)

	var subjectTypes []*v1.ExpTypeReference
	if relation.TypeInformation != nil {
		subjectTypes = make([]*v1.ExpTypeReference, 0, len(relation.TypeInformation.AllowedDirectRelations))
		for _, subjectType := range relation.TypeInformation.AllowedDirectRelations {
			typeref := expTypeAPIRepr(subjectType)
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

// expTypeAPIRepr builds an API representation of a type.
func expTypeAPIRepr(subjectType *core.AllowedRelation) *v1.ExpTypeReference {
	typeref := &v1.ExpTypeReference{
		SubjectDefinitionName: subjectType.Namespace,
		Typeref:               &v1.ExpTypeReference_IsTerminalSubject{},
	}

	if subjectType.GetRelation() != tuple.Ellipsis && subjectType.GetRelation() != "" {
		typeref.Typeref = &v1.ExpTypeReference_OptionalRelationName{
			OptionalRelationName: subjectType.GetRelation(),
		}
	} else if subjectType.GetPublicWildcard() != nil {
		typeref.Typeref = &v1.ExpTypeReference_IsPublicWildcard{
			IsPublicWildcard: true,
		}
	}

	if subjectType.GetRequiredCaveat() != nil {
		typeref.OptionalCaveatName = subjectType.GetRequiredCaveat().CaveatName
	}

	return typeref
}

// expCaveatAPIReprForName builds an API representation of a caveat.
func expCaveatAPIReprForName(caveatName string, schema *diff.DiffableSchema) (*v1.ExpCaveat, error) {
	caveatDef, ok := schema.GetCaveat(caveatName)
	if !ok {
		return nil, spiceerrors.MustBugf("caveat %q not found in schema", caveatName)
	}

	return expCaveatAPIRepr(caveatDef, nil)
}

// expCaveatAPIRepr builds an API representation of a caveat.
func expCaveatAPIRepr(caveatDef *core.CaveatDefinition, expSchemaFilters *expSchemaFilters) (*v1.ExpCaveat, error) {
	if expSchemaFilters != nil && !expSchemaFilters.HasCaveat(caveatDef.Name) {
		return nil, nil
	}

	parameters := make([]*v1.ExpCaveatParameter, 0, len(caveatDef.ParameterTypes))
	paramNames := maps.Keys(caveatDef.ParameterTypes)
	sort.Strings(paramNames)

	for _, paramName := range paramNames {
		paramType, ok := caveatDef.ParameterTypes[paramName]
		if !ok {
			return nil, spiceerrors.MustBugf("parameter %q not found in caveat %q", paramName, caveatDef.Name)
		}

		decoded, err := caveattypes.DecodeParameterType(paramType)
		if err != nil {
			return nil, spiceerrors.MustBugf("invalid parameter type on caveat: %v", err)
		}

		parameters = append(parameters, &v1.ExpCaveatParameter{
			Name:             paramName,
			Type:             decoded.String(),
			ParentCaveatName: caveatDef.Name,
		})
	}

	parameterTypes, err := caveattypes.DecodeParameterTypes(caveatDef.ParameterTypes)
	if err != nil {
		return nil, spiceerrors.MustBugf("invalid caveat parameters: %v", err)
	}

	deserializedExpression, err := caveats.DeserializeCaveat(caveatDef.SerializedExpression, parameterTypes)
	if err != nil {
		return nil, spiceerrors.MustBugf("invalid caveat expression bytes: %v", err)
	}

	exprString, err := deserializedExpression.ExprString()
	if err != nil {
		return nil, spiceerrors.MustBugf("invalid caveat expression: %v", err)
	}

	comments := namespace.GetComments(caveatDef.Metadata)
	return &v1.ExpCaveat{
		Name:       caveatDef.Name,
		Comment:    strings.Join(comments, "\n"),
		Parameters: parameters,
		Expression: exprString,
	}, nil
}

// expCaveatAPIParamRepr builds an API representation of a caveat parameter.
func expCaveatAPIParamRepr(paramName, parentCaveatName string, schema *diff.DiffableSchema) (*v1.ExpCaveatParameter, error) {
	caveatDef, ok := schema.GetCaveat(parentCaveatName)
	if !ok {
		return nil, spiceerrors.MustBugf("caveat %q not found in schema", parentCaveatName)
	}

	paramType, ok := caveatDef.ParameterTypes[paramName]
	if !ok {
		return nil, spiceerrors.MustBugf("parameter %q not found in caveat %q", paramName, parentCaveatName)
	}

	decoded, err := caveattypes.DecodeParameterType(paramType)
	if err != nil {
		return nil, spiceerrors.MustBugf("invalid parameter type on caveat: %v", err)
	}

	return &v1.ExpCaveatParameter{
		Name:             paramName,
		Type:             decoded.String(),
		ParentCaveatName: parentCaveatName,
	}, nil
}
