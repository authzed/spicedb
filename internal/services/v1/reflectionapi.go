package v1

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
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

type schemaFilters struct {
	filters []*v1.ReflectionSchemaFilter
}

func newSchemaFilters(filters []*v1.ReflectionSchemaFilter) (*schemaFilters, error) {
	for _, filter := range filters {
		if filter.GetOptionalDefinitionNameFilter() != "" {
			if filter.GetOptionalCaveatNameFilter() != "" {
				return nil, NewInvalidFilterErr("cannot filter by both definition and caveat name", filter.String())
			}
		}

		if filter.GetOptionalRelationNameFilter() != "" {
			if filter.GetOptionalDefinitionNameFilter() == "" {
				return nil, NewInvalidFilterErr("relation name match requires definition name match", filter.String())
			}

			if filter.GetOptionalPermissionNameFilter() != "" {
				return nil, NewInvalidFilterErr("cannot filter by both relation and permission name", filter.String())
			}
		}

		if filter.GetOptionalPermissionNameFilter() != "" {
			if filter.GetOptionalDefinitionNameFilter() == "" {
				return nil, NewInvalidFilterErr("permission name match requires definition name match", filter.String())
			}
		}
	}

	return &schemaFilters{filters: filters}, nil
}

func (sf *schemaFilters) HasNamespaces() bool {
	if len(sf.filters) == 0 {
		return true
	}

	for _, filter := range sf.filters {
		if filter.GetOptionalDefinitionNameFilter() != "" {
			return true
		}
	}

	return false
}

func (sf *schemaFilters) HasCaveats() bool {
	if len(sf.filters) == 0 {
		return true
	}

	for _, filter := range sf.filters {
		if filter.GetOptionalCaveatNameFilter() != "" {
			return true
		}
	}

	return false
}

func (sf *schemaFilters) HasNamespace(namespaceName string) bool {
	if len(sf.filters) == 0 {
		return true
	}

	hasDefinitionFilter := false
	for _, filter := range sf.filters {
		if filter.GetOptionalDefinitionNameFilter() == "" {
			continue
		}

		hasDefinitionFilter = true
		isMatch := strings.HasPrefix(namespaceName, filter.GetOptionalDefinitionNameFilter())
		if isMatch {
			return true
		}
	}

	return !hasDefinitionFilter
}

func (sf *schemaFilters) HasCaveat(caveatName string) bool {
	if len(sf.filters) == 0 {
		return true
	}

	hasCaveatFilter := false
	for _, filter := range sf.filters {
		if filter.GetOptionalCaveatNameFilter() == "" {
			continue
		}

		hasCaveatFilter = true
		isMatch := strings.HasPrefix(caveatName, filter.GetOptionalCaveatNameFilter())
		if isMatch {
			return true
		}
	}

	return !hasCaveatFilter
}

func (sf *schemaFilters) HasRelation(namespaceName, relationName string) bool {
	if len(sf.filters) == 0 {
		return true
	}

	hasRelationFilter := false
	for _, filter := range sf.filters {
		if filter.GetOptionalRelationNameFilter() == "" {
			continue
		}

		hasRelationFilter = true
		isMatch := strings.HasPrefix(relationName, filter.GetOptionalRelationNameFilter())
		if !isMatch {
			continue
		}

		isMatch = strings.HasPrefix(namespaceName, filter.GetOptionalDefinitionNameFilter())
		if isMatch {
			return true
		}
	}

	return !hasRelationFilter
}

func (sf *schemaFilters) HasPermission(namespaceName, permissionName string) bool {
	if len(sf.filters) == 0 {
		return true
	}

	hasPermissionFilter := false
	for _, filter := range sf.filters {
		if filter.GetOptionalPermissionNameFilter() == "" {
			continue
		}

		hasPermissionFilter = true
		isMatch := strings.HasPrefix(permissionName, filter.GetOptionalPermissionNameFilter())
		if !isMatch {
			continue
		}

		isMatch = strings.HasPrefix(namespaceName, filter.GetOptionalDefinitionNameFilter())
		if isMatch {
			return true
		}
	}

	return !hasPermissionFilter
}

// convertDiff converts a schema diff into an API response.
func convertDiff(
	ctx context.Context,
	diff *diff.SchemaDiff,
	existingSchema *diff.DiffableSchema,
	comparisonSchema *diff.DiffableSchema,
	atRevision datastore.Revision,
	caveatTypeSet *caveattypes.TypeSet,
) (*v1.DiffSchemaResponse, error) {
	size := len(diff.AddedNamespaces) + len(diff.RemovedNamespaces) + len(diff.AddedCaveats) + len(diff.RemovedCaveats) + len(diff.ChangedNamespaces) + len(diff.ChangedCaveats)
	diffs := make([]*v1.ReflectionSchemaDiff, 0, size)

	// Add/remove namespaces.
	for _, ns := range diff.AddedNamespaces {
		nsDef, err := namespaceAPIReprForName(ns, comparisonSchema)
		if err != nil {
			return nil, err
		}

		diffs = append(diffs, &v1.ReflectionSchemaDiff{
			Diff: &v1.ReflectionSchemaDiff_DefinitionAdded{
				DefinitionAdded: nsDef,
			},
		})
	}

	for _, ns := range diff.RemovedNamespaces {
		nsDef, err := namespaceAPIReprForName(ns, existingSchema)
		if err != nil {
			return nil, err
		}

		diffs = append(diffs, &v1.ReflectionSchemaDiff{
			Diff: &v1.ReflectionSchemaDiff_DefinitionRemoved{
				DefinitionRemoved: nsDef,
			},
		})
	}

	// Add/remove caveats.
	for _, caveat := range diff.AddedCaveats {
		caveatDef, err := caveatAPIReprForName(caveat, comparisonSchema, caveatTypeSet)
		if err != nil {
			return nil, err
		}

		diffs = append(diffs, &v1.ReflectionSchemaDiff{
			Diff: &v1.ReflectionSchemaDiff_CaveatAdded{
				CaveatAdded: caveatDef,
			},
		})
	}

	for _, caveat := range diff.RemovedCaveats {
		caveatDef, err := caveatAPIReprForName(caveat, existingSchema, caveatTypeSet)
		if err != nil {
			return nil, err
		}

		diffs = append(diffs, &v1.ReflectionSchemaDiff{
			Diff: &v1.ReflectionSchemaDiff_CaveatRemoved{
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

				perm, err := permissionAPIRepr(permission, nsName, nil)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_PermissionAdded{
						PermissionAdded: perm,
					},
				})

			case nsdiff.AddedRelation:
				relation, ok := comparisonSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, spiceerrors.MustBugf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := relationAPIRepr(relation, nsName, nil)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_RelationAdded{
						RelationAdded: rel,
					},
				})

			case nsdiff.ChangedPermissionComment:
				permission, ok := comparisonSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, spiceerrors.MustBugf("permission %q not found in namespace %q", delta.RelationName, nsName)
				}

				perm, err := permissionAPIRepr(permission, nsName, nil)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_PermissionDocCommentChanged{
						PermissionDocCommentChanged: perm,
					},
				})

			case nsdiff.ChangedPermissionImpl:
				permission, ok := comparisonSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, spiceerrors.MustBugf("permission %q not found in namespace %q", delta.RelationName, nsName)
				}

				perm, err := permissionAPIRepr(permission, nsName, nil)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_PermissionExprChanged{
						PermissionExprChanged: perm,
					},
				})

			case nsdiff.ChangedRelationComment:
				relation, ok := comparisonSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, spiceerrors.MustBugf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := relationAPIRepr(relation, nsName, nil)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_RelationDocCommentChanged{
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

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_DefinitionDocCommentChanged{
						DefinitionDocCommentChanged: def,
					},
				})

			case nsdiff.RelationAllowedTypeRemoved:
				relation, ok := comparisonSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, spiceerrors.MustBugf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := relationAPIRepr(relation, nsName, nil)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_RelationSubjectTypeRemoved{
						RelationSubjectTypeRemoved: &v1.ReflectionRelationSubjectTypeChange{
							Relation:           rel,
							ChangedSubjectType: typeAPIRepr(delta.AllowedType),
						},
					},
				})

			case nsdiff.RelationAllowedTypeAdded:
				relation, ok := comparisonSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, spiceerrors.MustBugf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := relationAPIRepr(relation, nsName, nil)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_RelationSubjectTypeAdded{
						RelationSubjectTypeAdded: &v1.ReflectionRelationSubjectTypeChange{
							Relation:           rel,
							ChangedSubjectType: typeAPIRepr(delta.AllowedType),
						},
					},
				})

			case nsdiff.RemovedPermission:
				permission, ok := existingSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, spiceerrors.MustBugf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				perm, err := permissionAPIRepr(permission, nsName, nil)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_PermissionRemoved{
						PermissionRemoved: perm,
					},
				})

			case nsdiff.RemovedRelation:
				relation, ok := existingSchema.GetRelation(nsName, delta.RelationName)
				if !ok {
					return nil, spiceerrors.MustBugf("relation %q not found in namespace %q", delta.RelationName, nsName)
				}

				rel, err := relationAPIRepr(relation, nsName, nil)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_RelationRemoved{
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
				caveat, err := caveatAPIReprForName(caveatName, comparisonSchema, caveatTypeSet)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_CaveatDocCommentChanged{
						CaveatDocCommentChanged: caveat,
					},
				})

			case caveatdiff.AddedParameter:
				paramDef, err := caveatAPIParamRepr(delta.ParameterName, caveatName, comparisonSchema, caveatTypeSet)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_CaveatParameterAdded{
						CaveatParameterAdded: paramDef,
					},
				})

			case caveatdiff.RemovedParameter:
				paramDef, err := caveatAPIParamRepr(delta.ParameterName, caveatName, existingSchema, caveatTypeSet)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_CaveatParameterRemoved{
						CaveatParameterRemoved: paramDef,
					},
				})

			case caveatdiff.ParameterTypeChanged:
				previousParamDef, err := caveatAPIParamRepr(delta.ParameterName, caveatName, existingSchema, caveatTypeSet)
				if err != nil {
					return nil, err
				}

				paramDef, err := caveatAPIParamRepr(delta.ParameterName, caveatName, comparisonSchema, caveatTypeSet)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_CaveatParameterTypeChanged{
						CaveatParameterTypeChanged: &v1.ReflectionCaveatParameterTypeChange{
							Parameter:    paramDef,
							PreviousType: previousParamDef.GetType(),
						},
					},
				})

			case caveatdiff.CaveatExpressionChanged:
				caveat, err := caveatAPIReprForName(caveatName, comparisonSchema, caveatTypeSet)
				if err != nil {
					return nil, err
				}

				diffs = append(diffs, &v1.ReflectionSchemaDiff{
					Diff: &v1.ReflectionSchemaDiff_CaveatExprChanged{
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

	ds := datastoremw.MustFromContext(ctx)
	zedToken, err := zedtoken.NewFromRevision(ctx, atRevision, ds)
	if err != nil {
		return nil, fmt.Errorf("failed to create zed token: %w", err)
	}

	return &v1.DiffSchemaResponse{
		Diffs:  diffs,
		ReadAt: zedToken,
	}, nil
}

// namespaceAPIReprForName builds an API representation of a namespace.
func namespaceAPIReprForName(namespaceName string, schema *diff.DiffableSchema) (*v1.ReflectionDefinition, error) {
	nsDef, ok := schema.GetNamespace(namespaceName)
	if !ok {
		return nil, spiceerrors.MustBugf("namespace %q not found in schema", namespaceName)
	}

	return namespaceAPIRepr(nsDef, nil)
}

func namespaceAPIRepr(nsDef *core.NamespaceDefinition, schemaFilters *schemaFilters) (*v1.ReflectionDefinition, error) {
	if schemaFilters != nil && !schemaFilters.HasNamespace(nsDef.GetName()) {
		return nil, nil
	}

	relations := make([]*v1.ReflectionRelation, 0, len(nsDef.GetRelation()))
	permissions := make([]*v1.ReflectionPermission, 0, len(nsDef.GetRelation()))

	for _, rel := range nsDef.GetRelation() {
		if namespace.GetRelationKind(rel) == iv1.RelationMetadata_PERMISSION {
			permission, err := permissionAPIRepr(rel, nsDef.GetName(), schemaFilters)
			if err != nil {
				return nil, err
			}

			if permission != nil {
				permissions = append(permissions, permission)
			}
			continue
		}

		relation, err := relationAPIRepr(rel, nsDef.GetName(), schemaFilters)
		if err != nil {
			return nil, err
		}

		if relation != nil {
			relations = append(relations, relation)
		}
	}

	comments := namespace.GetComments(nsDef.GetMetadata())
	return &v1.ReflectionDefinition{
		Name:        nsDef.GetName(),
		Comment:     strings.Join(comments, "\n"),
		Relations:   relations,
		Permissions: permissions,
	}, nil
}

// permissionAPIRepr builds an API representation of a permission.
func permissionAPIRepr(relation *core.Relation, parentDefName string, schemaFilters *schemaFilters) (*v1.ReflectionPermission, error) {
	if schemaFilters != nil && !schemaFilters.HasPermission(parentDefName, relation.GetName()) {
		return nil, nil
	}

	comments := namespace.GetComments(relation.GetMetadata())
	return &v1.ReflectionPermission{
		Name:                 relation.GetName(),
		Comment:              strings.Join(comments, "\n"),
		ParentDefinitionName: parentDefName,
	}, nil
}

// relationAPIRepresentation builds an API representation of a relation.
func relationAPIRepr(relation *core.Relation, parentDefName string, schemaFilters *schemaFilters) (*v1.ReflectionRelation, error) {
	if schemaFilters != nil && !schemaFilters.HasRelation(parentDefName, relation.GetName()) {
		return nil, nil
	}

	comments := namespace.GetComments(relation.GetMetadata())

	var subjectTypes []*v1.ReflectionTypeReference
	if relation.GetTypeInformation() != nil {
		subjectTypes = make([]*v1.ReflectionTypeReference, 0, len(relation.GetTypeInformation().GetAllowedDirectRelations()))
		for _, subjectType := range relation.GetTypeInformation().GetAllowedDirectRelations() {
			typeref := typeAPIRepr(subjectType)
			subjectTypes = append(subjectTypes, typeref)
		}
	}

	return &v1.ReflectionRelation{
		Name:                 relation.GetName(),
		Comment:              strings.Join(comments, "\n"),
		ParentDefinitionName: parentDefName,
		SubjectTypes:         subjectTypes,
	}, nil
}

// typeAPIRepr builds an API representation of a type.
func typeAPIRepr(subjectType *core.AllowedRelation) *v1.ReflectionTypeReference {
	typeref := &v1.ReflectionTypeReference{
		SubjectDefinitionName: subjectType.GetNamespace(),
		Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
	}

	if subjectType.GetRelation() != tuple.Ellipsis && subjectType.GetRelation() != "" {
		typeref.Typeref = &v1.ReflectionTypeReference_OptionalRelationName{
			OptionalRelationName: subjectType.GetRelation(),
		}
	} else if subjectType.GetPublicWildcard() != nil {
		typeref.Typeref = &v1.ReflectionTypeReference_IsPublicWildcard{
			IsPublicWildcard: true,
		}
	}

	if subjectType.GetRequiredCaveat() != nil {
		typeref.OptionalCaveatName = subjectType.GetRequiredCaveat().GetCaveatName()
	}

	return typeref
}

// caveatAPIReprForName builds an API representation of a caveat.
func caveatAPIReprForName(caveatName string, schema *diff.DiffableSchema, caveatTypeSet *caveattypes.TypeSet) (*v1.ReflectionCaveat, error) {
	caveatDef, ok := schema.GetCaveat(caveatName)
	if !ok {
		return nil, spiceerrors.MustBugf("caveat %q not found in schema", caveatName)
	}

	return caveatAPIRepr(caveatDef, nil, caveatTypeSet)
}

// caveatAPIRepr builds an API representation of a caveat.
func caveatAPIRepr(caveatDef *core.CaveatDefinition, schemaFilters *schemaFilters, caveatTypeSet *caveattypes.TypeSet) (*v1.ReflectionCaveat, error) {
	if schemaFilters != nil && !schemaFilters.HasCaveat(caveatDef.GetName()) {
		return nil, nil
	}

	parameters := make([]*v1.ReflectionCaveatParameter, 0, len(caveatDef.GetParameterTypes()))
	paramNames := slices.Collect(maps.Keys(caveatDef.GetParameterTypes()))
	sort.Strings(paramNames)

	for _, paramName := range paramNames {
		paramType, ok := caveatDef.GetParameterTypes()[paramName]
		if !ok {
			return nil, spiceerrors.MustBugf("parameter %q not found in caveat %q", paramName, caveatDef.GetName())
		}

		decoded, err := caveattypes.DecodeParameterType(caveatTypeSet, paramType)
		if err != nil {
			return nil, spiceerrors.MustBugf("invalid parameter type on caveat: %v", err)
		}

		parameters = append(parameters, &v1.ReflectionCaveatParameter{
			Name:             paramName,
			Type:             decoded.String(),
			ParentCaveatName: caveatDef.GetName(),
		})
	}

	parameterTypes, err := caveattypes.DecodeParameterTypes(caveatTypeSet, caveatDef.GetParameterTypes())
	if err != nil {
		return nil, spiceerrors.MustBugf("invalid caveat parameters: %v", err)
	}

	deserializedReflectionression, err := caveats.DeserializeCaveatWithTypeSet(caveatTypeSet, caveatDef.GetSerializedExpression(), parameterTypes)
	if err != nil {
		return nil, spiceerrors.MustBugf("invalid caveat expression bytes: %v", err)
	}

	exprString, err := deserializedReflectionression.ExprString()
	if err != nil {
		return nil, spiceerrors.MustBugf("invalid caveat expression: %v", err)
	}

	comments := namespace.GetComments(caveatDef.GetMetadata())
	return &v1.ReflectionCaveat{
		Name:       caveatDef.GetName(),
		Comment:    strings.Join(comments, "\n"),
		Parameters: parameters,
		Expression: exprString,
	}, nil
}

// caveatAPIParamRepresentation builds an API representation of a caveat parameter.
func caveatAPIParamRepr(paramName, parentCaveatName string, schema *diff.DiffableSchema, caveatTypeSet *caveattypes.TypeSet) (*v1.ReflectionCaveatParameter, error) {
	caveatDef, ok := schema.GetCaveat(parentCaveatName)
	if !ok {
		return nil, spiceerrors.MustBugf("caveat %q not found in schema", parentCaveatName)
	}

	paramType, ok := caveatDef.GetParameterTypes()[paramName]
	if !ok {
		return nil, spiceerrors.MustBugf("parameter %q not found in caveat %q", paramName, parentCaveatName)
	}

	decoded, err := caveattypes.DecodeParameterType(caveatTypeSet, paramType)
	if err != nil {
		return nil, spiceerrors.MustBugf("invalid parameter type on caveat: %v", err)
	}

	return &v1.ReflectionCaveatParameter{
		Name:             paramName,
		Type:             decoded.String(),
		ParentCaveatName: parentCaveatName,
	}, nil
}
