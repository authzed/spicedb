package schema

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/graph"
	nspkg "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// GetValidatedDefinition runs validation on the type system for the definition to ensure it is consistent.
func (ts *TypeSystem) GetValidatedDefinition(ctx context.Context, definition string) (*ValidatedDefinition, error) {
	def, validated, err := ts.getDefinition(ctx, definition)
	if err != nil {
		return nil, err
	}
	if validated {
		return &ValidatedDefinition{Definition: def}, nil
	}
	vdef, err := def.Validate(ctx)
	if err != nil {
		return nil, err
	}
	ts.Lock()
	defer ts.Unlock()
	if _, ok := ts.validatedDefinitions[definition]; !ok {
		ts.validatedDefinitions[definition] = vdef
	}
	return vdef, nil
}

func (def *Definition) Validate(ctx context.Context) (*ValidatedDefinition, error) {
	for _, relation := range def.relationMap {
		relation := relation

		// Validate the usersets's.
		usersetRewrite := relation.GetUsersetRewrite()
		rerr, err := graph.WalkRewrite(usersetRewrite, func(childOneof *core.SetOperation_Child) (any, error) {
			switch child := childOneof.ChildType.(type) {
			case *core.SetOperation_Child_ComputedUserset:
				relationName := child.ComputedUserset.GetRelation()
				_, ok := def.relationMap[relationName]
				if !ok {
					return NewTypeWithSourceError(
						NewRelationNotFoundErr(def.nsDef.Name, relationName),
						childOneof,
						relationName,
					), nil
				}

			case *core.SetOperation_Child_TupleToUserset:
				ttu := child.TupleToUserset
				if ttu == nil {
					return nil, nil
				}

				tupleset := ttu.GetTupleset()
				if tupleset == nil {
					return nil, nil
				}

				relationName := tupleset.GetRelation()
				found, ok := def.relationMap[relationName]
				if !ok {
					return NewTypeWithSourceError(
						NewRelationNotFoundErr(def.nsDef.Name, relationName),
						childOneof,
						relationName,
					), nil
				}

				if nspkg.GetRelationKind(found) == iv1.RelationMetadata_PERMISSION {
					return NewTypeWithSourceError(
						NewPermissionUsedOnLeftOfArrowErr(def.nsDef.Name, relation.Name, relationName),
						childOneof, relationName), nil
				}

				// Ensure the tupleset relation doesn't itself import wildcard.
				referencedWildcard, err := def.TypeSystem().referencesWildcardType(ctx, def, relationName)
				if err != nil {
					return err, nil
				}

				if referencedWildcard != nil {
					return NewTypeWithSourceError(
						NewWildcardUsedInArrowErr(
							def.nsDef.Name,
							relation.Name,
							relationName,
							referencedWildcard.WildcardType.GetNamespace(),
							tuple.StringCoreRR(referencedWildcard.ReferencingRelation),
						),
						childOneof, relationName,
					), nil
				}

			case *core.SetOperation_Child_FunctionedTupleToUserset:
				ttu := child.FunctionedTupleToUserset
				if ttu == nil {
					return nil, nil
				}

				tupleset := ttu.GetTupleset()
				if tupleset == nil {
					return nil, nil
				}

				relationName := tupleset.GetRelation()
				found, ok := def.relationMap[relationName]
				if !ok {
					return NewTypeWithSourceError(
						NewRelationNotFoundErr(def.nsDef.Name, relationName),
						childOneof,
						relationName,
					), nil
				}

				if nspkg.GetRelationKind(found) == iv1.RelationMetadata_PERMISSION {
					return NewTypeWithSourceError(
						NewPermissionUsedOnLeftOfArrowErr(def.nsDef.Name, relation.Name, relationName),
						childOneof, relationName), nil
				}

				// Ensure the tupleset relation doesn't itself import wildcard.
				referencedWildcard, err := def.TypeSystem().referencesWildcardType(ctx, def, relationName)
				if err != nil {
					return err, nil
				}

				if referencedWildcard != nil {
					return NewTypeWithSourceError(
						NewWildcardUsedInArrowErr(
							def.nsDef.Name,
							relation.Name,
							relationName,
							referencedWildcard.WildcardType.GetNamespace(),
							tuple.StringCoreRR(referencedWildcard.ReferencingRelation),
						),
						childOneof, relationName,
					), nil
				}
			}
			return nil, nil
		})
		if rerr != nil {
			return nil, asTypeError(rerr.(error))
		}
		if err != nil {
			return nil, err
		}

		// Validate type information.
		typeInfo := relation.TypeInformation
		if typeInfo == nil {
			continue
		}

		allowedDirectRelations := typeInfo.GetAllowedDirectRelations()

		// Check for a _this or the lack of a userset_rewrite. If either is found,
		// then the allowed list must have at least one type.
		hasThis, err := graph.HasThis(usersetRewrite)
		if err != nil {
			return nil, err
		}

		if usersetRewrite == nil || hasThis {
			if len(allowedDirectRelations) == 0 {
				return nil, NewTypeWithSourceError(
					NewMissingAllowedRelationsErr(def.nsDef.Name, relation.Name),
					relation, relation.Name,
				)
			}
		} else {
			if len(allowedDirectRelations) != 0 {
				// NOTE: This is a legacy error and should never really occur with schema.
				return nil, NewTypeWithSourceError(
					fmt.Errorf("direct relations are not allowed under relation `%s`", relation.Name),
					relation, relation.Name)
			}
		}

		// Allowed relations verification:
		// 1) that all allowed relations are not this very relation
		// 2) that they exist within the referenced namespace
		// 3) that they are not duplicated in any way
		// 4) that if they have a caveat reference, the caveat is valid
		encountered := mapz.NewSet[string]()

		for _, allowedRelation := range allowedDirectRelations {
			source := SourceForAllowedRelation(allowedRelation)
			if !encountered.Add(source) {
				return nil, NewTypeWithSourceError(
					NewDuplicateAllowedRelationErr(def.nsDef.Name, relation.Name, source),
					allowedRelation,
					source,
				)
			}

			// Check the namespace.
			if allowedRelation.GetNamespace() == def.nsDef.Name {
				if allowedRelation.GetPublicWildcard() == nil && allowedRelation.GetRelation() != tuple.Ellipsis {
					_, ok := def.relationMap[allowedRelation.GetRelation()]
					if !ok {
						return nil, NewTypeWithSourceError(
							NewRelationNotFoundErr(allowedRelation.GetNamespace(), allowedRelation.GetRelation()),
							allowedRelation,
							allowedRelation.GetRelation(),
						)
					}
				}
			} else {
				subjectTS, err := def.TypeSystem().GetDefinition(ctx, allowedRelation.GetNamespace())
				if err != nil {
					return nil, NewTypeWithSourceError(
						fmt.Errorf("could not lookup definition `%s` for relation `%s`: %w", allowedRelation.GetNamespace(), relation.Name, err),
						allowedRelation,
						allowedRelation.GetNamespace(),
					)
				}

				// Check for relations.
				if allowedRelation.GetPublicWildcard() == nil && allowedRelation.GetRelation() != tuple.Ellipsis {
					// Ensure the relation exists.
					ok := subjectTS.HasRelation(allowedRelation.GetRelation())
					if !ok {
						return nil, NewTypeWithSourceError(
							NewRelationNotFoundErr(allowedRelation.GetNamespace(), allowedRelation.GetRelation()),
							allowedRelation,
							allowedRelation.GetRelation(),
						)
					}

					// Ensure the relation doesn't itself import wildcard.
					referencedWildcard, err := def.TypeSystem().referencesWildcardType(ctx, subjectTS, allowedRelation.GetRelation())
					if err != nil {
						return nil, err
					}

					if referencedWildcard != nil {
						return nil, NewTypeWithSourceError(
							NewTransitiveWildcardErr(
								def.nsDef.Name,
								relation.GetName(),
								allowedRelation.Namespace,
								allowedRelation.GetRelation(),
								referencedWildcard.WildcardType.GetNamespace(),
								tuple.StringCoreRR(referencedWildcard.ReferencingRelation),
							),
							allowedRelation,
							tuple.JoinRelRef(allowedRelation.GetNamespace(), allowedRelation.GetRelation()),
						)
					}
				}
			}

			// Check the caveat, if any.
			if allowedRelation.GetRequiredCaveat() != nil {
				_, err := def.TypeSystem().resolver.LookupCaveat(ctx, allowedRelation.GetRequiredCaveat().CaveatName)
				if err != nil {
					return nil, NewTypeWithSourceError(
						fmt.Errorf("could not lookup caveat `%s` for relation `%s`: %w", allowedRelation.GetRequiredCaveat().CaveatName, relation.Name, err),
						allowedRelation,
						source,
					)
				}
			}
		}
	}

	return &ValidatedDefinition{def}, nil
}

// referencesWildcardType returns true if the relation references a wildcard type, either directly or via
// another relation.
func (ts *TypeSystem) referencesWildcardType(ctx context.Context, def *Definition, relationName string) (*WildcardTypeReference, error) {
	return ts.referencesWildcardTypeWithEncountered(ctx, def, relationName, map[string]bool{})
}

func (ts *TypeSystem) referencesWildcardTypeWithEncountered(ctx context.Context, def *Definition, relationName string, encountered map[string]bool) (*WildcardTypeReference, error) {
	if ts.wildcardCheckCache == nil {
		ts.wildcardCheckCache = make(map[string]*WildcardTypeReference, 1)
	}

	cached, isCached := ts.wildcardCheckCache[relationName]
	if isCached {
		return cached, nil
	}

	computed, err := ts.computeReferencesWildcardType(ctx, def, relationName, encountered)
	if err != nil {
		return nil, err
	}

	ts.wildcardCheckCache[relationName] = computed
	return computed, nil
}

func (ts *TypeSystem) computeReferencesWildcardType(ctx context.Context, def *Definition, relationName string, encountered map[string]bool) (*WildcardTypeReference, error) {
	relString := tuple.JoinRelRef(def.nsDef.Name, relationName)
	if _, ok := encountered[relString]; ok {
		return nil, nil
	}
	encountered[relString] = true

	allowedRels, err := def.AllowedDirectRelationsAndWildcards(relationName)
	if err != nil {
		return nil, asTypeError(err)
	}

	for _, allowedRelation := range allowedRels {
		if allowedRelation.GetPublicWildcard() != nil {
			return &WildcardTypeReference{
				ReferencingRelation: &core.RelationReference{
					Namespace: def.nsDef.Name,
					Relation:  relationName,
				},
				WildcardType: allowedRelation,
			}, nil
		}

		if allowedRelation.GetRelation() != tuple.Ellipsis {
			if allowedRelation.GetNamespace() == def.nsDef.Name {
				found, err := ts.referencesWildcardTypeWithEncountered(ctx, def, allowedRelation.GetRelation(), encountered)
				if err != nil {
					return nil, asTypeError(err)
				}

				if found != nil {
					return found, nil
				}
				continue
			}

			subjectTS, err := ts.GetDefinition(ctx, allowedRelation.GetNamespace())
			if err != nil {
				return nil, asTypeError(err)
			}

			found, err := ts.referencesWildcardTypeWithEncountered(ctx, subjectTS, allowedRelation.GetRelation(), encountered)
			if err != nil {
				return nil, asTypeError(err)
			}

			if found != nil {
				return found, nil
			}
		}
	}

	return nil, nil
}

// ValidatedDefinition is a typesafe reference to a definition that has been validated.
type ValidatedDefinition struct {
	*Definition
}
