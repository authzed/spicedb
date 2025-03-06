package schema

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// Graph is a struct holding reachability information.
type Graph struct {
	arrowSet         *ArrowSet
	ts               *TypeSystem
	referenceInfoMap map[nsAndRel][]RelationReferenceInfo
}

// BuildGraph builds the graph of all reachable information in the schema.
func BuildGraph(ctx context.Context, r *CompiledSchemaResolver) (*Graph, error) {
	arrowSet, err := buildArrowSet(ctx, r)
	if err != nil {
		return nil, err
	}

	ts := NewTypeSystem(r)
	referenceInfoMap, err := preComputeRelationReferenceInfo(ctx, arrowSet, r, ts)
	if err != nil {
		return nil, err
	}

	return &Graph{
		ts:               ts,
		arrowSet:         arrowSet,
		referenceInfoMap: referenceInfoMap,
	}, nil
}

// Arrows returns the set of arrows found in the reachability graph.
func (g *Graph) Arrows() *ArrowSet {
	return g.arrowSet
}

// RelationsReferencing returns all relations/permissions in the schema that reference the specified
// relation in some manner.
func (g *Graph) RelationsReferencing(namespaceName string, relationName string) []RelationReferenceInfo {
	return g.referenceInfoMap[nsAndRel{
		Namespace: namespaceName,
		Relation:  relationName,
	}]
}

// ReferenceType is an enum describing what kind of relation reference we hold in a RelationReferenceInfo.
type ReferenceType int

const (
	RelationInExpression ReferenceType = iota
	RelationIsSubjectType
	RelationIsTuplesetForArrow
	RelationIsComputedUsersetForArrow
)

// RelationReferenceInfo holds the relation and metadata for a relation found in the full reachability graph.
type RelationReferenceInfo struct {
	Relation *core.RelationReference
	Type     ReferenceType
	Arrow    *ArrowInformation
}

func relationsReferencing(ctx context.Context, arrowSet *ArrowSet, res FullSchemaResolver, ts *TypeSystem, namespaceName string, relationName string) ([]RelationReferenceInfo, error) {
	foundReferences := make([]RelationReferenceInfo, 0)

	for _, name := range res.AllDefinitionNames() {
		def, err := ts.GetValidatedDefinition(ctx, name)
		if err != nil {
			return nil, err
		}
		for _, relation := range def.Namespace().Relation {
			// Check for the use of the relation directly as part of any permissions in the same namespace.
			if def.IsPermission(relation.Name) && name == namespaceName {
				hasReference, err := expressionReferencesRelation(ctx, relation.GetUsersetRewrite(), relationName)
				if err != nil {
					return nil, err
				}

				if hasReference {
					foundReferences = append(foundReferences, RelationReferenceInfo{
						Relation: &core.RelationReference{
							Namespace: name,
							Relation:  relation.Name,
						},
						Type: RelationInExpression,
					})
				}
				continue
			}

			// Check for the use of the relation as a subject type on any relation in the entire schema.
			isAllowed, err := def.IsAllowedDirectRelation(relation.Name, namespaceName, relationName)
			if err != nil {
				return nil, err
			}

			if isAllowed == DirectRelationValid {
				foundReferences = append(foundReferences, RelationReferenceInfo{
					Relation: &core.RelationReference{
						Namespace: name,
						Relation:  relation.Name,
					},
					Type: RelationIsSubjectType,
				})
			}
		}
	}

	// Add any arrow references.
	key := namespaceName + "#" + relationName
	foundArrows, _ := arrowSet.arrowsByFullTuplesetRelation.Get(key)
	for _, arrow := range foundArrows {
		arrow := arrow
		foundReferences = append(foundReferences, RelationReferenceInfo{
			Relation: &core.RelationReference{
				Namespace: namespaceName,
				Relation:  arrow.ParentRelationName,
			},
			Type:  RelationIsTuplesetForArrow,
			Arrow: &arrow,
		})
	}

	for _, tuplesetRelationKey := range arrowSet.reachableComputedUsersetRelationsByTuplesetRelation.Keys() {
		values, ok := arrowSet.reachableComputedUsersetRelationsByTuplesetRelation.Get(tuplesetRelationKey)
		if !ok {
			continue
		}

		if slices.Contains(values, key) {
			pieces := strings.Split(tuplesetRelationKey, "#")
			foundReferences = append(foundReferences, RelationReferenceInfo{
				Relation: &core.RelationReference{
					Namespace: pieces[0],
					Relation:  pieces[1],
				},
				Type: RelationIsComputedUsersetForArrow,
			})
		}
	}

	return foundReferences, nil
}

type nsAndRel struct {
	Namespace string
	Relation  string
}

func preComputeRelationReferenceInfo(ctx context.Context, arrowSet *ArrowSet, res FullSchemaResolver, ts *TypeSystem) (map[nsAndRel][]RelationReferenceInfo, error) {
	nsAndRelToInfo := make(map[nsAndRel][]RelationReferenceInfo)

	for _, namespaceName := range res.AllDefinitionNames() {
		outerTS, err := ts.GetValidatedDefinition(ctx, namespaceName)
		if err != nil {
			return nil, err
		}
		for _, outerRelation := range outerTS.Namespace().Relation {
			referenceInfos, err := relationsReferencing(ctx, arrowSet, res, ts, namespaceName, outerRelation.Name)
			if err != nil {
				return nil, err
			}

			nsAndRel := nsAndRel{
				Namespace: namespaceName,
				Relation:  outerRelation.Name,
			}
			nsAndRelToInfo[nsAndRel] = referenceInfos
		}
	}

	return nsAndRelToInfo, nil
}

func expressionReferencesRelation(ctx context.Context, rewrite *core.UsersetRewrite, relationName string) (bool, error) {
	// TODO(jschorr): Precompute this and maybe create a visitor pattern to stop repeating this everywhere
	switch rw := rewrite.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		return setOperationReferencesRelation(ctx, rw.Union, relationName)
	case *core.UsersetRewrite_Intersection:
		return setOperationReferencesRelation(ctx, rw.Intersection, relationName)
	case *core.UsersetRewrite_Exclusion:
		return setOperationReferencesRelation(ctx, rw.Exclusion, relationName)
	default:
		return false, errors.New("userset rewrite operation not implemented in expressionReferencesRelation")
	}
}

func setOperationReferencesRelation(ctx context.Context, so *core.SetOperation, relationName string) (bool, error) {
	for _, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *core.SetOperation_Child_ComputedUserset:
			if child.ComputedUserset.Relation == relationName {
				return true, nil
			}

		case *core.SetOperation_Child_UsersetRewrite:
			result, err := expressionReferencesRelation(ctx, child.UsersetRewrite, relationName)
			if result || err != nil {
				return result, err
			}

		case *core.SetOperation_Child_TupleToUserset:
			// Nothing to do, handled above via arrow set

		case *core.SetOperation_Child_XThis:
			// Nothing to do

		case *core.SetOperation_Child_XNil:
			// Nothing to do

		default:
			return false, fmt.Errorf("unknown set operation child `%T` in setOperationReferencesRelation", child)
		}
	}

	return false, nil
}
