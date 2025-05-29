package schema

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// ArrowInformation holds information about an arrow (TupleToUserset) in the schema.
type ArrowInformation struct {
	Arrow              *core.TupleToUserset
	Path               string
	ParentRelationName string
}

// ArrowSet represents a set of all the arrows (TupleToUserset's) found in the schema.
type ArrowSet struct {
	res                                                 FullSchemaResolver
	ts                                                  *TypeSystem
	arrowsByFullTuplesetRelation                        *mapz.MultiMap[string, ArrowInformation]
	arrowsByComputedUsersetNamespaceAndRelation         *mapz.MultiMap[string, ArrowInformation]
	reachableComputedUsersetRelationsByTuplesetRelation *mapz.MultiMap[string, string]
}

// buildArrowSet builds a new set of all arrows found in the given schema.
func buildArrowSet(ctx context.Context, res FullSchemaResolver) (*ArrowSet, error) {
	arrowSet := &ArrowSet{
		res:                          res,
		ts:                           NewTypeSystem(res),
		arrowsByFullTuplesetRelation: mapz.NewMultiMap[string, ArrowInformation](),
		arrowsByComputedUsersetNamespaceAndRelation:         mapz.NewMultiMap[string, ArrowInformation](),
		reachableComputedUsersetRelationsByTuplesetRelation: mapz.NewMultiMap[string, string](),
	}
	if err := arrowSet.compute(ctx); err != nil {
		return nil, err
	}
	return arrowSet, nil
}

// AllReachableRelations returns all relations reachable through arrows, including tupleset relations
// and computed userset relations.
func (as *ArrowSet) AllReachableRelations() *mapz.Set[string] {
	c := mapz.NewSet(as.reachableComputedUsersetRelationsByTuplesetRelation.Values()...)
	c.Extend(as.arrowsByFullTuplesetRelation.Keys())
	return c
}

// HasPossibleArrowWithComputedUserset returns true if there is a *possible* arrow with the given relation name
// as the arrow's computed userset/for a subject type that has the given namespace.
func (as *ArrowSet) HasPossibleArrowWithComputedUserset(namespaceName string, relationName string) bool {
	return as.arrowsByComputedUsersetNamespaceAndRelation.Has(namespaceName + "#" + relationName)
}

// LookupTuplesetArrows finds all arrows with the given namespace and relation name as the arrows' tupleset.
func (as *ArrowSet) LookupTuplesetArrows(namespaceName string, relationName string) []ArrowInformation {
	key := namespaceName + "#" + relationName
	found, _ := as.arrowsByFullTuplesetRelation.Get(key)
	return found
}

func (as *ArrowSet) compute(ctx context.Context) error {
	for _, name := range as.res.AllDefinitionNames() {
		def, err := as.ts.GetValidatedDefinition(ctx, name)
		if err != nil {
			return err
		}
		for _, relation := range def.nsDef.Relation {
			if err := as.collectArrowInformationForRelation(ctx, def, relation.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (as *ArrowSet) add(ttu *core.TupleToUserset, path string, namespaceName string, relationName string) {
	tsKey := namespaceName + "#" + ttu.Tupleset.Relation
	as.arrowsByFullTuplesetRelation.Add(tsKey, ArrowInformation{Path: path, Arrow: ttu, ParentRelationName: relationName})
}

func (as *ArrowSet) collectArrowInformationForRelation(ctx context.Context, def *ValidatedDefinition, relationName string) error {
	if !def.IsPermission(relationName) {
		return nil
	}

	relation, ok := def.GetRelation(relationName)
	if !ok {
		return asTypeError(NewRelationNotFoundErr(def.Namespace().GetName(), relationName))
	}
	return as.collectArrowInformationForRewrite(ctx, relation.UsersetRewrite, def, relation, relationName)
}

func (as *ArrowSet) collectArrowInformationForRewrite(ctx context.Context, rewrite *core.UsersetRewrite, def *ValidatedDefinition, relation *core.Relation, path string) error {
	switch rw := rewrite.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		return as.collectArrowInformationForSetOperation(ctx, rw.Union, def, relation, path)
	case *core.UsersetRewrite_Intersection:
		return as.collectArrowInformationForSetOperation(ctx, rw.Intersection, def, relation, path)
	case *core.UsersetRewrite_Exclusion:
		return as.collectArrowInformationForSetOperation(ctx, rw.Exclusion, def, relation, path)
	default:
		return errors.New("userset rewrite operation not implemented in addArrowRelationsForRewrite")
	}
}

func (as *ArrowSet) collectArrowInformationForSetOperation(ctx context.Context, so *core.SetOperation, def *ValidatedDefinition, relation *core.Relation, path string) error {
	for index, childOneof := range so.Child {
		updatedPath := path + "." + strconv.Itoa(index)
		switch child := childOneof.ChildType.(type) {
		case *core.SetOperation_Child_ComputedUserset:
			// Nothing to do

		case *core.SetOperation_Child_UsersetRewrite:
			err := as.collectArrowInformationForRewrite(ctx, child.UsersetRewrite, def, relation, updatedPath)
			if err != nil {
				return err
			}

		case *core.SetOperation_Child_TupleToUserset:
			as.add(child.TupleToUserset, updatedPath, def.Namespace().Name, relation.Name)

			allowedSubjectTypes, err := def.AllowedSubjectRelations(child.TupleToUserset.Tupleset.Relation)
			if err != nil {
				return err
			}

			for _, ast := range allowedSubjectTypes {
				def, err := as.ts.GetValidatedDefinition(ctx, ast.Namespace)
				if err != nil {
					return err
				}

				// NOTE: this is explicitly added to the arrowsByComputedUsersetNamespaceAndRelation without
				// checking if the relation/permission exists, because its needed for schema diff tracking.
				as.arrowsByComputedUsersetNamespaceAndRelation.Add(ast.Namespace+"#"+child.TupleToUserset.ComputedUserset.Relation, ArrowInformation{Path: path, Arrow: child.TupleToUserset, ParentRelationName: relation.Name})
				if def.HasRelation(child.TupleToUserset.ComputedUserset.Relation) {
					as.reachableComputedUsersetRelationsByTuplesetRelation.Add(ast.Namespace+"#"+child.TupleToUserset.Tupleset.Relation, ast.Namespace+"#"+child.TupleToUserset.ComputedUserset.Relation)
				}
			}

		case *core.SetOperation_Child_XThis:
			// Nothing to do

		case *core.SetOperation_Child_XNil:
			// Nothing to do

		default:
			return fmt.Errorf("unknown set operation child `%T` in addArrowRelationsInSetOperation", child)
		}
	}

	return nil
}
