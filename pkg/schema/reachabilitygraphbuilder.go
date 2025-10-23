package schema

import (
	"context"
	"errors"
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

type reachabilityOption int

const (
	reachabilityFull reachabilityOption = iota
	reachabilityFirst
)

func computeReachability(ctx context.Context, def *Definition, relationName string, option reachabilityOption) (*core.ReachabilityGraph, error) {
	targetRelation, ok := def.relationMap[relationName]
	if !ok {
		return nil, fmt.Errorf("relation `%s` not found under type `%s` missing when computing reachability", relationName, def.nsDef.GetName())
	}

	if !def.HasTypeInformation(relationName) && targetRelation.GetUsersetRewrite() == nil {
		return nil, fmt.Errorf("relation `%s` missing type information when computing reachability for namespace `%s`", relationName, def.nsDef.GetName())
	}

	graph := &core.ReachabilityGraph{
		EntrypointsBySubjectType:     map[string]*core.ReachabilityEntrypoints{},
		EntrypointsBySubjectRelation: map[string]*core.ReachabilityEntrypoints{},
	}

	usersetRewrite := targetRelation.GetUsersetRewrite()
	if usersetRewrite != nil {
		return graph, computeRewriteReachability(ctx, graph, usersetRewrite, core.ReachabilityEntrypoint_DIRECT_OPERATION_RESULT, targetRelation, def, option)
	}

	// If there is no userRewrite, then we have a relation and its entrypoints will all be
	// relation entrypoints.
	return graph, addSubjectLinks(graph, core.ReachabilityEntrypoint_DIRECT_OPERATION_RESULT, targetRelation, def)
}

func computeRewriteReachability(ctx context.Context, graph *core.ReachabilityGraph, rewrite *core.UsersetRewrite, operationResultState core.ReachabilityEntrypoint_EntrypointResultStatus, targetRelation *core.Relation, def *Definition, option reachabilityOption) error {
	switch rw := rewrite.GetRewriteOperation().(type) {
	case *core.UsersetRewrite_Union:
		return computeRewriteOpReachability(ctx, rw.Union.GetChild(), operationResultState, graph, targetRelation, def, option)

	case *core.UsersetRewrite_Intersection:
		// If optimized mode is set, only return the first child of the intersection.
		if option == reachabilityFirst {
			return computeRewriteOpReachability(ctx, rw.Intersection.GetChild()[0:1], core.ReachabilityEntrypoint_REACHABLE_CONDITIONAL_RESULT, graph, targetRelation, def, option)
		}

		return computeRewriteOpReachability(ctx, rw.Intersection.GetChild(), core.ReachabilityEntrypoint_REACHABLE_CONDITIONAL_RESULT, graph, targetRelation, def, option)

	case *core.UsersetRewrite_Exclusion:
		// If optimized mode is set, only return the first child of the exclusion.
		if option == reachabilityFirst {
			return computeRewriteOpReachability(ctx, rw.Exclusion.GetChild()[0:1], core.ReachabilityEntrypoint_REACHABLE_CONDITIONAL_RESULT, graph, targetRelation, def, option)
		}

		return computeRewriteOpReachability(ctx, rw.Exclusion.GetChild(), core.ReachabilityEntrypoint_REACHABLE_CONDITIONAL_RESULT, graph, targetRelation, def, option)

	default:
		return fmt.Errorf("unknown kind of userset rewrite in reachability computation: %T", rw)
	}
}

func computeRewriteOpReachability(ctx context.Context, children []*core.SetOperation_Child, operationResultState core.ReachabilityEntrypoint_EntrypointResultStatus, graph *core.ReachabilityGraph, targetRelation *core.Relation, def *Definition, option reachabilityOption) error {
	rr := &core.RelationReference{
		Namespace: def.nsDef.GetName(),
		Relation:  targetRelation.GetName(),
	}

	for _, childOneof := range children {
		switch child := childOneof.GetChildType().(type) {
		case *core.SetOperation_Child_XThis:
			return errors.New("use of _this is unsupported; please rewrite your schema")

		case *core.SetOperation_Child_ComputedUserset:
			// A computed userset adds an entrypoint indicating that the relation is rewritten.
			err := addSubjectEntrypoint(graph, def.nsDef.GetName(), child.ComputedUserset.GetRelation(), &core.ReachabilityEntrypoint{
				Kind:                    core.ReachabilityEntrypoint_COMPUTED_USERSET_ENTRYPOINT,
				TargetRelation:          rr,
				ComputedUsersetRelation: child.ComputedUserset.GetRelation(),
				ResultStatus:            operationResultState,
			})
			if err != nil {
				return err
			}

		case *core.SetOperation_Child_UsersetRewrite:
			err := computeRewriteReachability(ctx, graph, child.UsersetRewrite, operationResultState, targetRelation, def, option)
			if err != nil {
				return err
			}

		case *core.SetOperation_Child_TupleToUserset:
			tuplesetRelation := child.TupleToUserset.GetTupleset().GetRelation()
			computedUsersetRelation := child.TupleToUserset.GetComputedUserset().GetRelation()
			if err := computeTTUReachability(ctx, graph, tuplesetRelation, computedUsersetRelation, operationResultState, rr, def); err != nil {
				return err
			}

		case *core.SetOperation_Child_FunctionedTupleToUserset:
			tuplesetRelation := child.FunctionedTupleToUserset.GetTupleset().GetRelation()
			computedUsersetRelation := child.FunctionedTupleToUserset.GetComputedUserset().GetRelation()

			switch child.FunctionedTupleToUserset.GetFunction() {
			case core.FunctionedTupleToUserset_FUNCTION_ANY:
				// Nothing to change.

			case core.FunctionedTupleToUserset_FUNCTION_ALL:
				// Mark as a conditional result.
				operationResultState = core.ReachabilityEntrypoint_REACHABLE_CONDITIONAL_RESULT

			default:
				return spiceerrors.MustBugf("unknown function type `%T` in reachability graph building", child.FunctionedTupleToUserset.GetFunction())
			}

			if err := computeTTUReachability(ctx, graph, tuplesetRelation, computedUsersetRelation, operationResultState, rr, def); err != nil {
				return err
			}

		case *core.SetOperation_Child_XNil:
			// nil has no entrypoints.
			return nil

		default:
			return spiceerrors.MustBugf("unknown set operation child `%T` in reachability graph building", child)
		}
	}

	return nil
}

func computeTTUReachability(
	ctx context.Context,
	graph *core.ReachabilityGraph,
	tuplesetRelation string,
	computedUsersetRelation string,
	operationResultState core.ReachabilityEntrypoint_EntrypointResultStatus,
	rr *core.RelationReference,
	def *Definition,
) error {
	directRelationTypes, err := def.AllowedDirectRelationsAndWildcards(tuplesetRelation)
	if err != nil {
		return err
	}

	for _, allowedRelationType := range directRelationTypes {
		// For each namespace allowed to be found on the right hand side of the
		// tupleset relation, include the *computed userset* relation as an entrypoint.
		//
		// For example, given a schema:
		//
		// ```
		// definition user {}
		//
		// definition parent1 {
		//   relation somerel: user
		// }
		//
		// definition parent2 {
		//   relation somerel: user
		// }
		//
		// definition child {
		//   relation parent: parent1 | parent2
		//   permission someperm = parent->somerel
		// }
		// ```
		//
		// We will add an entrypoint for the arrow itself, keyed to the relation type
		// included from the computed userset.
		//
		// Using the above example, this will add entrypoints for `parent1#somerel`
		// and `parent2#somerel`, which are the subjects reached after resolving the
		// right side of the arrow.

		// Check if the relation does exist on the allowed type, and only add the entrypoint if present.
		relDef, err := def.ts.GetDefinition(ctx, allowedRelationType.GetNamespace())
		if err != nil {
			return err
		}

		if relDef.HasRelation(computedUsersetRelation) {
			err := addSubjectEntrypoint(graph, allowedRelationType.GetNamespace(), computedUsersetRelation, &core.ReachabilityEntrypoint{
				Kind:                    core.ReachabilityEntrypoint_TUPLESET_TO_USERSET_ENTRYPOINT,
				TargetRelation:          rr,
				ResultStatus:            operationResultState,
				ComputedUsersetRelation: computedUsersetRelation,
				TuplesetRelation:        tuplesetRelation,
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func addSubjectEntrypoint(graph *core.ReachabilityGraph, namespaceName string, relationName string, entrypoint *core.ReachabilityEntrypoint) error {
	key := tuple.JoinRelRef(namespaceName, relationName)
	if relationName == "" {
		return spiceerrors.MustBugf("found empty relation name for subject entrypoint")
	}

	if graph.GetEntrypointsBySubjectRelation()[key] == nil {
		graph.EntrypointsBySubjectRelation[key] = &core.ReachabilityEntrypoints{
			Entrypoints: []*core.ReachabilityEntrypoint{},
			SubjectRelation: &core.RelationReference{
				Namespace: namespaceName,
				Relation:  relationName,
			},
		}
	}

	graph.EntrypointsBySubjectRelation[key].Entrypoints = append(
		graph.EntrypointsBySubjectRelation[key].Entrypoints,
		entrypoint,
	)

	return nil
}

func addSubjectLinks(graph *core.ReachabilityGraph, operationResultState core.ReachabilityEntrypoint_EntrypointResultStatus, relation *core.Relation, def *Definition) error {
	typeInfo := relation.GetTypeInformation()
	if typeInfo == nil {
		return fmt.Errorf("missing type information for relation %s#%s", def.nsDef.GetName(), relation.GetName())
	}

	rr := &core.RelationReference{
		Namespace: def.nsDef.GetName(),
		Relation:  relation.GetName(),
	}

	allowedDirectRelations := typeInfo.GetAllowedDirectRelations()
	for _, directRelation := range allowedDirectRelations {
		// If the allowed relation is a wildcard, add it as a subject *type* entrypoint, rather than
		// a subject relation.
		if directRelation.GetPublicWildcard() != nil {
			if graph.GetEntrypointsBySubjectType()[directRelation.GetNamespace()] == nil {
				graph.EntrypointsBySubjectType[directRelation.GetNamespace()] = &core.ReachabilityEntrypoints{
					Entrypoints: []*core.ReachabilityEntrypoint{},
					SubjectType: directRelation.GetNamespace(),
				}
			}

			graph.EntrypointsBySubjectType[directRelation.GetNamespace()].Entrypoints = append(
				graph.EntrypointsBySubjectType[directRelation.GetNamespace()].Entrypoints,
				&core.ReachabilityEntrypoint{
					Kind:           core.ReachabilityEntrypoint_RELATION_ENTRYPOINT,
					TargetRelation: rr,
					ResultStatus:   operationResultState,
				},
			)
			continue
		}

		err := addSubjectEntrypoint(graph, directRelation.GetNamespace(), directRelation.GetRelation(), &core.ReachabilityEntrypoint{
			Kind:           core.ReachabilityEntrypoint_RELATION_ENTRYPOINT,
			TargetRelation: rr,
			ResultStatus:   operationResultState,
		})
		if err != nil {
			return err
		}
	}

	return nil
}
