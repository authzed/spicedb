package namespace

import (
	"context"
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type reachabilityOption int

const (
	reachabilityFull reachabilityOption = iota
	reachabilityOptimized
)

func computeReachability(ctx context.Context, ts *TypeSystem, relationName string, option reachabilityOption) (*core.ReachabilityGraph, error) {
	targetRelation, ok := ts.relationMap[relationName]
	if !ok {
		return nil, fmt.Errorf("relation `%s` not found under type `%s` missing when computing reachability", relationName, ts.nsDef.Name)
	}

	if !ts.HasTypeInformation(relationName) && targetRelation.GetUsersetRewrite() == nil {
		return nil, fmt.Errorf("relation `%s` missing type information when computing reachability for namespace `%s`", relationName, ts.nsDef.Name)
	}

	graph := &core.ReachabilityGraph{
		EntrypointsBySubjectType:     map[string]*core.ReachabilityEntrypoints{},
		EntrypointsBySubjectRelation: map[string]*core.ReachabilityEntrypoints{},
	}

	usersetRewrite := targetRelation.GetUsersetRewrite()
	if usersetRewrite != nil {
		return graph, computeRewriteReachability(ctx, graph, usersetRewrite, core.ReachabilityEntrypoint_DIRECT_OPERATION_RESULT, targetRelation, ts, option)
	}

	// If there is no userRewrite, then we have a relation and its entrypoints will all be
	// relation entrypoints.
	return graph, addSubjectLinks(graph, core.ReachabilityEntrypoint_DIRECT_OPERATION_RESULT, targetRelation, ts)
}

func computeRewriteReachability(ctx context.Context, graph *core.ReachabilityGraph, rewrite *core.UsersetRewrite, operationResultState core.ReachabilityEntrypoint_EntrypointResultStatus, targetRelation *core.Relation, ts *TypeSystem, option reachabilityOption) error {
	switch rw := rewrite.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		return computeRewriteOpReachability(ctx, rw.Union.Child, operationResultState, graph, targetRelation, ts, option)

	case *core.UsersetRewrite_Intersection:
		// If optimized mode is set, only return the first child of the intersection.
		if option == reachabilityOptimized {
			return computeRewriteOpReachability(ctx, rw.Intersection.Child[0:1], core.ReachabilityEntrypoint_REACHABLE_CONDITIONAL_RESULT, graph, targetRelation, ts, option)
		}

		return computeRewriteOpReachability(ctx, rw.Intersection.Child, core.ReachabilityEntrypoint_REACHABLE_CONDITIONAL_RESULT, graph, targetRelation, ts, option)

	case *core.UsersetRewrite_Exclusion:
		// If optimized mode is set, only return the first child of the exclusion.
		if option == reachabilityOptimized {
			return computeRewriteOpReachability(ctx, rw.Exclusion.Child[0:1], core.ReachabilityEntrypoint_REACHABLE_CONDITIONAL_RESULT, graph, targetRelation, ts, option)
		}

		return computeRewriteOpReachability(ctx, rw.Exclusion.Child, core.ReachabilityEntrypoint_REACHABLE_CONDITIONAL_RESULT, graph, targetRelation, ts, option)

	default:
		return fmt.Errorf("unknown kind of userset rewrite in reachability computation: %T", rw)
	}
}

func computeRewriteOpReachability(ctx context.Context, children []*core.SetOperation_Child, operationResultState core.ReachabilityEntrypoint_EntrypointResultStatus, graph *core.ReachabilityGraph, targetRelation *core.Relation, ts *TypeSystem, option reachabilityOption) error {
	rr := &core.RelationReference{
		Namespace: ts.nsDef.Name,
		Relation:  targetRelation.Name,
	}

	for _, childOneof := range children {
		switch child := childOneof.ChildType.(type) {
		case *core.SetOperation_Child_XThis:
			return fmt.Errorf("use of _this is unsupported; please rewrite your schema")

		case *core.SetOperation_Child_ComputedUserset:
			// A computed userset adds an entrypoint indicating that the relation is rewritten.
			addSubjectEntrypoint(graph, ts.nsDef.Name, child.ComputedUserset.Relation, &core.ReachabilityEntrypoint{
				Kind:           core.ReachabilityEntrypoint_COMPUTED_USERSET_ENTRYPOINT,
				TargetRelation: rr,
				ResultStatus:   operationResultState,
			})

		case *core.SetOperation_Child_UsersetRewrite:
			err := computeRewriteReachability(ctx, graph, child.UsersetRewrite, operationResultState, targetRelation, ts, option)
			if err != nil {
				return err
			}

		case *core.SetOperation_Child_TupleToUserset:
			tuplesetRelation := child.TupleToUserset.Tupleset.Relation
			directRelationTypes, err := ts.AllowedDirectRelationsAndWildcards(tuplesetRelation)
			if err != nil {
				return err
			}

			computedUsersetRelation := child.TupleToUserset.ComputedUserset.Relation
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
				relTypeSystem, err := ts.typeSystemForNamespace(ctx, allowedRelationType.Namespace)
				if err != nil {
					return err
				}

				if relTypeSystem.HasRelation(computedUsersetRelation) {
					addSubjectEntrypoint(graph, allowedRelationType.Namespace, computedUsersetRelation, &core.ReachabilityEntrypoint{
						Kind:             core.ReachabilityEntrypoint_TUPLESET_TO_USERSET_ENTRYPOINT,
						TargetRelation:   rr,
						ResultStatus:     operationResultState,
						TuplesetRelation: tuplesetRelation,
					})
				}
			}

		case *core.SetOperation_Child_XNil:
			// nil has no entrypoints.
			return nil

		default:
			return fmt.Errorf("unknown set operation child `%T` in reachability graph building", child)
		}
	}

	return nil
}

func addSubjectEntrypoint(graph *core.ReachabilityGraph, namespaceName string, relationName string, entrypoint *core.ReachabilityEntrypoint) {
	key := relationKey(namespaceName, relationName)
	if relationName == "" {
		panic("found empty relation name for subject entrypoint")
	}

	if graph.EntrypointsBySubjectRelation[key] == nil {
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
}

func addSubjectLinks(graph *core.ReachabilityGraph, operationResultState core.ReachabilityEntrypoint_EntrypointResultStatus, relation *core.Relation, ts *TypeSystem) error {
	typeInfo := relation.GetTypeInformation()
	if typeInfo == nil {
		return fmt.Errorf("missing type information for relation %s#%s", ts.nsDef.Name, relation.Name)
	}

	rr := &core.RelationReference{
		Namespace: ts.nsDef.Name,
		Relation:  relation.Name,
	}

	allowedDirectRelations := typeInfo.GetAllowedDirectRelations()
	for _, directRelation := range allowedDirectRelations {
		// If the allowed relation is a wildcard, add it as a subject *type* entrypoint, rather than
		// a subject relation.
		if directRelation.GetPublicWildcard() != nil {
			if graph.EntrypointsBySubjectType[directRelation.Namespace] == nil {
				graph.EntrypointsBySubjectType[directRelation.Namespace] = &core.ReachabilityEntrypoints{
					Entrypoints: []*core.ReachabilityEntrypoint{},
					SubjectType: directRelation.Namespace,
				}
			}

			graph.EntrypointsBySubjectType[directRelation.Namespace].Entrypoints = append(
				graph.EntrypointsBySubjectType[directRelation.Namespace].Entrypoints,
				&core.ReachabilityEntrypoint{
					Kind:           core.ReachabilityEntrypoint_RELATION_ENTRYPOINT,
					TargetRelation: rr,
					ResultStatus:   operationResultState,
				},
			)
			continue
		}

		addSubjectEntrypoint(graph, directRelation.Namespace, directRelation.GetRelation(), &core.ReachabilityEntrypoint{
			Kind:           core.ReachabilityEntrypoint_RELATION_ENTRYPOINT,
			TargetRelation: rr,
			ResultStatus:   operationResultState,
		})
	}

	return nil
}

func relationKey(namespaceName string, relationName string) string {
	return fmt.Sprintf("%s#%s", namespaceName, relationName)
}
