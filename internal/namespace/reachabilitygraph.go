package namespace

import (
	"context"
	"fmt"

	"github.com/jwangsadinata/go-multimap/slicemultimap"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

// ReachabilityGraph is a graph which holds the various paths of reachability of objects
// of a particular Object relation from a set of Subject relations.
type ReachabilityGraph struct {
	namespaceName string
	relationName  string

	// rootNode holds the root node for the structural tree. This is
	// the node representing the relation itself.
	rootNode *rgStructuralNode

	// relationToEntrypoints is a multimap from a relation string (as returned by relationKey)
	// to the entrypoints into the reachability graph.
	relationToEntrypoints *slicemultimap.MultiMap

	// subGraphs are the map of subgraphs reachable from this graph (by relationKey).
	subGraphs map[string]*ReachabilityGraph
}

// ReachabilityEntrypointKind defines the various kinds of entrypoints into the reachability
// graph.
type ReachabilityEntrypointKind int

const (
	// SubjectEntrypoint is an entrypoint for the subject of a tuple: the right side of
	// any tuple with the matching relation can enter here.
	SubjectEntrypoint ReachabilityEntrypointKind = iota

	// AliasedRelationEntrypoint is an entrypoint for an aliased relation: the specified
	// relation is "rewritten" (aliased) to the target relation (this is from a computed_userset
	// or the top branch of a tupleset_to_userset)
	AliasedRelationEntrypoint

	// WalkedRelationEntrypoint is an entrypoint for a tuple that is walked a single step:
	// the tuple's relation is rewritten based on the allowed right side relation and then
	// a walk is performed (this is from the computed_userset branch of a TTU)
	WalkedRelationEntrypoint
)

// ReachabilityEntrypoint defines an entrypoint into the reachability graph.
type ReachabilityEntrypoint struct {
	node                 *rgStructuralNode
	kind                 ReachabilityEntrypointKind
	subjectRelationKinds []*pb.RelationReference
}

func (re ReachabilityEntrypoint) id() string {
	return re.node.nodeID
}

// AliasingRelation is the relation in which the current tuple should be aliased.
// Only applies to AliasedRelationEntrypoint.
func (re ReachabilityEntrypoint) AliasingRelation() (string, string) {
	if re.kind != AliasedRelationEntrypoint {
		panic("Invalid kind")
	}

	return re.node.graph.namespaceName, re.node.graph.relationName
}

// SubjectTargetRelation is the relation to which the current tuple should walk via
// a reverse tuple query.
// Only applies to SubjectEntrypoint.
func (re ReachabilityEntrypoint) SubjectTargetRelation() (string, string) {
	if re.kind != SubjectEntrypoint {
		panic("Invalid kind")
	}

	return re.node.graph.namespaceName, re.node.graph.relationName
}

// WalkRelationAndTypes returns the relation namespace, name and allowed right side types
// for a WalkedRelationEntrypoint.
func (re ReachabilityEntrypoint) WalkRelationAndTypes() (string, string, []*pb.RelationReference) {
	if re.kind != WalkedRelationEntrypoint {
		panic("Invalid kind")
	}

	return re.node.graph.namespaceName, re.node.tupleToUserset.Tupleset.Relation, re.subjectRelationKinds
}

// Kind returns the kind of the entrypoint.
func (re ReachabilityEntrypoint) Kind() ReachabilityEntrypointKind {
	return re.kind
}

// DescribePath returns a human-readable description of the endpoint's path for debugging.
func (re ReachabilityEntrypoint) DescribePath() string {
	return re.node.DescribePath()
}

// Describe returns a human-readable description of the endpoint for debugging.
func (re ReachabilityEntrypoint) Describe() string {
	return fmt.Sprintf("%v: %s", re.Kind(), re.node.Describe())
}

// Entrypoints returns all the entrypoints into the reachability graph for the given subject relation.
func (rg *ReachabilityGraph) Entrypoints(namespaceName string, relationName string) []ReachabilityEntrypoint {
	entrypointsMap := map[string]ReachabilityEntrypoint{}
	rg.collectEntrypoints(namespaceName, relationName, entrypointsMap, map[string]bool{})

	entrypoints := []ReachabilityEntrypoint{}
	for _, entrypoint := range entrypointsMap {
		entrypoints = append(entrypoints, entrypoint)
	}
	return entrypoints
}

func (rg *ReachabilityGraph) collectEntrypoints(namespaceName string, relationName string, entrypoints map[string]ReachabilityEntrypoint, encountered map[string]bool) {
	key := relationKey(rg.namespaceName, rg.relationName)
	if _, ok := encountered[key]; ok {
		return
	}
	encountered[key] = true

	// Local entrypoints.
	found, ok := rg.relationToEntrypoints.Get(relationKey(namespaceName, relationName))
	if ok {
		for _, entrypointRef := range found {
			entrypoint := entrypointRef.(ReachabilityEntrypoint)
			entrypoints[entrypoint.id()] = entrypoint
		}
	}

	// Check sub graphs.
	for _, subGraph := range rg.subGraphs {
		subGraph.collectEntrypoints(namespaceName, relationName, entrypoints, encountered)
	}
}

// rgStructuralNode represents a structural node for the relation.
type rgStructuralNode struct {
	parentNode *rgStructuralNode
	graph      *ReachabilityGraph
	nodeID     string

	relation       *pb.Relation
	rewrite        *pb.UsersetRewrite
	tupleToUserset *pb.TupleToUserset

	childThis            *pb.SetOperation_Child_XThis
	childComputedUserset *pb.SetOperation_Child_ComputedUserset
	childTupleToUserset  *pb.SetOperation_Child_TupleToUserset
	childUsersetRewrite  *pb.SetOperation_Child_UsersetRewrite
}

// Describe returns a human-readable description of the node itself.
func (rgn *rgStructuralNode) Describe() string {
	if rgn.rewrite != nil {
		switch rgn.rewrite.RewriteOperation.(type) {
		case *pb.UsersetRewrite_Union:
			return fmt.Sprintf("union (#%s)", rgn.nodeID)
		case *pb.UsersetRewrite_Intersection:
			return fmt.Sprintf("intersection (#%s)", rgn.nodeID)
		case *pb.UsersetRewrite_Exclusion:
			return fmt.Sprintf("exclusion (#%s)", rgn.nodeID)
		default:
			panic(fmt.Errorf("Unknown kind of userset rewrite"))
		}
	}

	if rgn.relation != nil {
		return fmt.Sprintf("%s::%s", rgn.graph.namespaceName, rgn.relation.Name)
	}

	if rgn.tupleToUserset != nil {
		return fmt.Sprintf("%s<=%s (#%s)", rgn.tupleToUserset.Tupleset.Relation, rgn.tupleToUserset.ComputedUserset.Relation, rgn.nodeID)
	}

	if rgn.childThis != nil {
		return "_this"
	}

	if rgn.childComputedUserset != nil {
		return fmt.Sprintf("<-%s", rgn.childComputedUserset.ComputedUserset.Relation)
	}

	if rgn.childTupleToUserset != nil {
		return ""
	}

	if rgn.childUsersetRewrite != nil {
		return ""
	}

	return "unknown"
}

// DescribePath returns a human-readable description of the node and its path.
func (rgn *rgStructuralNode) DescribePath() string {
	if rgn.parentNode == nil {
		return rgn.Describe()
	}

	return fmt.Sprintf("%s -> %s", rgn.Describe(), rgn.parentNode.DescribePath())
}

type buildContext struct {
	ctx            context.Context
	typeSystem     *NamespaceTypeSystem
	constructing   *ReachabilityGraph
	relation       *pb.Relation
	relationGraphs map[string]*ReachabilityGraph
	nodeCounter    int
}

func (bctx *buildContext) getNodeID() string {
	bctx.nodeCounter++
	return fmt.Sprintf("%v::%v#%v", bctx.typeSystem.nsDef.Name, bctx.relation.Name, bctx.nodeCounter)
}

func buildRelationReachabilityGraph(
	ctx context.Context,
	typeSystem *NamespaceTypeSystem,
	relation *pb.Relation,
	relationGraphs map[string]*ReachabilityGraph) (*ReachabilityGraph, error) {
	if relation == nil {
		panic("Got nil relation")
	}

	// Ensure that the relation's graph has not already been built. If so,
	// nothing more to do.
	namespaceName := typeSystem.nsDef.Name
	key := relationKey(namespaceName, relation.Name)

	existing, ok := relationGraphs[key]
	if ok {
		return existing, nil
	}

	// Place the incomplete graph into the map to allow for recursive relations.
	graph := &ReachabilityGraph{
		namespaceName: namespaceName,
		relationName:  relation.Name,

		rootNode:              nil,
		relationToEntrypoints: slicemultimap.New(),
		subGraphs:             map[string]*ReachabilityGraph{},
	}

	relationGraphs[key] = graph

	// Start construction of the structural tree. This will also walk
	// the defined direct relations and build any graphs for referenced relations.
	bctx := &buildContext{
		ctx:            ctx,
		typeSystem:     typeSystem,
		relationGraphs: relationGraphs,
		constructing:   graph,
		relation:       relation,
	}

	err := mapRelationNode(relation, bctx)
	if err != nil {
		return nil, err
	}

	return graph, nil
}

func mapRelationNode(relation *pb.Relation, bctx *buildContext) error {
	node := &rgStructuralNode{
		graph:    bctx.constructing,
		relation: relation,
		nodeID:   bctx.getNodeID(),
	}
	bctx.constructing.rootNode = node

	usersetRewrite := relation.GetUsersetRewrite()
	if usersetRewrite != nil {
		return mapRewriteNode(usersetRewrite, node, bctx)
	}

	// If there is no userRewrite, then the subject links go directly to the
	// relation.
	return addSubjectLinks(node, bctx)
}

func mapRewriteNode(rewrite *pb.UsersetRewrite, parentNode *rgStructuralNode, bctx *buildContext) error {
	node := &rgStructuralNode{
		parentNode: parentNode,
		graph:      bctx.constructing,
		nodeID:     bctx.getNodeID(),

		rewrite: rewrite,
	}

	switch rw := rewrite.RewriteOperation.(type) {
	case *pb.UsersetRewrite_Union:
		err := mapRewriteOperationNodes(rw.Union, node, bctx)
		if err != nil {
			return err
		}
	case *pb.UsersetRewrite_Intersection:
		err := mapRewriteOperationNodes(rw.Intersection, node, bctx)
		if err != nil {
			return err
		}

	case *pb.UsersetRewrite_Exclusion:
		err := mapRewriteOperationNodes(rw.Exclusion, node, bctx)
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("Unknown kind of userset rewrite")
	}

	return nil
}

func mapRewriteOperationNodes(so *pb.SetOperation, parentNode *rgStructuralNode, bctx *buildContext) error {
	for _, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *pb.SetOperation_Child_XThis:
			opNode := &rgStructuralNode{
				graph:      bctx.constructing,
				nodeID:     bctx.getNodeID(),
				parentNode: parentNode,

				childThis: child,
			}

			// A _this{} indicates subject links directly to the operation.
			err := addSubjectLinks(opNode, bctx)
			if err != nil {
				return err
			}

		case *pb.SetOperation_Child_ComputedUserset:
			opNode := &rgStructuralNode{
				graph:      bctx.constructing,
				nodeID:     bctx.getNodeID(),
				parentNode: parentNode,

				childComputedUserset: child,
			}

			// A computed userset adds a backlink from the referenced relation.
			relationName := child.ComputedUserset.Relation
			err := addIncomingSubGraph(parentNode, bctx.typeSystem.nsDef.Name, relationName, bctx)
			if err != nil {
				return err
			}

			// Add a link from the referenced relation to this operation.
			bctx.constructing.relationToEntrypoints.Put(
				relationKey(bctx.typeSystem.nsDef.Name, relationName),
				ReachabilityEntrypoint{
					node: opNode,
					kind: AliasedRelationEntrypoint,
				},
			)

		case *pb.SetOperation_Child_UsersetRewrite:
			opNode := &rgStructuralNode{
				graph:      bctx.constructing,
				nodeID:     bctx.getNodeID(),
				parentNode: parentNode,

				childUsersetRewrite: child,
			}

			err := mapRewriteNode(child.UsersetRewrite, opNode, bctx)
			if err != nil {
				return err
			}

		case *pb.SetOperation_Child_TupleToUserset:
			opNode := &rgStructuralNode{
				graph:      bctx.constructing,
				nodeID:     bctx.getNodeID(),
				parentNode: parentNode,

				childTupleToUserset: child,
			}

			ttu := child.TupleToUserset
			ttuNode := &rgStructuralNode{
				parentNode:     opNode,
				graph:          bctx.constructing,
				nodeID:         bctx.getNodeID(),
				tupleToUserset: ttu,
			}

			// Follow the type information to any relations allowed and use them to determine
			// the next set of relations.
			tuplesetRelation := ttu.Tupleset.Relation
			directRelationTypes, err := bctx.typeSystem.AllowedDirectRelations(tuplesetRelation)
			if err != nil {
				return err
			}

			// Add an alias from the tupleset relation to the parent relation.
			bctx.constructing.relationToEntrypoints.Put(
				relationKey(bctx.constructing.namespaceName, tuplesetRelation),
				ReachabilityEntrypoint{
					node: opNode,
					kind: AliasedRelationEntrypoint,
				},
			)

			computedUsersetRelation := ttu.ComputedUserset.Relation
			for _, allowedRelationType := range directRelationTypes {
				err := addIncomingSubGraph(ttuNode, allowedRelationType.Namespace, computedUsersetRelation, bctx)
				if err != nil {
					return err
				}

				// Add a walk from the referenced relation to the tupleset relation.
				bctx.constructing.relationToEntrypoints.Put(
					relationKey(allowedRelationType.Namespace, computedUsersetRelation),
					ReachabilityEntrypoint{
						node:                 ttuNode,
						kind:                 WalkedRelationEntrypoint,
						subjectRelationKinds: directRelationTypes,
					},
				)
			}
		}
	}

	return nil
}

// addSubjectLinks adds links from the defined allowed subject relation types on the
// relation to the given node.
func addSubjectLinks(node *rgStructuralNode, bctx *buildContext) error {
	typeInfo := bctx.relation.GetTypeInformation()
	if typeInfo == nil {
		return fmt.Errorf("Missing type information for relation %s", bctx.relation.Name)
	}

	allowedDirectRelations := typeInfo.GetAllowedDirectRelations()
	for _, directRelation := range allowedDirectRelations {
		// Add a subject entrypoint.
		bctx.constructing.relationToEntrypoints.Put(
			relationRefKey(directRelation),
			ReachabilityEntrypoint{
				node: node,
				kind: SubjectEntrypoint,
			},
		)

		if directRelation.Relation != "..." {
			// If there is a non-terminal relation here, then it needs to be followed into
			// this node.
			err := addIncomingSubGraph(node, directRelation.Namespace, directRelation.Relation, bctx)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// addIncomingSubGraph builds a reachability graph for the given relation and adds it to the current
// graph's subgraphs map.
func addIncomingSubGraph(node *rgStructuralNode, namespaceName string, relationName string, bctx *buildContext) error {
	namespaceTS := bctx.typeSystem
	if namespaceName != bctx.typeSystem.nsDef.Name {
		_, ts, _, err := bctx.typeSystem.manager.ReadNamespaceAndTypes(bctx.ctx, namespaceName)
		if err != nil {
			return fmt.Errorf("Error when reading referenced namespace %s: %w", namespaceName, err)
		}
		namespaceTS = ts
	}

	found, ok := namespaceTS.relationMap[relationName]
	if !ok {
		return fmt.Errorf("Unknown relation %s in namespace %s", relationName, namespaceName)
	}

	graph, err := buildRelationReachabilityGraph(bctx.ctx, namespaceTS, found, bctx.relationGraphs)
	if err != nil {
		return err
	}

	bctx.constructing.subGraphs[relationKey(namespaceName, relationName)] = graph
	return nil
}

func relationKey(namespaceName string, relationName string) string {
	return fmt.Sprintf("%s::%s", namespaceName, relationName)
}

func relationRefKey(ref *pb.RelationReference) string {
	return relationKey(ref.Namespace, ref.Relation)
}
