package namespace

import (
	"context"
	"fmt"

	"github.com/jwangsadinata/go-multimap/slicemultimap"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

// RelationKey is a key from a relation.
type RelationKey string

// NodeID is an ID for a node.
type NodeID string

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

	// nodeIDMap contains the map from node ID to node in this graph.
	nodeIDMap map[NodeID]*rgStructuralNode

	// subGraphs are the map of subgraphs reachable from this graph (by relationKey).
	subGraphs map[RelationKey]*ReachabilityGraph

	// nodeCounter holds a counter for generating unique node IDs for this graph. Note
	// that IDs will not be unique across the subgraphs, which is why newNode prefixes
	// the node IDs with the namespace and relation.
	nodeCounter int
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

func (re ReachabilityEntrypoint) id() NodeID {
	return re.node.nodeID
}

// AliasingRelation is the relation in which the current tuple should be aliased.
// Only applies to AliasedRelationEntrypoint.
func (re ReachabilityEntrypoint) AliasingRelation() (string, string, error) {
	if re.kind != AliasedRelationEntrypoint {
		return "", "", fmt.Errorf("%s is not an aliased relation entrypoint", re.node.Describe())
	}

	return re.node.graph.namespaceName, re.node.graph.relationName, nil
}

// SubjectTargetRelation is the relation to which the current tuple should walk via
// a reverse tuple query.
// Only applies to SubjectEntrypoint.
func (re ReachabilityEntrypoint) SubjectTargetRelation() (string, string, error) {
	if re.kind != SubjectEntrypoint {
		return "", "", fmt.Errorf("%s is not an subject target entrypoint", re.node.Describe())
	}

	return re.node.graph.namespaceName, re.node.graph.relationName, nil
}

// WalkRelationAndTypes returns the relation namespace, name and allowed right side types
// for a WalkedRelationEntrypoint.
func (re ReachabilityEntrypoint) WalkRelationAndTypes() (string, string, []*pb.RelationReference, error) {
	if re.kind != WalkedRelationEntrypoint {
		return "", "", []*pb.RelationReference{}, fmt.Errorf("%s is not a walked relation entrypoint", re.node.Describe())
	}

	return re.node.graph.namespaceName, re.node.tupleToUserset.Tupleset.Relation, re.subjectRelationKinds, nil
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

// ReductionNodeID returns the node ID of entrypoint's node, if it is under a node that requires
// reduction before continuation of a reverse walk. A reduction is required for intersection
// and exclusion operations (union is just deduplicated after the fact).
func (re ReachabilityEntrypoint) ReductionNodeID() (NodeID, error) {
	return re.node.reductionNodeID()
}

// Entrypoints returns all the entrypoints into the reachability graph for the given subject relation.
func (rg *ReachabilityGraph) Entrypoints(namespaceName string, relationName string) []ReachabilityEntrypoint {
	entrypointsMap := map[NodeID]ReachabilityEntrypoint{}
	rg.collectEntrypoints(namespaceName, relationName, entrypointsMap, map[RelationKey]bool{})

	entrypoints := []ReachabilityEntrypoint{}
	for _, entrypoint := range entrypointsMap {
		entrypoints = append(entrypoints, entrypoint)
	}
	return entrypoints
}

func (rg *ReachabilityGraph) collectEntrypoints(namespaceName string, relationName string, entrypoints map[NodeID]ReachabilityEntrypoint, encountered map[RelationKey]bool) {
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

func (rg *ReachabilityGraph) nodeWithID(nodeID NodeID, encountered map[RelationKey]bool) *rgStructuralNode {
	key := relationKey(rg.namespaceName, rg.relationName)
	if _, ok := encountered[key]; ok {
		return nil
	}
	encountered[key] = true

	node, ok := rg.nodeIDMap[nodeID]
	if ok {
		return node
	}

	// Check sub graphs.
	for _, subGraph := range rg.subGraphs {
		found := subGraph.nodeWithID(nodeID, encountered)
		if found != nil {
			return found
		}
	}

	return nil
}

func (rg *ReachabilityGraph) newNode(parentNode *rgStructuralNode) *rgStructuralNode {
	rg.nodeCounter++
	nodeID := NodeID(fmt.Sprintf("%v#%v::%v", rg.namespaceName, rg.relationName, rg.nodeCounter))
	node := &rgStructuralNode{
		graph:      rg,
		nodeID:     nodeID,
		parentNode: parentNode,
	}
	if parentNode != nil {
		parentNode.childNodes = append(parentNode.childNodes, node)
	}

	rg.nodeIDMap[node.nodeID] = node
	return node
}

// NewReducer returns a new Reducer helper for this graph.
func (rg *ReachabilityGraph) NewReducer() *Reducer {
	return &Reducer{
		graph:              rg,
		rewriteNodes:       map[NodeID]*rgStructuralNode{},
		resultsByChildNode: slicemultimap.New(),
	}
}

// Reducer is a helper which provides methods for performing reduction over
// intersection or exclusion ONR sets.
type Reducer struct {
	graph              *ReachabilityGraph
	rewriteNodes       map[NodeID]*rgStructuralNode
	resultsByChildNode *slicemultimap.MultiMap
}

// Add adds the given ONR as an incoming result for reduction via the reduction node.
func (r *Reducer) Add(reductionNodeID NodeID, onr *pb.ObjectAndRelation) error {
	node := r.graph.nodeWithID(reductionNodeID, map[RelationKey]bool{})
	if node == nil {
		return fmt.Errorf("unknown node for reduction: %s", reductionNodeID)
	}

	rewriteChildNode := node.rewriteChildNode()
	parentRewriteNode := rewriteChildNode.parentNode

	r.rewriteNodes[parentRewriteNode.nodeID] = parentRewriteNode
	r.resultsByChildNode.Put(rewriteChildNode.nodeID, onr)
	return nil
}

// Empty returns whether there are any rewrites necessary to be reduced.
func (r *Reducer) Empty() bool {
	return len(r.rewriteNodes) == 0
}

func (r *Reducer) runIntersection(
	rewriteNode *rgStructuralNode,
	intersection *pb.SetOperation,
	results *ONRSet) error {

	found := NewONRSet()
	for index := range intersection.Child {
		childNode := rewriteNode.childNodes[index]
		childResults, ok := r.resultsByChildNode.Get(childNode.nodeID)
		if !ok || len(childResults) == 0 {
			return nil
		}

		onrs := NewONRSet()
		for _, result := range childResults {
			onr := result.(*pb.ObjectAndRelation)
			if onr.Namespace != r.graph.namespaceName {
				return fmt.Errorf("invalid namespace for intersection reduction on node %s: expected %s, found %s", childNode.Describe(), r.graph.namespaceName, onr.Namespace)
			}

			// NOTE: The relation gets rewritten here to the relation that contains
			// this rewrite.
			onrs.Add(&pb.ObjectAndRelation{
				Namespace: onr.Namespace,
				ObjectId:  onr.ObjectId,
				Relation:  r.graph.relationName,
			})
		}

		if index == 0 {
			found = onrs
		} else {
			found = found.Intersect(onrs)
		}

		if found.IsEmpty() {
			return nil
		}
	}

	results.UpdateFrom(found)
	return nil
}

func (r *Reducer) runExclusion(
	rewriteNode *rgStructuralNode,
	exclusion *pb.SetOperation,
	results *ONRSet) error {

	found := NewONRSet()
	for index := range exclusion.Child {
		onrs := NewONRSet()

		childNode := rewriteNode.childNodes[index]
		childResults, ok := r.resultsByChildNode.Get(childNode.nodeID)
		if ok {
			for _, result := range childResults {
				onr := result.(*pb.ObjectAndRelation)
				if onr.Namespace != r.graph.namespaceName {
					return fmt.Errorf("invalid namespace for exclusion reduction on node %s: expected %s, found %s", childNode.Describe(), r.graph.namespaceName, onr.Namespace)
				}

				// NOTE: The relation gets rewritten here to the relation that contains
				// this rewrite.
				onrs.Add(&pb.ObjectAndRelation{
					Namespace: onr.Namespace,
					ObjectId:  onr.ObjectId,
					Relation:  r.graph.relationName,
				})
			}
		}

		if index == 0 {
			found = onrs
		} else {
			found = found.Subtract(onrs)
		}

		if found.IsEmpty() {
			return nil
		}
	}

	results.UpdateFrom(found)
	return nil
}

// Run performs reduction over all the rewrites+ONRs given to the Reducer, returning the
// resulting reachable ONRs, if any.
func (r *Reducer) Run() ([]*pb.ObjectAndRelation, error) {
	results := NewONRSet()

	for _, rewriteNode := range r.rewriteNodes {
		switch rw := rewriteNode.rewrite.RewriteOperation.(type) {
		case *pb.UsersetRewrite_Intersection:
			err := r.runIntersection(rewriteNode, rw.Intersection, results)
			if err != nil {
				return []*pb.ObjectAndRelation{}, err
			}

		case *pb.UsersetRewrite_Exclusion:
			err := r.runExclusion(rewriteNode, rw.Exclusion, results)
			if err != nil {
				return []*pb.ObjectAndRelation{}, err
			}

		default:
			return []*pb.ObjectAndRelation{}, fmt.Errorf("unknown or unsupported kind %v of userset rewrite in Reducer", rw)
		}
	}

	return results.AsSlice(), nil
}

// rgStructuralNode represents a structural node for the relation.
type rgStructuralNode struct {
	parentNode *rgStructuralNode
	childNodes []*rgStructuralNode

	graph  *ReachabilityGraph
	nodeID NodeID

	relation       *pb.Relation
	rewrite        *pb.UsersetRewrite
	tupleToUserset *pb.TupleToUserset

	childThis            *pb.SetOperation_Child_XThis
	childComputedUserset *pb.SetOperation_Child_ComputedUserset
	childTupleToUserset  *pb.SetOperation_Child_TupleToUserset
	childUsersetRewrite  *pb.SetOperation_Child_UsersetRewrite
}

// rewriteChildNode returns the current node if it is a child of a rewrite, or the parent node
// if this is the TTU node. Returns nil otherwise.
func (rgn *rgStructuralNode) rewriteChildNode() *rgStructuralNode {
	if rgn.childThis != nil || rgn.childComputedUserset != nil || rgn.childTupleToUserset != nil ||
		rgn.childUsersetRewrite != nil {
		return rgn
	}

	if rgn.tupleToUserset != nil {
		return rgn.parentNode
	}

	return nil
}

// reductionNodeID returns the ID of the parent reduction node of this node, if any, or
// empty if none. The reduction node is defined as the operation node under a rewrite
// for any exclusion or intersection.
func (rgn *rgStructuralNode) reductionNodeID() (NodeID, error) {
	if rgn.parentNode != nil {
		if rgn.parentNode.rewrite != nil {
			switch rgn.parentNode.rewrite.RewriteOperation.(type) {
			case *pb.UsersetRewrite_Union:
				break

			case *pb.UsersetRewrite_Intersection:
				return rgn.nodeID, nil

			case *pb.UsersetRewrite_Exclusion:
				return rgn.nodeID, nil

			default:
				return NodeID(""), fmt.Errorf("unknown kind of userset rewrite under reduction node ID")
			}
		}

		return rgn.parentNode.reductionNodeID()
	}

	return NodeID(""), nil
}

// Describe returns a human-readable description of the node itself.
func (rgn *rgStructuralNode) Describe() string {
	if rgn.rewrite != nil {
		switch t := rgn.rewrite.RewriteOperation.(type) {
		case *pb.UsersetRewrite_Union:
			return fmt.Sprintf("union (%s)", rgn.nodeID)
		case *pb.UsersetRewrite_Intersection:
			return fmt.Sprintf("intersection (%s)", rgn.nodeID)
		case *pb.UsersetRewrite_Exclusion:
			return fmt.Sprintf("exclusion (%s)", rgn.nodeID)
		default:
			return fmt.Sprintf("unknown-rewrite-node %v (%s)", t, rgn.nodeID)
		}
	}

	if rgn.relation != nil {
		return fmt.Sprintf("%s#%s", rgn.graph.namespaceName, rgn.relation.Name)
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
	relationGraphs map[RelationKey]*ReachabilityGraph
}

func buildRelationReachabilityGraph(
	ctx context.Context,
	typeSystem *NamespaceTypeSystem,
	relation *pb.Relation,
	relationGraphs map[RelationKey]*ReachabilityGraph) (*ReachabilityGraph, error) {
	if relation == nil {
		return nil, fmt.Errorf("got nil reduction when building reachability graph")
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
		nodeIDMap:             map[NodeID]*rgStructuralNode{},
		subGraphs:             map[RelationKey]*ReachabilityGraph{},
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
	node := bctx.constructing.newNode(nil)
	node.relation = relation

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
	node := bctx.constructing.newNode(parentNode)
	node.rewrite = rewrite

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
		return fmt.Errorf("unknown kind of userset rewrite")
	}

	return nil
}

func mapRewriteOperationNodes(so *pb.SetOperation, parentNode *rgStructuralNode, bctx *buildContext) error {
	for _, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *pb.SetOperation_Child_XThis:
			opNode := bctx.constructing.newNode(parentNode)
			opNode.childThis = child

			// A _this{} indicates subject links directly to the operation.
			err := addSubjectLinks(opNode, bctx)
			if err != nil {
				return err
			}

		case *pb.SetOperation_Child_ComputedUserset:
			opNode := bctx.constructing.newNode(parentNode)
			opNode.childComputedUserset = child

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
			opNode := bctx.constructing.newNode(parentNode)
			opNode.childUsersetRewrite = child

			err := mapRewriteNode(child.UsersetRewrite, opNode, bctx)
			if err != nil {
				return err
			}

		case *pb.SetOperation_Child_TupleToUserset:
			opNode := bctx.constructing.newNode(parentNode)
			opNode.childTupleToUserset = child

			ttu := child.TupleToUserset

			ttuNode := bctx.constructing.newNode(opNode)
			ttuNode.tupleToUserset = ttu

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
		return fmt.Errorf("missing type information for relation %s", bctx.relation.Name)
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
			return fmt.Errorf("error when reading referenced namespace %s: %w", namespaceName, err)
		}
		namespaceTS = ts
	}

	found, ok := namespaceTS.relationMap[relationName]
	if !ok {
		return nil
	}

	graph, err := buildRelationReachabilityGraph(bctx.ctx, namespaceTS, found, bctx.relationGraphs)
	if err != nil {
		return err
	}

	bctx.constructing.subGraphs[relationKey(namespaceName, relationName)] = graph
	return nil
}

func relationKey(namespaceName string, relationName string) RelationKey {
	return RelationKey(fmt.Sprintf("%s#%s", namespaceName, relationName))
}

func relationRefKey(ref *pb.RelationReference) RelationKey {
	return relationKey(ref.Namespace, ref.Relation)
}
