package compiler

import (
	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// DSLNode is a node in the DSL AST.
type DSLNode interface {
	GetType() dslshape.NodeType
	GetString(predicateName string) (string, error)
}

// NodeChain is a chain of nodes in the DSL AST.
type NodeChain struct {
	nodes []DSLNode
}

// Head returns the head node of the chain.
func (nc *NodeChain) Head() DSLNode {
	return nc.nodes[0]
}

// HasHeadType returns true if the head node of the chain is of the given type.
func (nc *NodeChain) HasHeadType(nodeType dslshape.NodeType) bool {
	return nc.nodes[0].GetType() == nodeType
}

// FindNodeOfType returns the first node of the given type in the chain, if any.
func (nc *NodeChain) FindNodeOfType(nodeType dslshape.NodeType) DSLNode {
	for _, node := range nc.nodes {
		if node.GetType() == nodeType {
			return node
		}
	}

	return nil
}

func (nc *NodeChain) String() string {
	var out string
	for _, node := range nc.nodes {
		out += node.GetType().String() + " "
	}
	return out

}

// PositionToAstNodeChain returns the AST node, and its parents (if any), found at the given position in the source, if any.
func PositionToAstNodeChain(schema *CompiledSchema, source input.Source, position input.Position) (*NodeChain, error) {
	rootSource, err := schema.rootNode.GetString(dslshape.NodePredicateSource)
	if err != nil {
		return nil, err
	}

	if rootSource != string(source) {
		return nil, nil
	}

	// Map the position to a file rune.
	runePosition, err := schema.mapper.LineAndColToRunePosition(position.LineNumber, position.ColumnPosition, source)
	if err != nil {
		return nil, err
	}

	// Find the node at the rune position.
	found, err := runePositionToAstNodeChain(schema.rootNode, runePosition)
	if err != nil {
		return nil, err
	}

	if found == nil {
		return nil, nil
	}

	return &NodeChain{nodes: found}, nil
}

func runePositionToAstNodeChain(node *dslNode, runePosition int) ([]DSLNode, error) {
	startRune, err := node.GetInt(dslshape.NodePredicateStartRune)
	if err != nil {
		return nil, err
	}

	endRune, err := node.GetInt(dslshape.NodePredicateEndRune)
	if err != nil {
		return nil, err
	}

	if runePosition < startRune || runePosition > endRune {
		return nil, nil
	}

	for _, child := range node.AllSubNodes() {
		childChain, err := runePositionToAstNodeChain(child, runePosition)
		if err != nil {
			return nil, err
		}

		if childChain != nil {
			return append(childChain, node), nil
		}
	}

	return []DSLNode{node}, nil
}
