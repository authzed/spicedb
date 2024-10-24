package compiler

import (
	"github.com/authzed/spicedb/pkg/composableschemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/composableschemadsl/input"
)

// DSLNode is a node in the DSL AST.
type DSLNode interface {
	GetType() dslshape.NodeType
	GetString(predicateName string) (string, error)
	GetInt(predicateName string) (int, error)
	Lookup(predicateName string) (DSLNode, error)
}

// NodeChain is a chain of nodes in the DSL AST.
type NodeChain struct {
	nodes        []DSLNode
	runePosition int
}

// Head returns the head node of the chain.
func (nc *NodeChain) Head() DSLNode {
	return nc.nodes[0]
}

// HasHeadType returns true if the head node of the chain is of the given type.
func (nc *NodeChain) HasHeadType(nodeType dslshape.NodeType) bool {
	return nc.nodes[0].GetType() == nodeType
}

// ForRunePosition returns the rune position of the chain.
func (nc *NodeChain) ForRunePosition() int {
	return nc.runePosition
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

	return &NodeChain{nodes: found, runePosition: runePosition}, nil
}

func runePositionToAstNodeChain(node *dslNode, runePosition int) ([]DSLNode, error) {
	if !node.Has(dslshape.NodePredicateStartRune) {
		return nil, nil
	}

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
			return append(childChain, wrapper{node}), nil
		}
	}

	return []DSLNode{wrapper{node}}, nil
}

type wrapper struct {
	node *dslNode
}

func (w wrapper) GetType() dslshape.NodeType {
	return w.node.GetType()
}

func (w wrapper) GetString(predicateName string) (string, error) {
	return w.node.GetString(predicateName)
}

func (w wrapper) GetInt(predicateName string) (int, error) {
	return w.node.GetInt(predicateName)
}

func (w wrapper) Lookup(predicateName string) (DSLNode, error) {
	found, err := w.node.Lookup(predicateName)
	if err != nil {
		return nil, err
	}

	return wrapper{found}, nil
}
