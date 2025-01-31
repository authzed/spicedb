package compiler

import (
	"container/list"
	"fmt"

	"github.com/authzed/spicedb/pkg/composableschemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/composableschemadsl/input"
	"github.com/authzed/spicedb/pkg/composableschemadsl/parser"
)

type dslNode struct {
	nodeType   dslshape.NodeType
	properties map[string]interface{}
	children   map[string]*list.List
}

func createAstNode(_ input.Source, kind dslshape.NodeType) parser.AstNode {
	return &dslNode{
		nodeType:   kind,
		properties: make(map[string]interface{}),
		children:   make(map[string]*list.List),
	}
}

func (tn *dslNode) GetType() dslshape.NodeType {
	return tn.nodeType
}

func (tn *dslNode) Connect(predicate string, other parser.AstNode) {
	if tn.children[predicate] == nil {
		tn.children[predicate] = list.New()
	}

	tn.children[predicate].PushBack(other)
}

// Used to preserve import order when doing import operations on AST
func (tn *dslNode) ConnectAndHoistMany(predicate string, other *list.List) {
	if tn.children[predicate] == nil {
		tn.children[predicate] = list.New()
	}

	tn.children[predicate].PushFrontList(other)
}

func (tn *dslNode) MustDecorate(property string, value string) parser.AstNode {
	if _, ok := tn.properties[property]; ok {
		panic(fmt.Sprintf("Existing key for property %s\n\tNode: %v", property, tn.properties))
	}

	tn.properties[property] = value
	return tn
}

func (tn *dslNode) MustDecorateWithInt(property string, value int) parser.AstNode {
	if _, ok := tn.properties[property]; ok {
		panic(fmt.Sprintf("Existing key for property %s\n\tNode: %v", property, tn.properties))
	}

	tn.properties[property] = value
	return tn
}

func (tn *dslNode) Range(mapper input.PositionMapper) (input.SourceRange, error) {
	sourceStr, err := tn.GetString(dslshape.NodePredicateSource)
	if err != nil {
		return nil, err
	}

	source := input.Source(sourceStr)

	startRune, err := tn.GetInt(dslshape.NodePredicateStartRune)
	if err != nil {
		return nil, err
	}

	endRune, err := tn.GetInt(dslshape.NodePredicateEndRune)
	if err != nil {
		return nil, err
	}

	return source.RangeForRunePositions(startRune, endRune, mapper), nil
}

func (tn *dslNode) Has(predicateName string) bool {
	_, ok := tn.properties[predicateName]
	return ok
}

func (tn *dslNode) GetInt(predicateName string) (int, error) {
	predicate, ok := tn.properties[predicateName]
	if !ok {
		return 0, fmt.Errorf("unknown predicate %s", predicateName)
	}

	value, ok := predicate.(int)
	if !ok {
		return 0, fmt.Errorf("predicate %s is not an int", predicateName)
	}

	return value, nil
}

func (tn *dslNode) GetString(predicateName string) (string, error) {
	predicate, ok := tn.properties[predicateName]
	if !ok {
		return "", fmt.Errorf("unknown predicate %s", predicateName)
	}

	value, ok := predicate.(string)
	if !ok {
		return "", fmt.Errorf("predicate %s is not a string", predicateName)
	}

	return value, nil
}

func (tn *dslNode) AllSubNodes() []*dslNode {
	nodes := []*dslNode{}
	for _, childList := range tn.children {
		for e := childList.Front(); e != nil; e = e.Next() {
			nodes = append(nodes, e.Value.(*dslNode))
		}
	}
	return nodes
}

func (tn *dslNode) GetChildren() []*dslNode {
	return tn.List(dslshape.NodePredicateChild)
}

func (tn *dslNode) FindAll(nodeType dslshape.NodeType) []*dslNode {
	found := []*dslNode{}
	if tn.nodeType == dslshape.NodeTypeError {
		found = append(found, tn)
	}

	for _, childList := range tn.children {
		for e := childList.Front(); e != nil; e = e.Next() {
			childFound := e.Value.(*dslNode).FindAll(nodeType)
			found = append(found, childFound...)
		}
	}
	return found
}

func (tn *dslNode) List(predicateName string) []*dslNode {
	children := []*dslNode{}
	childList, ok := tn.children[predicateName]
	if !ok {
		return children
	}

	for e := childList.Front(); e != nil; e = e.Next() {
		children = append(children, e.Value.(*dslNode))
	}

	return children
}

func (tn *dslNode) Lookup(predicateName string) (*dslNode, error) {
	childList, ok := tn.children[predicateName]
	if !ok {
		return nil, fmt.Errorf("unknown predicate %s", predicateName)
	}

	for e := childList.Front(); e != nil; e = e.Next() {
		return e.Value.(*dslNode), nil
	}

	return nil, fmt.Errorf("nothing in predicate %s", predicateName)
}

func (tn *dslNode) Errorf(message string, args ...interface{}) error {
	return withNodeError{
		error:           fmt.Errorf(message, args...),
		errorSourceCode: "",
		node:            tn,
	}
}

func (tn *dslNode) WithSourceErrorf(sourceCode string, message string, args ...interface{}) error {
	return withNodeError{
		error:           fmt.Errorf(message, args...),
		errorSourceCode: sourceCode,
		node:            tn,
	}
}
