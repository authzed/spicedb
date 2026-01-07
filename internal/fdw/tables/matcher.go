package tables

// Pattern Matching System for pg_query AST Nodes
//
// This package provides a declarative pattern matching system for parsing and validating
// SQL queries represented as pg_query.Node AST structures. It replaces imperative nested
// if/switch statements with composable patterns.
//
// # Overview
//
// The pattern matching system provides two main APIs:
//
// 1. PatternMatcher - High-level API for common SQL parsing tasks
// 2. Pattern interface - Low-level composable patterns for custom matching
//
// # Using PatternMatcher (Recommended)
//
// For most SQL parsing needs, use the PatternMatcher high-level API:
//
//	pm := NewPatternMatcher()
//
//	// Match a WHERE clause and extract all field conditions
//	fields, err := pm.MatchWhereClause(whereNode)
//	// Returns: map[string]valueOrRef{
//	//     "resource_type": {value: "document"},
//	//     "resource_id": {parameterIndex: 1},
//	// }
//
//	// Match a single value or reference expression
//	valOrRef, err := pm.MatchValueOrRef(exprNode)
//	// Returns: valueOrRef{value: "document"} or
//	//          valueOrRef{parameterIndex: 1} or
//	//          valueOrRef{fieldName: "column_name"} or
//	//          valueOrRef{isSubQueryPlaceholder: true}
//
// # Available Patterns
//
// The system includes these built-in pattern types:
//
// - equalityPattern: Matches "column = value" or "value = column" expressions
// - columnRefPattern: Matches column references and captures column names
// - literalPattern: Matches string literal constants
// - paramRefPattern: Matches parameter references like $1, $2
// - subQueryPlaceholderPattern: Matches ((SELECT null::text)::text) placeholders
//
// # Pattern Results
//
// Patterns return a MatchResult containing extracted values:
//
//	type MatchResult struct {
//	    Values map[string]any // Maps capture names to extracted values
//	}

import (
	"errors"
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Pattern represents a declarative pattern for matching pg_query nodes.
// It defines what structure we expect and how to extract values from it.
type Pattern interface {
	// Match attempts to match the node against this pattern.
	// Returns the extracted values and whether the match succeeded.
	Match(node *pg_query.Node) (MatchResult, bool)
}

// MatchResult contains values extracted from a successful pattern match.
type MatchResult struct {
	// Values maps capture names to their extracted values
	Values map[string]any
}

// columnRefPattern matches a column reference and captures its name
type columnRefPattern struct {
	CaptureName string // Name to capture the column name as
}

func (c columnRefPattern) Match(node *pg_query.Node) (MatchResult, bool) {
	colRef := node.GetColumnRef()
	if colRef == nil {
		return MatchResult{}, false
	}

	if len(colRef.Fields) != 1 {
		return MatchResult{}, false
	}

	str := colRef.Fields[0].GetString_()
	if str == nil {
		return MatchResult{}, false
	}

	result := MatchResult{Values: make(map[string]any)}
	if c.CaptureName != "" {
		result.Values[c.CaptureName] = str.Sval
	}

	return result, true
}

// literalPattern matches a literal constant value
type literalPattern struct {
	CaptureName string // Name to capture the literal value as
}

func (l literalPattern) Match(node *pg_query.Node) (MatchResult, bool) {
	aConst := node.GetAConst()
	if aConst == nil {
		return MatchResult{}, false
	}

	result := MatchResult{Values: make(map[string]any)}
	if l.CaptureName != "" {
		result.Values[l.CaptureName] = aConst.GetSval().Sval
	}

	return result, true
}

// paramRefPattern matches a parameter reference like $1, $2, etc.
type paramRefPattern struct {
	CaptureName string // Name to capture the parameter index as
}

func (p paramRefPattern) Match(node *pg_query.Node) (MatchResult, bool) {
	paramRef := node.GetParamRef()
	if paramRef == nil {
		return MatchResult{}, false
	}

	result := MatchResult{Values: make(map[string]any)}
	if p.CaptureName != "" {
		result.Values[p.CaptureName] = paramRef.Number
	}

	return result, true
}

// Example usage patterns:

// equalityPattern matches "column = value" or "value = column" or "column = $1"
type equalityPattern struct{}

func (e equalityPattern) Match(node *pg_query.Node) (MatchResult, bool) {
	aExpr := node.GetAExpr()
	if aExpr == nil {
		return MatchResult{}, false
	}

	// Check for = operator
	if len(aExpr.Name) != 1 || aExpr.Name[0].GetString_().Sval != "=" {
		return MatchResult{}, false
	}

	pm := NewPatternMatcher()
	leftRef, leftErr := pm.MatchValueOrRef(aExpr.Lexpr)
	if leftErr != nil {
		return MatchResult{}, false
	}

	rightRef, rightErr := pm.MatchValueOrRef(aExpr.Rexpr)
	if rightErr != nil {
		return MatchResult{}, false
	}

	// Determine which is the column and which is the value
	var fieldName string
	var fieldValue valueOrRef

	if leftRef.fieldName != "" && rightRef.fieldName != "" {
		return MatchResult{}, false // Both sides are columns
	}

	switch {
	case leftRef.fieldName != "":
		fieldName = leftRef.fieldName
		fieldValue = rightRef
	case rightRef.fieldName != "":
		fieldName = rightRef.fieldName
		fieldValue = leftRef
	default:
		return MatchResult{}, false // Neither side is a column
	}

	return MatchResult{
		Values: map[string]any{
			"field_name":  fieldName,
			"field_value": fieldValue,
		},
	}, true
}

// subQueryPlaceholderPattern matches ((SELECT null::text)::text)
type subQueryPlaceholderPattern struct{}

func (s subQueryPlaceholderPattern) Match(node *pg_query.Node) (MatchResult, bool) {
	typeCast := node.GetTypeCast()
	if typeCast == nil {
		return MatchResult{}, false
	}

	// Check outer type cast is text
	if typeCast.GetTypeName() == nil {
		return MatchResult{}, false
	}

	if len(typeCast.GetTypeName().GetNames()) != 1 {
		return MatchResult{}, false
	}

	if typeCast.GetTypeName().GetNames()[0].GetString_() == nil {
		return MatchResult{}, false
	}

	if typeCast.GetTypeName().GetNames()[0].GetString_().Sval != "text" {
		return MatchResult{}, false
	}

	// Check for sublink
	if typeCast.GetArg() == nil {
		return MatchResult{}, false
	}

	sublink := typeCast.GetArg().GetSubLink()
	if sublink == nil {
		return MatchResult{}, false
	}

	if sublink.GetSubLinkType() != pg_query.SubLinkType_EXPR_SUBLINK {
		return MatchResult{}, false
	}

	if sublink.GetSubselect() == nil || sublink.GetSubselect().GetSelectStmt() == nil {
		return MatchResult{}, false
	}

	targetList := sublink.GetSubselect().GetSelectStmt().GetTargetList()
	if len(targetList) != 1 {
		return MatchResult{}, false
	}

	targetListTypeCast := targetList[0].GetResTarget().GetVal().GetTypeCast()
	if targetListTypeCast == nil {
		return MatchResult{}, false
	}

	// Check inner type cast
	if targetListTypeCast.GetTypeName() == nil {
		return MatchResult{}, false
	}

	if len(targetListTypeCast.GetTypeName().GetNames()) != 1 {
		return MatchResult{}, false
	}

	if targetListTypeCast.GetTypeName().GetNames()[0].GetString_() == nil {
		return MatchResult{}, false
	}

	if targetListTypeCast.GetTypeName().GetNames()[0].GetString_().Sval != "text" {
		return MatchResult{}, false
	}

	// Check for null constant
	if targetListTypeCast.GetArg() == nil || targetListTypeCast.GetArg().GetAConst() == nil {
		return MatchResult{}, false
	}

	if !targetListTypeCast.GetArg().GetAConst().Isnull {
		return MatchResult{}, false
	}

	return MatchResult{
		Values: map[string]any{
			"is_placeholder": true,
		},
	}, true
}

// PatternMatcher is a high-level API for common SQL pattern matching
type PatternMatcher struct{}

// NewPatternMatcher creates a new pattern matcher
func NewPatternMatcher() *PatternMatcher {
	return &PatternMatcher{}
}

// MatchWhereClause matches a WHERE clause and extracts field conditions
func (pm *PatternMatcher) MatchWhereClause(node *pg_query.Node) (map[string]valueOrRef, error) {
	if node == nil {
		return make(map[string]valueOrRef), nil
	}

	fields := make(map[string]valueOrRef)

	// Try to match as AND expression
	if boolExpr := node.GetBoolExpr(); boolExpr != nil {
		if boolExpr.Boolop != pg_query.BoolExprType_AND_EXPR {
			return nil, errors.New("only AND supported")
		}

		for _, arg := range boolExpr.Args {
			if err := pm.matchSingleCondition(arg, fields); err != nil {
				return nil, err
			}
		}

		return fields, nil
	}

	// Try to match as single condition
	if err := pm.matchSingleCondition(node, fields); err != nil {
		return nil, err
	}

	return fields, nil
}

// matchSingleCondition matches a single WHERE condition (e.g., "column = value")
func (pm *PatternMatcher) matchSingleCondition(node *pg_query.Node, fields map[string]valueOrRef) error {
	// Check if this is an AExpr (binary operation)
	aExpr := node.GetAExpr()
	if aExpr != nil {
		// Check operator
		if len(aExpr.Name) != 1 || aExpr.Name[0].GetString_().Sval != "=" {
			return errors.New("only = supported")
		}

		// Try to parse both sides
		leftRef, leftErr := pm.MatchValueOrRef(aExpr.Lexpr)
		rightRef, rightErr := pm.MatchValueOrRef(aExpr.Rexpr)

		// If either side failed to parse, return that error
		if leftErr != nil {
			return leftErr
		}
		if rightErr != nil {
			return rightErr
		}

		// Check for both sides being columns
		if leftRef.fieldName != "" && rightRef.fieldName != "" {
			return errors.New("only one side of the expression can be a column")
		}

		// Check for neither side being a column
		if leftRef.fieldName == "" && rightRef.fieldName == "" {
			return errors.New("missing column name")
		}
	}

	// Try equality pattern
	eqPattern := equalityPattern{}
	if result, ok := eqPattern.Match(node); ok {
		fieldName, ok := result.Values["field_name"].(string)
		if !ok {
			return errors.New("equality pattern did not capture field_name as string")
		}
		fieldValue, ok := result.Values["field_value"].(valueOrRef)
		if !ok {
			return errors.New("equality pattern did not capture field_value as valueOrRef")
		}

		if _, exists := fields[fieldName]; exists {
			return fmt.Errorf("field %s already set", fieldName)
		}

		fields[fieldName] = fieldValue
		return nil
	}

	return fmt.Errorf("operation not supported: %v", node)
}

// MatchValueOrRef matches a value or reference expression
func (pm *PatternMatcher) MatchValueOrRef(node *pg_query.Node) (valueOrRef, error) {
	// Try literal
	litPattern := literalPattern{CaptureName: "value"}
	if result, ok := litPattern.Match(node); ok {
		value, ok := result.Values["value"].(string)
		if !ok {
			return valueOrRef{}, errors.New("literal pattern did not capture string value")
		}
		return valueOrRef{value: value}, nil
	}

	// Try parameter
	paramPattern := paramRefPattern{CaptureName: "param"}
	if result, ok := paramPattern.Match(node); ok {
		paramIndex, ok := result.Values["param"].(int32)
		if !ok {
			return valueOrRef{}, errors.New("parameter pattern did not capture int32 value")
		}
		return valueOrRef{parameterIndex: paramIndex}, nil
	}

	// Try column reference
	colPattern := columnRefPattern{CaptureName: "column"}
	if result, ok := colPattern.Match(node); ok {
		fieldName, ok := result.Values["column"].(string)
		if !ok {
			return valueOrRef{}, errors.New("column pattern did not capture string value")
		}
		return valueOrRef{fieldName: fieldName}, nil
	}

	// Try subquery placeholder
	subqPattern := subQueryPlaceholderPattern{}
	if result, ok := subqPattern.Match(node); ok {
		isPlaceholder, ok := result.Values["is_placeholder"].(bool)
		if !ok {
			return valueOrRef{}, errors.New("subquery placeholder pattern did not capture bool value")
		}
		if isPlaceholder {
			return valueOrRef{isSubQueryPlaceholder: true}, nil
		}
	}

	return valueOrRef{}, errors.New("unsupported expression type")
}
