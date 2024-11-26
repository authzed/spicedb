package caveats

import (
	"fmt"
	"strings"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/pkg/caveats/replacer"
	"github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	impl "github.com/authzed/spicedb/pkg/proto/impl/v1"
)

const anonymousCaveat = ""

const maxCaveatExpressionSize = 100_000 // characters

// CompiledCaveat is a compiled form of a caveat.
type CompiledCaveat struct {
	// env is the environment under which the CEL program was compiled.
	celEnv *cel.Env

	// ast is the AST form of the CEL program.
	ast *cel.Ast

	// name of the caveat
	name string
}

// Name represents a user-friendly reference to a caveat
func (cc CompiledCaveat) Name() string {
	return cc.name
}

// ExprString returns the string-form of the caveat.
func (cc CompiledCaveat) ExprString() (string, error) {
	return cel.AstToString(cc.ast)
}

// RewriteVariable replaces the use of a variable with another variable in the compiled caveat.
func (cc CompiledCaveat) RewriteVariable(oldName, newName string) (CompiledCaveat, error) {
	// Find the existing parameter name and get its type.
	oldExpr, issues := cc.celEnv.Compile(oldName)
	if issues.Err() != nil {
		return CompiledCaveat{}, fmt.Errorf("failed to parse old variable name: %w", issues.Err())
	}

	oldType := oldExpr.OutputType()

	// Ensure the new variable name is not used.
	_, niss := cc.celEnv.Compile(newName)
	if niss.Err() == nil {
		return CompiledCaveat{}, fmt.Errorf("variable name '%s' is already used", newName)
	}

	// Extend the environment with the new variable name.
	extended, err := cc.celEnv.Extend(cel.Variable(newName, oldType))
	if err != nil {
		return CompiledCaveat{}, fmt.Errorf("failed to extend environment: %w", err)
	}

	// Replace the variable in the AST.
	updatedAst, err := replacer.ReplaceVariable(extended, cc.ast, oldName, newName)
	if err != nil {
		return CompiledCaveat{}, fmt.Errorf("failed to rewrite variable: %w", err)
	}

	return CompiledCaveat{extended, updatedAst, cc.name}, nil
}

// Serialize serializes the compiled caveat into a byte string for storage.
func (cc CompiledCaveat) Serialize() ([]byte, error) {
	cexpr, err := cel.AstToCheckedExpr(cc.ast)
	if err != nil {
		return nil, err
	}

	caveat := &impl.DecodedCaveat{
		KindOneof: &impl.DecodedCaveat_Cel{
			Cel: cexpr,
		},
		Name: cc.name,
	}

	// TODO(jschorr): change back to MarshalVT once stable is supported.
	// See: https://github.com/planetscale/vtprotobuf/pull/133
	return proto.MarshalOptions{Deterministic: true}.Marshal(caveat)
}

// ReferencedParameters returns the names of the parameters referenced in the expression.
func (cc CompiledCaveat) ReferencedParameters(parameters []string) (*mapz.Set[string], error) {
	referencedParams := mapz.NewSet[string]()
	definedParameters := mapz.NewSet[string]()
	definedParameters.Extend(parameters)

	checked, err := cel.AstToCheckedExpr(cc.ast)
	if err != nil {
		return nil, err
	}

	referencedParameters(definedParameters, checked.Expr, referencedParams)
	return referencedParams, nil
}

// CompileCaveatWithName compiles a caveat string into a compiled caveat with a given name,
// or returns the compilation errors.
func CompileCaveatWithName(env *Environment, exprString, name string) (*CompiledCaveat, error) {
	c, err := CompileCaveatWithSource(env, name, common.NewStringSource(exprString, name), nil)
	if err != nil {
		return nil, err
	}
	c.name = name
	return c, nil
}

// CompileCaveatWithSource compiles a caveat source into a compiled caveat, or returns the compilation errors.
func CompileCaveatWithSource(env *Environment, name string, source common.Source, startPosition SourcePosition) (*CompiledCaveat, error) {
	celEnv, err := env.asCelEnvironment()
	if err != nil {
		return nil, err
	}

	if len(strings.TrimSpace(source.Content())) > maxCaveatExpressionSize {
		return nil, fmt.Errorf("caveat expression provided exceeds maximum allowed size of %d characters", maxCaveatExpressionSize)
	}

	ast, issues := celEnv.CompileSource(source)
	if issues != nil && issues.Err() != nil {
		if startPosition == nil {
			return nil, MultipleCompilationError{issues.Err(), issues}
		}

		// Construct errors with the source location adjusted based on the starting source position
		// in the parent schema (if any). This ensures that the errors coming out of CEL show the correct
		// *overall* location information..
		line, col, err := startPosition.LineAndColumn()
		if err != nil {
			return nil, err
		}

		adjustedErrors := common.NewErrors(source)
		for _, existingErr := range issues.Errors() {
			location := existingErr.Location

			// NOTE: Our locations are zero-indexed while CEL is 1-indexed, so we need to adjust the line/column values accordingly.
			if location.Line() == 1 {
				location = common.NewLocation(line+location.Line(), col+location.Column())
			} else {
				location = common.NewLocation(line+location.Line(), location.Column())
			}

			adjustedError := &common.Error{
				Message:  existingErr.Message,
				ExprID:   existingErr.ExprID,
				Location: location,
			}

			adjustedErrors = adjustedErrors.Append([]*common.Error{
				adjustedError,
			})
		}

		adjustedIssues := cel.NewIssues(adjustedErrors)
		return nil, MultipleCompilationError{adjustedIssues.Err(), adjustedIssues}
	}

	if ast.OutputType() != cel.BoolType {
		return nil, MultipleCompilationError{fmt.Errorf("caveat expression must result in a boolean value: found `%s`", ast.OutputType().String()), nil}
	}

	compiled := &CompiledCaveat{celEnv, ast, anonymousCaveat}
	compiled.name = name
	return compiled, nil
}

// compileCaveat compiles a caveat string into a compiled caveat, or returns the compilation errors.
func compileCaveat(env *Environment, exprString string) (*CompiledCaveat, error) {
	s := common.NewStringSource(exprString, "caveat")
	return CompileCaveatWithSource(env, "caveat", s, nil)
}

// DeserializeCaveat deserializes a byte-serialized caveat back into a CompiledCaveat.
func DeserializeCaveat(serialized []byte, parameterTypes map[string]types.VariableType) (*CompiledCaveat, error) {
	if len(serialized) == 0 {
		return nil, fmt.Errorf("given empty serialized")
	}

	caveat := &impl.DecodedCaveat{}
	err := caveat.UnmarshalVT(serialized)
	if err != nil {
		return nil, err
	}

	env, err := EnvForVariables(parameterTypes)
	if err != nil {
		return nil, err
	}

	celEnv, err := env.asCelEnvironment()
	if err != nil {
		return nil, err
	}

	ast := cel.CheckedExprToAst(caveat.GetCel())
	return &CompiledCaveat{celEnv, ast, caveat.Name}, nil
}
