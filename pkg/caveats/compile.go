package caveats

import (
	"fmt"

	"github.com/authzed/spicedb/internal/util"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common"

	impl "github.com/authzed/spicedb/pkg/proto/impl/v1"
)

const anonymousCaveat = ""

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

	return caveat.MarshalVT()
}

// ReferencedParameters returns the names of the parameters referenced in the expression.
func (cc CompiledCaveat) ReferencedParameters(parameters []string) *util.Set[string] {
	referencedParams := util.NewSet[string]()
	definedParameters := util.NewSet[string]()
	definedParameters.Extend(parameters)

	referencedParameters(definedParameters, cc.ast.Expr(), referencedParams)
	return referencedParams
}

// CompileCaveatWithName compiles a caveat string into a compiled caveat with a given name,
// or returns the compilation errors.
func CompileCaveatWithName(env *Environment, exprString, name string) (*CompiledCaveat, error) {
	c, err := CompileCaveatWithSource(env, name, common.NewStringSource(exprString, name))
	if err != nil {
		return nil, err
	}
	c.name = name
	return c, nil
}

// CompileCaveat compiles a caveat string into a compiled caveat, or returns the compilation errors.
func CompileCaveat(env *Environment, exprString string) (*CompiledCaveat, error) {
	s := common.NewStringSource(exprString, "caveat")
	return CompileCaveatWithSource(env, "caveat", s)
}

// CompileCaveatWithSource compiles a caveat source into a compiled caveat, or returns the compilation errors.
func CompileCaveatWithSource(env *Environment, name string, source common.Source) (*CompiledCaveat, error) {
	celEnv, err := env.asCelEnvironment()
	if err != nil {
		return nil, err
	}

	ast, issues := celEnv.CompileSource(source)
	if issues != nil && issues.Err() != nil {
		return nil, CompilationErrors{issues.Err(), issues}
	}

	if ast.OutputType() != cel.BoolType {
		return nil, CompilationErrors{fmt.Errorf("caveat expression must result in a boolean value: found `%s`", ast.OutputType().String()), nil}
	}

	compiled := &CompiledCaveat{celEnv, ast, anonymousCaveat}
	compiled.name = name
	return compiled, nil
}

// DeserializeCaveat deserializes a byte-serialized caveat back into a CompiledCaveat.
func DeserializeCaveat(serialized []byte) (*CompiledCaveat, error) {
	celEnv, err := NewEnvironment().asCelEnvironment()
	if err != nil {
		return nil, err
	}

	caveat := &impl.DecodedCaveat{}
	err = caveat.UnmarshalVT(serialized)
	if err != nil {
		return nil, err
	}

	ast := cel.CheckedExprToAst(caveat.GetCel())
	return &CompiledCaveat{celEnv, ast, caveat.Name}, nil
}
