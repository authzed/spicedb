package caveats

import (
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common"

	impl "github.com/authzed/spicedb/pkg/proto/impl/v1"
)

// CompiledCaveat is a compiled form of a caveat.
type CompiledCaveat struct {
	// env is the environment under which the CEL program was compiled.
	celEnv *cel.Env

	// ast is the AST form of the CEL program.
	ast *cel.Ast
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
	}

	return caveat.MarshalVT()
}

// CompilationErrors is a wrapping error for containing compilation errors for a Caveat.
type CompilationErrors struct {
	error

	issues *cel.Issues
}

// CompileCaveat compiles a caveat string into a compiled caveat, or returns the compilation errors.
func CompileCaveat(env *Environment, exprString string) (*CompiledCaveat, error) {
	celEnv, err := env.asCelEnvironment()
	if err != nil {
		return nil, err
	}

	s := common.NewStringSource(exprString, "caveat")
	ast, issues := celEnv.CompileSource(s)
	if issues != nil && issues.Err() != nil {
		return nil, CompilationErrors{issues.Err(), issues}
	}

	if ast.OutputType() != cel.BoolType {
		return nil, CompilationErrors{fmt.Errorf("caveat expression must result in a boolean value: found `%s`", ast.OutputType().String()), nil}
	}

	return &CompiledCaveat{celEnv, ast}, nil
}

// DeserializeCaveat deserializes a byte-serialized caveat back into a CompiledCaveat.
func DeserializeCaveat(env *Environment, serialized []byte) (*CompiledCaveat, error) {
	celEnv, err := env.asCelEnvironment()
	if err != nil {
		return nil, err
	}

	caveat := &impl.DecodedCaveat{}
	err = caveat.UnmarshalVT(serialized)
	if err != nil {
		return nil, err
	}

	ast := cel.CheckedExprToAst(caveat.GetCel())
	return &CompiledCaveat{celEnv, ast}, nil
}
