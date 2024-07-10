package replacer

import (
	"fmt"

	"github.com/authzed/cel-go/cel"
)

func ReplaceVariable(e *cel.Env, existingAst *cel.Ast, oldVarName string, newVarName string) (*cel.Ast, error) {
	newExpr, iss := e.Compile(newVarName)
	if iss.Err() != nil {
		return nil, fmt.Errorf("failed to compile new variable name: %w", iss.Err())
	}

	inlinedVars := []*InlineVariable{}
	inlinedVars = append(inlinedVars, newInlineVariable(oldVarName, newExpr))

	opt := cel.NewStaticOptimizer(newModifiedInliningOptimizer(inlinedVars...))
	optimized, iss := opt.Optimize(e, existingAst)
	if iss.Err() != nil {
		return nil, fmt.Errorf("failed to optimize: %w", iss.Err())
	}

	return optimized, nil
}
