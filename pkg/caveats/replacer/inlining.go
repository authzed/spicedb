// Modified version of https://github.com/authzed/cel-go/blob/b707d132d96bb5450df92d138860126bf03f805f/cel/inlining.go
// which changes it so variable replacement is always done without cel.bind calls. See
// the "CHANGED" below.
//
// Original Copyright notice:
// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package replacer

import (
	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/ast"
	"github.com/authzed/cel-go/common/containers"
	"github.com/authzed/cel-go/common/operators"
	"github.com/authzed/cel-go/common/overloads"
	"github.com/authzed/cel-go/common/types"
	"github.com/authzed/cel-go/common/types/traits"
)

// InlineVariable holds a variable name to be matched and an AST representing
// the expression graph which should be used to replace it.
type InlineVariable struct {
	name  string
	alias string
	def   *ast.AST
}

// Name returns the qualified variable or field selection to replace.
func (v *InlineVariable) Name() string {
	return v.name
}

// Alias returns the alias to use when performing cel.bind() calls during inlining.
func (v *InlineVariable) Alias() string {
	return v.alias
}

// Expr returns the inlined expression value.
func (v *InlineVariable) Expr() ast.Expr {
	return v.def.Expr()
}

// Type indicates the inlined expression type.
func (v *InlineVariable) Type() *cel.Type {
	return v.def.GetType(v.def.Expr().ID())
}

// newInlineVariable declares a variable name to be replaced by a checked expression.
func newInlineVariable(name string, definition *cel.Ast) *InlineVariable {
	return newInlineVariableWithAlias(name, name, definition)
}

// newInlineVariableWithAlias declares a variable name to be replaced by a checked expression.
// If the variable occurs more than once, the provided alias will be used to replace the expressions
// where the variable name occurs.
func newInlineVariableWithAlias(name, alias string, definition *cel.Ast) *InlineVariable {
	return &InlineVariable{name: name, alias: alias, def: definition.NativeRep()}
}

// newModifiedInliningOptimizer creates and optimizer which replaces variables with expression definitions.
func newModifiedInliningOptimizer(inlineVars ...*InlineVariable) cel.ASTOptimizer {
	return &inliningOptimizer{variables: inlineVars}
}

type inliningOptimizer struct {
	variables []*InlineVariable
}

func (opt *inliningOptimizer) Optimize(ctx *cel.OptimizerContext, a *ast.AST) *ast.AST {
	root := ast.NavigateAST(a)
	for _, inlineVar := range opt.variables {
		matches := ast.MatchDescendants(root, opt.matchVariable(inlineVar.Name()))
		// Skip cases where the variable isn't in the expression graph
		if len(matches) == 0 {
			continue
		}

		// CHANGED: *ALWAYS* do a direct replacement of the expression sub-graph.
		for _, match := range matches {
			// Copy the inlined AST expr and source info.
			copyExpr := ctx.CopyASTAndMetadata(inlineVar.def)
			opt.inlineExpr(ctx, match, copyExpr, inlineVar.Type())
		}
	}
	return a
}

// inlineExpr replaces the current expression with the inlined one, unless the location of the inlining
// happens within a presence test, e.g. has(a.b.c) -> inline alpha for a.b.c in which case an attempt is
// made to determine whether the inlined value can be presence or existence tested.
func (opt *inliningOptimizer) inlineExpr(ctx *cel.OptimizerContext, prev ast.NavigableExpr, inlined ast.Expr, inlinedType *cel.Type) {
	switch prev.Kind() {
	case ast.SelectKind:
		sel := prev.AsSelect()
		if !sel.IsTestOnly() {
			ctx.UpdateExpr(prev, inlined)
			return
		}
		opt.rewritePresenceExpr(ctx, prev, inlined, inlinedType)
	default:
		ctx.UpdateExpr(prev, inlined)
	}
}

// rewritePresenceExpr converts the inlined expression, when it occurs within a has() macro, to type-safe
// expression appropriate for the inlined type, if possible.
//
// If the rewrite is not possible an error is reported at the inline expression site.
func (opt *inliningOptimizer) rewritePresenceExpr(ctx *cel.OptimizerContext, prev, inlined ast.Expr, inlinedType *cel.Type) {
	// If the input inlined expression is not a select expression it won't work with the has()
	// macro. Attempt to rewrite the presence test in terms of the typed input, otherwise error.
	if inlined.Kind() == ast.SelectKind {
		presenceTest, hasMacro := ctx.NewHasMacro(prev.ID(), inlined)
		ctx.UpdateExpr(prev, presenceTest)
		ctx.SetMacroCall(prev.ID(), hasMacro)
		return
	}

	ctx.ClearMacroCall(prev.ID())
	if inlinedType.IsAssignableType(cel.NullType) {
		ctx.UpdateExpr(prev,
			ctx.NewCall(operators.NotEquals,
				inlined,
				ctx.NewLiteral(types.NullValue),
			))
		return
	}
	if inlinedType.HasTrait(traits.SizerType) {
		ctx.UpdateExpr(prev,
			ctx.NewCall(operators.NotEquals,
				ctx.NewMemberCall(overloads.Size, inlined),
				ctx.NewLiteral(types.IntZero),
			))
		return
	}
	ctx.ReportErrorAtID(prev.ID(), "unable to inline expression type %v into presence test", inlinedType)
}

// matchVariable matches simple identifiers, select expressions, and presence test expressions
// which match the (potentially) qualified variable name provided as input.
//
// Note, this function does not support inlining against select expressions which includes optional
// field selection. This may be a future refinement.
func (opt *inliningOptimizer) matchVariable(varName string) ast.ExprMatcher {
	return func(e ast.NavigableExpr) bool {
		if e.Kind() == ast.IdentKind && e.AsIdent() == varName {
			return true
		}
		if e.Kind() == ast.SelectKind {
			sel := e.AsSelect()
			// While the `ToQualifiedName` call could take the select directly, this
			// would skip presence tests from possible matches, which we would like
			// to include.
			qualName, found := containers.ToQualifiedName(sel.Operand())
			return found && qualName+"."+sel.FieldName() == varName
		}
		return false
	}
}
