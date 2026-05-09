// Copyright 2023-2025 Buf Technologies, Inc.
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

package protovalidate

import (
	"fmt"
	"slices"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/google/cel-go/cel"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// astSet represents a collection of compiledAST and their associated cel.Env.
type astSet []compiledAST

// Merge combines a set with another, producing a new ASTSet.
func (set astSet) Merge(other astSet) astSet {
	out := make([]compiledAST, 0, len(set)+len(other))
	out = append(out, set...)
	out = append(out, other...)
	return out
}

// ReduceResiduals generates a ProgramSet, performing a partial evaluation of
// the ASTSet to optimize the expression. If the expression is optimized to
// either a true or empty string constant result, no compiledProgram is
// generated for it. The main usage of this is to elide tautological expressions
// from the final result.
func (set astSet) ReduceResiduals(rules protoreflect.Message, opts ...cel.ProgramOption) (programSet, error) {
	residuals := make(astSet, 0, len(set))
	options := append([]cel.ProgramOption{
		cel.EvalOptions(
			cel.OptTrackState,
			cel.OptExhaustiveEval,
			cel.OptOptimize,
			cel.OptPartialEval,
		),
	}, opts...)

	activation := getBindings()
	defer putBindings(activation)
	activation.Rules = rules.Interface()

	for _, ast := range set {
		activation.Rule = ast.Value.Interface()
		program, err := ast.toProgram(ast.Env, options...)
		if err != nil {
			residuals = append(residuals, ast)
			continue
		}
		val, details, _ := program.Program.Eval(activation)
		if val != nil {
			switch value := val.Value().(type) {
			case bool:
				if value {
					continue
				}
			case string:
				if value == "" {
					continue
				}
			}
		}
		residual, err := ast.Env.ResidualAst(ast.AST, details)
		if err != nil {
			residuals = append(residuals, ast)
		} else {
			residuals = append(residuals, compiledAST{
				AST:        residual,
				Env:        ast.Env,
				Rules:      ast.Rules,
				Source:     ast.Source,
				Path:       ast.Path,
				Value:      ast.Value,
				Descriptor: ast.Descriptor,
			})
		}
	}

	return residuals.ToProgramSet(opts...)
}

// ToProgramSet generates a ProgramSet from the specified ASTs.
func (set astSet) ToProgramSet(opts ...cel.ProgramOption) (out programSet, err error) {
	if len(set) == 0 {
		return out, nil
	}
	out.env = set[0].Env
	programs := make([]compiledProgram, len(set))
	for i, ast := range set {
		programs[i], err = ast.toProgram(ast.Env, opts...)
		if err != nil {
			return out, err
		}
	}
	out.programs = programs
	return out, nil
}

// SetRuleValue sets the rule and rules value for the programs in the ASTSet.
func (set astSet) WithRuleValues(
	rules protoreflect.Message,
	ruleValue protoreflect.Value,
	ruleDescriptor protoreflect.FieldDescriptor,
) (out astSet, err error) {
	out = slices.Clone(set)
	for i := range set {
		out[i].Rules = rules
		out[i].Value = ruleValue
		out[i].Descriptor = ruleDescriptor
	}
	return out, nil
}

type compiledAST struct {
	AST        *cel.Ast
	Env        *cel.Env
	Rules      protoreflect.Message
	Source     *validate.Rule
	Path       []*validate.FieldPathElement
	Value      protoreflect.Value
	Descriptor protoreflect.FieldDescriptor
}

func (ast compiledAST) toProgram(env *cel.Env, opts ...cel.ProgramOption) (out compiledProgram, err error) {
	prog, err := env.Program(ast.AST, opts...)
	if err != nil {
		return out, &CompilationError{cause: fmt.Errorf("failed to compile program %s: %w", ast.Source.GetId(), err)}
	}
	return compiledProgram{
		Program:    prog,
		Rules:      ast.Rules,
		Source:     ast.Source,
		Path:       ast.Path,
		Value:      ast.Value,
		Descriptor: ast.Descriptor,
	}, nil
}
