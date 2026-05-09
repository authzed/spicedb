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

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/google/cel-go/cel"
)

// An expressions instance is a container for the information needed to compile
// and evaluate a list of CEL-based expressions, originating from a
// validate.Rule.
type expressions struct {
	Rules    []*validate.Rule
	RulePath []*validate.FieldPathElement
}

// compile produces a ProgramSet from the provided expressions in the given
// environment. If the generated cel.Program require cel.ProgramOption params,
// use CompileASTs instead with a subsequent call to ASTSet.ToProgramSet.
func compile(
	expressions expressions,
	env *cel.Env,
	envOpts ...cel.EnvOption,
) (set programSet, err error) {
	if len(expressions.Rules) == 0 {
		return set, nil
	}

	set.env = env
	if len(envOpts) > 0 {
		set.env, err = extendEnv(env, envOpts...)
		if err != nil {
			return set, &CompilationError{cause: fmt.Errorf(
				"failed to extend environment: %w", err)}
		}
	}

	programs := make([]compiledProgram, len(expressions.Rules))
	for i, rule := range expressions.Rules {
		var ast compiledAST
		ast, err = compileAST(set.env, rule, expressions.RulePath)
		if err != nil {
			return set, err
		}

		programs[i], err = ast.toProgram(set.env)
		if err != nil {
			return set, err
		}
	}
	set.programs = programs
	return set, nil
}

// compileASTs parses and type checks a set of expressions, producing a resulting
// ASTSet. The value can then be converted to a ProgramSet via
// ASTSet.ToProgramSet or ASTSet.ReduceResiduals. Use Compile instead if no
// cel.ProgramOption args need to be provided or residuals do not need to be
// computed.
func compileASTs(
	expressions expressions,
	env *cel.Env,
	envOpts ...cel.EnvOption,
) (set astSet, err error) {
	if len(expressions.Rules) == 0 {
		return set, nil
	}

	if len(envOpts) > 0 {
		env, err = extendEnv(env, envOpts...)
		if err != nil {
			return set, &CompilationError{cause: fmt.Errorf(
				"failed to extend environment: %w", err)}
		}
	}

	set = make([]compiledAST, len(expressions.Rules))
	for i, rule := range expressions.Rules {
		set[i], err = compileAST(env, rule, expressions.RulePath)
		if err != nil {
			return set, err
		}
	}

	return set, nil
}

func compileAST(env *cel.Env, rule *validate.Rule, rulePath []*validate.FieldPathElement) (out compiledAST, err error) {
	ast, issues := env.Compile(rule.GetExpression())
	if err := issues.Err(); err != nil {
		return out, &CompilationError{cause: fmt.Errorf(
			"failed to compile expression %s: %w", rule.GetId(), err)}
	}

	outType := ast.OutputType()
	if !(outType.IsAssignableType(cel.BoolType) || outType.IsAssignableType(cel.StringType)) {
		return out, &CompilationError{cause: fmt.Errorf(
			"expression %s outputs %s, wanted either bool or string",
			rule.GetId(), outType.String())}
	}

	return compiledAST{
		AST:    ast,
		Env:    env,
		Source: rule,
		Path:   rulePath,
	}, nil
}
