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
	"github.com/google/cel-go/common/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// programSet is a collection of compiledProgram expressions that are evaluated
// together with the same input value. All expressions in a programSet may refer
// to a `this` variable. It also holds the CEL environment used for compilation,
// which provides the correct type adapter for evaluation.
type programSet struct {
	programs []compiledProgram
	env      *cel.Env
}

// Eval applies the contained expressions to the provided `this` val, returning
// either *errors.ValidationError if the input is invalid or errors.RuntimeError
// if there is a type or range error. If failFast is true, execution stops at
// the first failed expression.
func (s programSet) Eval(
	val protoreflect.Value,
	fieldDesc protoreflect.FieldDescriptor,
	cfg *validationConfig,
) error {
	if len(s.programs) == 0 {
		return nil
	}
	activation := getBindings()
	defer putBindings(activation)
	activation.This = newOptional(thisToCel(val.Interface(), fieldDesc, s.env.CELTypeAdapter()))
	var violations []*Violation
	for _, expr := range s.programs {
		violation, err := expr.eval(activation, cfg)
		if err != nil {
			return err
		}
		if violation != nil {
			violations = append(violations, violation)
			if cfg.failFast {
				break
			}
		}
	}

	if len(violations) > 0 {
		return &ValidationError{Violations: violations}
	}

	return nil
}

func thisToCel(val any, fieldDesc protoreflect.FieldDescriptor, adapter types.Adapter) any {
	switch value := val.(type) {
	case protoreflect.Message:
		return value.Interface()
	case protoreflect.Map:
		return newProtoMap(adapter, value, fieldDesc.MapKey(), fieldDesc.MapValue())
	default:
		return value
	}
}

// compiledProgram is a parsed and type-checked cel.Program along with the
// source Expression.
type compiledProgram struct {
	Program    cel.Program
	Rules      protoreflect.Message
	Source     *validate.Rule
	Path       []*validate.FieldPathElement
	Value      protoreflect.Value
	Descriptor protoreflect.FieldDescriptor
}

//nolint:nilnil // non-existence of violations is intentional
func (expr compiledProgram) eval(activation *bindings, cfg *validationConfig) (*Violation, error) {
	activation.NowFn = cfg.nowFn
	if expr.Rules != nil {
		activation.Rules = expr.Rules.Interface()
	}
	if expr.Value.IsValid() {
		activation.Rule = expr.Value.Interface()
	}
	value, _, err := expr.Program.Eval(activation)
	if err != nil {
		return nil, &RuntimeError{cause: fmt.Errorf(
			"error evaluating %s: %w", expr.Source.GetId(), err)}
	}
	switch val := value.Value().(type) {
	case string:
		if val == "" {
			return nil, nil
		}
		return &Violation{
			Proto: validate.Violation_builder{
				Rule:    expr.rulePath(),
				RuleId:  proto.String(expr.Source.GetId()),
				Message: proto.String(val),
			}.Build(),
			RuleValue:      expr.Value,
			RuleDescriptor: expr.Descriptor,
		}, nil
	case bool:
		if val {
			return nil, nil
		}
		message := expr.Source.GetMessage()
		if message == "" {
			message = fmt.Sprintf("%q returned false", expr.Source.GetExpression())
		}
		return &Violation{
			Proto: validate.Violation_builder{
				Rule:    expr.rulePath(),
				RuleId:  proto.String(expr.Source.GetId()),
				Message: proto.String(message),
			}.Build(),
			RuleValue:      expr.Value,
			RuleDescriptor: expr.Descriptor,
		}, nil
	default:
		return nil, &RuntimeError{cause: fmt.Errorf(
			"resolved to an unexpected type %T", val)}
	}
}

func (expr compiledProgram) rulePath() *validate.FieldPath {
	if len(expr.Path) > 0 {
		return validate.FieldPath_builder{Elements: expr.Path}.Build()
	}
	return nil
}
