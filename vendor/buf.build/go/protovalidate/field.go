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
	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

//nolint:gochecknoglobals
var (
	requiredRuleDescriptor = (&validate.FieldRules{}).ProtoReflect().Descriptor().Fields().ByName("required")
	requiredRulePath       = validate.FieldPath_builder{
		Elements: []*validate.FieldPathElement{
			fieldPathElement(requiredRuleDescriptor),
		},
	}.Build()
)

// field performs validation on a single message field, defined by its
// descriptor.
type field struct {
	// Value is the evaluator to apply to the field's value
	Value value
	// Required indicates that the field must have a set value.
	Required bool
	// HasPresence reports whether the field distinguishes between unpopulated
	// and default values.
	HasPresence bool
	// Whether validation should be ignored for certain conditions.
	Ignore validate.Ignore
	// Err stores if there was a compilation error constructing this evaluator. It is stored
	// here so that it can be returned as part of validating this specific field.
	Err error
}

// shouldIgnoreAlways returns whether this field should always skip validation.
// If true, this will take precedence and all checks are skipped.
func (f field) shouldIgnoreAlways() bool {
	return f.Ignore == validate.Ignore_IGNORE_ALWAYS
}

// shouldIgnoreEmpty returns whether this field should skip validation on its zero value.
// This field is generally true for nullable fields or fields with the
// ignore_empty rule explicitly set.
func (f field) shouldIgnoreEmpty() bool {
	return f.HasPresence || f.Ignore == validate.Ignore_IGNORE_IF_ZERO_VALUE
}

func (f field) Evaluate(_ protoreflect.Message, val protoreflect.Value, cfg *validationConfig) error {
	return f.EvaluateMessage(val.Message(), cfg)
}

func (f field) EvaluateMessage(msg protoreflect.Message, cfg *validationConfig) (err error) {
	if f.shouldIgnoreAlways() {
		return nil
	}
	if !cfg.filter.ShouldValidate(msg, f.Value.Descriptor) {
		return nil
	}

	if f.Err != nil {
		return f.Err
	}

	if f.Required && !msg.Has(f.Value.Descriptor) {
		return &ValidationError{Violations: []*Violation{{
			Proto: validate.Violation_builder{
				Field:   fieldPath(f.Value.Descriptor),
				Rule:    prefixRulePath(f.Value.NestedRule, requiredRulePath),
				RuleId:  proto.String("required"),
				Message: proto.String("value is required"),
			}.Build(),
			FieldValue:      protoreflect.Value{},
			FieldDescriptor: f.Value.Descriptor,
			RuleValue:       protoreflect.ValueOfBool(true),
			RuleDescriptor:  requiredRuleDescriptor,
		}}}
	}

	if f.shouldIgnoreEmpty() && !msg.Has(f.Value.Descriptor) {
		return nil
	}

	return f.Value.EvaluateField(msg, msg.Get(f.Value.Descriptor), cfg, true)
}

func (f field) Tautology() bool {
	return !f.Required && f.Value.Tautology() && f.Err == nil
}

var _ messageEvaluator = field{}
