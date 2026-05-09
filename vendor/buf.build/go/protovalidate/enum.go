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
	enumRuleDescriptor            = (&validate.FieldRules{}).ProtoReflect().Descriptor().Fields().ByName("enum")
	enumDefinedOnlyRuleDescriptor = (&validate.EnumRules{}).ProtoReflect().Descriptor().Fields().ByName("defined_only")
	enumDefinedOnlyRulePath       = validate.FieldPath_builder{
		Elements: []*validate.FieldPathElement{
			fieldPathElement(enumRuleDescriptor),
			fieldPathElement(enumDefinedOnlyRuleDescriptor),
		},
	}.Build()
)

// definedEnum is an evaluator that checks an enum value being a member of
// the defined values exclusively. This check is handled outside CEL as enums
// are completely type erased to integers.
type definedEnum struct {
	base

	// ValueDescriptors captures all the defined values for this enum
	ValueDescriptors protoreflect.EnumValueDescriptors
}

func (d definedEnum) Evaluate(_ protoreflect.Message, val protoreflect.Value, _ *validationConfig) error {
	if d.ValueDescriptors.ByNumber(val.Enum()) == nil {
		return &ValidationError{Violations: []*Violation{{
			Proto: validate.Violation_builder{
				Field:   d.fieldPath(),
				Rule:    d.rulePath(enumDefinedOnlyRulePath),
				RuleId:  proto.String("enum.defined_only"),
				Message: proto.String("value must be one of the defined enum values"),
			}.Build(),
			FieldValue:      val,
			FieldDescriptor: d.Descriptor,
			RuleValue:       protoreflect.ValueOfBool(true),
			RuleDescriptor:  enumDefinedOnlyRuleDescriptor,
		}}}
	}
	return nil
}

func (d definedEnum) Tautology() bool {
	return false
}

var _ evaluator = definedEnum{}
