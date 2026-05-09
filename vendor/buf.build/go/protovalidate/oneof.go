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

// oneof performs validation on a oneof union.
type oneof struct {
	// Descriptor is the OneofDescriptor targeted by this evaluator
	Descriptor protoreflect.OneofDescriptor
	// Required indicates that a member of the oneof must be set
	Required bool
}

func (o oneof) Evaluate(_ protoreflect.Message, val protoreflect.Value, cfg *validationConfig) error {
	return o.EvaluateMessage(val.Message(), cfg)
}

func (o oneof) EvaluateMessage(msg protoreflect.Message, cfg *validationConfig) error {
	if !cfg.filter.ShouldValidate(msg, o.Descriptor) ||
		!o.Required || msg.WhichOneof(o.Descriptor) != nil {
		return nil
	}
	return &ValidationError{Violations: []*Violation{{
		Proto: validate.Violation_builder{
			Field: validate.FieldPath_builder{
				Elements: []*validate.FieldPathElement{
					validate.FieldPathElement_builder{
						FieldName: proto.String(string(o.Descriptor.Name())),
					}.Build(),
				},
			}.Build(),
			RuleId:  proto.String("required"),
			Message: proto.String("exactly one field is required in oneof"),
		}.Build(),
	}}}
}

func (o oneof) Tautology() bool {
	return !o.Required
}

var _ messageEvaluator = oneof{}
