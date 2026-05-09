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
	"strings"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// messageOneof is a message evaluator that  performs validation on the specified
// fields, ensuring that only one is set. If `required` is true, it enforces that one of
// the fields _must_ be set.
type messageOneof struct {
	Fields   []protoreflect.FieldDescriptor
	Required bool
}

func (o messageOneof) formatFields() string {
	names := make([]string, len(o.Fields))
	for idx, fdesc := range o.Fields {
		names[idx] = string(fdesc.Name())
	}
	return strings.Join(names, ", ")
}

func (o messageOneof) Evaluate(_ protoreflect.Message, val protoreflect.Value, cfg *validationConfig) error {
	return o.EvaluateMessage(val.Message(), cfg)
}

func (o messageOneof) EvaluateMessage(msg protoreflect.Message, cfg *validationConfig) error {
	if !cfg.filter.ShouldValidate(msg, msg.Descriptor()) {
		return nil
	}
	err := &ValidationError{}
	if len(o.Fields) > 0 {
		count := 0
		for _, fdesc := range o.Fields {
			if msg.Has(fdesc) {
				count++
			}
		}
		if count > 1 {
			err.Violations = append(err.Violations, &Violation{
				Proto: validate.Violation_builder{
					RuleId:  proto.String("message.oneof"),
					Message: proto.String(fmt.Sprintf("only one of %s can be set", o.formatFields())),
				}.Build(),
			})
			return err
		}
		if o.Required && count != 1 {
			err.Violations = append(err.Violations, &Violation{
				Proto: validate.Violation_builder{
					RuleId:  proto.String("message.oneof"),
					Message: proto.String(fmt.Sprintf("one of %s must be set", o.formatFields())),
				}.Build(),
			})
			return err
		}
	}
	return nil
}

func (o messageOneof) Tautology() bool {
	return false
}

var _ messageEvaluator = messageOneof{}
