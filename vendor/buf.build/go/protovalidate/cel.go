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
	"errors"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// celPrograms is an evaluator that executes an expression.ProgramSet.
type celPrograms struct {
	base
	programSet
}

func (c celPrograms) Evaluate(_ protoreflect.Message, val protoreflect.Value, cfg *validationConfig) error {
	err := c.Eval(val, c.Descriptor, cfg)
	if err != nil {
		var valErr *ValidationError
		if errors.As(err, &valErr) {
			for _, violation := range valErr.Violations {
				violation.Proto.SetField(c.fieldPath())
				violation.Proto.SetRule(c.rulePath(violation.Proto.GetRule()))
				violation.FieldValue = val
				violation.FieldDescriptor = c.Descriptor
			}
		}
	}
	return err
}

func (c celPrograms) EvaluateMessage(msg protoreflect.Message, cfg *validationConfig) error {
	return c.Eval(protoreflect.ValueOfMessage(msg), nil, cfg)
}

func (c celPrograms) Tautology() bool {
	return len(c.programs) == 0
}

var (
	_ evaluator        = celPrograms{}
	_ messageEvaluator = celPrograms{}
)
