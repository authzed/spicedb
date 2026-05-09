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
	"strings"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Violation represents a single instance where a validation rule was not met.
// It provides information about the field that caused the violation, the
// specific unfulfilled rule, and a human-readable error message.
type Violation struct {
	// Proto contains the violation's proto.Message form.
	Proto *validate.Violation

	// FieldValue contains the value of the specific field that failed
	// validation. If there was no value, this will contain an invalid value.
	FieldValue protoreflect.Value

	// FieldDescriptor contains the field descriptor corresponding to the
	// field that failed validation.
	FieldDescriptor protoreflect.FieldDescriptor

	// RuleValue contains the value of the rule that specified the failed
	// rule. Not all rules have a value; only standard and
	// predefined rules have rule values. In violations caused by other
	// kinds of rules, like custom contraints, this will contain an
	// invalid value.
	RuleValue protoreflect.Value

	// RuleDescriptor contains the field descriptor corresponding to the
	// rule that failed validation.
	RuleDescriptor protoreflect.FieldDescriptor
}

// String implements fmt.Stringer.
func (v *Violation) String() string {
	if v == nil {
		return ""
	}
	if v.Proto == nil {
		// default case so that we don't have an empty string
		return "[unknown]"
	}
	bldr := &strings.Builder{}
	if fieldPath := FieldPathString(v.Proto.GetField()); fieldPath != "" {
		bldr.WriteString(fieldPath)
		bldr.WriteString(": ")
	}
	if message := v.Proto.GetMessage(); message != "" {
		bldr.WriteString(message)
	} else if ruleID := v.Proto.GetRuleId(); ruleID != "" {
		bldr.WriteString("[")
		bldr.WriteString(ruleID)
		bldr.WriteString("]")
	} else {
		// default case
		bldr.WriteString("[unknown]")
	}
	return bldr.String()
}
