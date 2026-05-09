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
	"slices"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// base is a common struct used by all field evaluators. It holds
// some common information used across all field evaluators.
type base struct {
	// Descriptor is the FieldDescriptor targeted by this evaluator, or nil if
	// there is none.
	Descriptor protoreflect.FieldDescriptor

	// FieldPathElement is the field path element that pertains to this evaluator, or
	// nil if there is none.
	FieldPathElement *validate.FieldPathElement

	// RulePrefix is a static prefix this evaluator should add to the rule path
	// of violations.
	RulePrefix *validate.FieldPath
}

func newBase(valEval *value) base {
	return base{
		Descriptor:       valEval.Descriptor,
		FieldPathElement: fieldPathElement(valEval.Descriptor),
		RulePrefix:       valEval.NestedRule,
	}
}

func (b *base) fieldPath() *validate.FieldPath {
	if b.FieldPathElement == nil {
		return nil
	}
	return validate.FieldPath_builder{
		Elements: []*validate.FieldPathElement{
			b.FieldPathElement,
		},
	}.Build()
}

func (b *base) rulePath(suffix *validate.FieldPath) *validate.FieldPath {
	return prefixRulePath(b.RulePrefix, suffix)
}

func prefixRulePath(prefix *validate.FieldPath, suffix *validate.FieldPath) *validate.FieldPath {
	if len(prefix.GetElements()) > 0 {
		return validate.FieldPath_builder{
			Elements: slices.Concat(prefix.GetElements(), suffix.GetElements()),
		}.Build()
	}
	return suffix
}
