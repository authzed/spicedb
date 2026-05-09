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
	"strconv"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

//nolint:gochecknoglobals
var (
	mapRuleDescriptor     = (&validate.FieldRules{}).ProtoReflect().Descriptor().Fields().ByName("map")
	mapKeysRuleDescriptor = (&validate.MapRules{}).ProtoReflect().Descriptor().Fields().ByName("keys")
	mapKeysRulePath       = validate.FieldPath_builder{
		Elements: []*validate.FieldPathElement{
			fieldPathElement(mapRuleDescriptor),
			fieldPathElement(mapKeysRuleDescriptor),
		},
	}.Build()
	mapValuesDescriptor = (&validate.MapRules{}).ProtoReflect().Descriptor().Fields().ByName("values")
	mapValuesRulePath   = validate.FieldPath_builder{
		Elements: []*validate.FieldPathElement{
			fieldPathElement(mapRuleDescriptor),
			fieldPathElement(mapValuesDescriptor),
		},
	}.Build()
)

// kvPairs performs validation on a map field's KV Pairs.
//
// This type uses pointer receivers to avoid a heap allocation on every
// Evaluate call. The closure passed to Map().Range() captures the receiver,
// and with a value receiver the entire struct would escape to the heap.
// With a pointer receiver, only the pointer is captured.
type kvPairs struct {
	base

	// KeyRules are checked on the map keys
	KeyRules value
	// ValueRules are checked on the map values
	ValueRules value
}

func newKVPairs(valEval *value) *kvPairs {
	return &kvPairs{
		base:       newBase(valEval),
		KeyRules:   value{NestedRule: mapKeysRulePath},
		ValueRules: value{NestedRule: mapValuesRulePath},
	}
}

func (m *kvPairs) Evaluate(msg protoreflect.Message, val protoreflect.Value, cfg *validationConfig) (err error) {
	var ok bool
	val.Map().Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
		evalErr := m.evalPairs(msg, key, value, cfg)
		if evalErr != nil {
			element := validate.FieldPathElement_builder{
				FieldNumber: proto.Int32(m.FieldPathElement.GetFieldNumber()),
				FieldType:   m.base.FieldPathElement.GetFieldType().Enum(),
				FieldName:   proto.String(m.FieldPathElement.GetFieldName()),
			}
			element.KeyType = descriptorpb.FieldDescriptorProto_Type(m.base.Descriptor.MapKey().Kind()).Enum()
			element.ValueType = descriptorpb.FieldDescriptorProto_Type(m.base.Descriptor.MapValue().Kind()).Enum()
			switch m.base.Descriptor.MapKey().Kind() {
			case protoreflect.BoolKind:
				element.BoolKey = proto.Bool(key.Bool())
			case protoreflect.Int32Kind, protoreflect.Int64Kind,
				protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind,
				protoreflect.Sint32Kind, protoreflect.Sint64Kind:
				element.IntKey = proto.Int64(key.Int())
			case protoreflect.Uint32Kind, protoreflect.Uint64Kind,
				protoreflect.Fixed32Kind, protoreflect.Fixed64Kind:
				element.UintKey = proto.Uint64(key.Uint())
			case protoreflect.StringKind:
				element.StringKey = proto.String(key.String())
			case protoreflect.EnumKind, protoreflect.FloatKind, protoreflect.DoubleKind,
				protoreflect.BytesKind, protoreflect.MessageKind, protoreflect.GroupKind:
				fallthrough
			default:
				err = &CompilationError{cause: fmt.Errorf(
					"unexpected map key type %s",
					m.base.Descriptor.MapKey().Kind())}
				return false
			}
			updateViolationPaths(evalErr, element.Build(), m.RulePrefix.GetElements())
		}
		ok, err = mergeViolations(err, evalErr, cfg)
		return ok
	})
	return err
}

func (m *kvPairs) evalPairs(msg protoreflect.Message, key protoreflect.MapKey, value protoreflect.Value, cfg *validationConfig) (err error) {
	evalErr := m.KeyRules.EvaluateField(msg, key.Value(), cfg, true)
	markViolationForKey(evalErr)
	ok, err := mergeViolations(err, evalErr, cfg)
	if !ok {
		return err
	}

	evalErr = m.ValueRules.EvaluateField(msg, value, cfg, true)
	_, err = mergeViolations(err, evalErr, cfg)
	return err
}

func (m *kvPairs) Tautology() bool {
	return m.KeyRules.Tautology() &&
		m.ValueRules.Tautology()
}

func (m *kvPairs) formatKey(key any) string {
	switch k := key.(type) {
	case string:
		return strconv.Quote(k)
	default:
		return fmt.Sprintf("%v", key)
	}
}

var _ evaluator = (*kvPairs)(nil)
