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
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	// fieldRulesDesc provides a Descriptor for validate.FieldRules.
	fieldRulesDesc = (*validate.FieldRules)(nil).ProtoReflect().Descriptor()

	// fieldRulesOneofDesc provides the OneofDescriptor for the type union
	// in FieldRules.
	fieldRulesOneofDesc = fieldRulesDesc.Oneofs().ByName("type")

	// mapFieldRulesDesc provides the FieldDescriptor for the map standard
	// rules.
	mapFieldRulesDesc = fieldRulesDesc.Fields().ByName("map")

	// repeatedFieldRulesDesc provides the FieldDescriptor for the repeated
	// standard rules.
	repeatedFieldRulesDesc = fieldRulesDesc.Fields().ByName("repeated")
)

// expectedStandardRules maps protocol buffer field kinds to their
// expected field rules.
var expectedStandardRules = map[protoreflect.Kind]protoreflect.FieldDescriptor{
	protoreflect.FloatKind:    fieldRulesDesc.Fields().ByName("float"),
	protoreflect.DoubleKind:   fieldRulesDesc.Fields().ByName("double"),
	protoreflect.Int32Kind:    fieldRulesDesc.Fields().ByName("int32"),
	protoreflect.Int64Kind:    fieldRulesDesc.Fields().ByName("int64"),
	protoreflect.Uint32Kind:   fieldRulesDesc.Fields().ByName("uint32"),
	protoreflect.Uint64Kind:   fieldRulesDesc.Fields().ByName("uint64"),
	protoreflect.Sint32Kind:   fieldRulesDesc.Fields().ByName("sint32"),
	protoreflect.Sint64Kind:   fieldRulesDesc.Fields().ByName("sint64"),
	protoreflect.Fixed32Kind:  fieldRulesDesc.Fields().ByName("fixed32"),
	protoreflect.Fixed64Kind:  fieldRulesDesc.Fields().ByName("fixed64"),
	protoreflect.Sfixed32Kind: fieldRulesDesc.Fields().ByName("sfixed32"),
	protoreflect.Sfixed64Kind: fieldRulesDesc.Fields().ByName("sfixed64"),
	protoreflect.BoolKind:     fieldRulesDesc.Fields().ByName("bool"),
	protoreflect.StringKind:   fieldRulesDesc.Fields().ByName("string"),
	protoreflect.BytesKind:    fieldRulesDesc.Fields().ByName("bytes"),
	protoreflect.EnumKind:     fieldRulesDesc.Fields().ByName("enum"),
}

var expectedWKTRules = map[protoreflect.FullName]protoreflect.FieldDescriptor{
	"google.protobuf.Any":       fieldRulesDesc.Fields().ByName("any"),
	"google.protobuf.Duration":  fieldRulesDesc.Fields().ByName("duration"),
	"google.protobuf.FieldMask": fieldRulesDesc.Fields().ByName("field_mask"),
	"google.protobuf.Timestamp": fieldRulesDesc.Fields().ByName("timestamp"),
}

// expectedWrapperRules returns the validate.FieldRules field that
// is expected for the given wrapper well-known type's full name. If ok is
// false, no standard rules exist for that type.
func expectedWrapperRules(fqn protoreflect.FullName) (desc protoreflect.FieldDescriptor, ok bool) {
	switch fqn {
	case "google.protobuf.BoolValue":
		return expectedStandardRules[protoreflect.BoolKind], true
	case "google.protobuf.BytesValue":
		return expectedStandardRules[protoreflect.BytesKind], true
	case "google.protobuf.DoubleValue":
		return expectedStandardRules[protoreflect.DoubleKind], true
	case "google.protobuf.FloatValue":
		return expectedStandardRules[protoreflect.FloatKind], true
	case "google.protobuf.Int32Value":
		return expectedStandardRules[protoreflect.Int32Kind], true
	case "google.protobuf.Int64Value":
		return expectedStandardRules[protoreflect.Int64Kind], true
	case "google.protobuf.StringValue":
		return expectedStandardRules[protoreflect.StringKind], true
	case "google.protobuf.UInt32Value":
		return expectedStandardRules[protoreflect.Uint32Kind], true
	case "google.protobuf.UInt64Value":
		return expectedStandardRules[protoreflect.Uint64Kind], true
	default:
		return nil, false
	}
}
