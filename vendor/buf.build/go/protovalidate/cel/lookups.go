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

package cel

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ProtoFieldToType resolves the CEL value type for the provided
// FieldDescriptor. If generic is true, the specific subtypes of map and
// repeated fields will be replaced with cel.DynType. If forItems is true, the
// type for the repeated list items is returned instead of the list type itself.
func ProtoFieldToType(fieldDesc protoreflect.FieldDescriptor, generic, forItems bool) *cel.Type {
	if !forItems {
		switch {
		case fieldDesc.IsMap():
			if generic {
				return cel.MapType(cel.DynType, cel.DynType)
			}
			keyType := ProtoFieldToType(fieldDesc.MapKey(), false, true)
			valType := ProtoFieldToType(fieldDesc.MapValue(), false, true)
			return cel.MapType(keyType, valType)
		case fieldDesc.IsList():
			if generic {
				return cel.ListType(cel.DynType)
			}
			itemType := ProtoFieldToType(fieldDesc, false, true)
			return cel.ListType(itemType)
		}
	}

	if fieldDesc.Kind() == protoreflect.MessageKind ||
		fieldDesc.Kind() == protoreflect.GroupKind {
		switch fqn := fieldDesc.Message().FullName(); fqn {
		case "google.protobuf.Any":
			return cel.AnyType
		case "google.protobuf.Duration":
			return cel.DurationType
		case "google.protobuf.Timestamp":
			return cel.TimestampType
		default:
			return cel.ObjectType(string(fqn))
		}
	}
	return protoKindToType(fieldDesc.Kind())
}

func ProtoFieldToValue(fieldDesc protoreflect.FieldDescriptor, value protoreflect.Value, forItems bool) ref.Val {
	switch {
	case fieldDesc.IsList() && !forItems:
		return types.NewProtoList(types.DefaultTypeAdapter, value.List())
	default:
		return types.DefaultTypeAdapter.NativeToValue(value.Interface())
	}
}

// protoKindToType maps a protoreflect.Kind to a compatible cel.Type.
func protoKindToType(kind protoreflect.Kind) *cel.Type {
	switch kind {
	case
		protoreflect.FloatKind,
		protoreflect.DoubleKind:
		return cel.DoubleType
	case
		protoreflect.Int32Kind,
		protoreflect.Int64Kind,
		protoreflect.Sint32Kind,
		protoreflect.Sint64Kind,
		protoreflect.Sfixed32Kind,
		protoreflect.Sfixed64Kind,
		protoreflect.EnumKind:
		return cel.IntType
	case
		protoreflect.Uint32Kind,
		protoreflect.Uint64Kind,
		protoreflect.Fixed32Kind,
		protoreflect.Fixed64Kind:
		return cel.UintType
	case protoreflect.BoolKind:
		return cel.BoolType
	case protoreflect.StringKind:
		return cel.StringType
	case protoreflect.BytesKind:
		return cel.BytesType
	case
		protoreflect.MessageKind,
		protoreflect.GroupKind:
		return cel.DynType
	default:
		return cel.DynType
	}
}
