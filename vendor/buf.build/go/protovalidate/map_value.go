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
	"fmt"
	"math"
	"reflect"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// protoMap wraps a protoreflect.Map to implement ref.Val and traits.Mapper,
// allowing lazy access to map entries without copying the entire map upfront.
// This provides significant performance improvements when validating map fields.
type protoMap struct {
	adapter types.Adapter
	m       protoreflect.Map
	keyDesc protoreflect.FieldDescriptor
	valDesc protoreflect.FieldDescriptor
}

// newProtoMap creates a new protoMap wrapper around the given protoreflect.Map.
// The adapter is used to convert native values to CEL values. The keyDesc and
// valDesc are the field descriptors for the map's key and value types.
func newProtoMap(
	adapter types.Adapter,
	mapVal protoreflect.Map,
	keyDesc protoreflect.FieldDescriptor,
	valDesc protoreflect.FieldDescriptor,
) *protoMap {
	return &protoMap{
		adapter: adapter,
		m:       mapVal,
		keyDesc: keyDesc,
		valDesc: valDesc,
	}
}

// ref.Val implementation

func (m *protoMap) ConvertToNative(typeDesc reflect.Type) (any, error) {
	switch typeDesc.Kind() {
	case reflect.Map:
		nativeMap := reflect.MakeMapWithSize(typeDesc, m.m.Len())
		var convErr error
		m.m.Range(func(key protoreflect.MapKey, val protoreflect.Value) bool {
			nativeKey, err := m.adapter.NativeToValue(key.Interface()).ConvertToNative(typeDesc.Key())
			if err != nil {
				convErr = err
				return false
			}
			nativeVal, err := m.nativeValue(val).ConvertToNative(typeDesc.Elem())
			if err != nil {
				convErr = err
				return false
			}
			nativeMap.SetMapIndex(reflect.ValueOf(nativeKey), reflect.ValueOf(nativeVal))
			return true
		})
		if convErr != nil {
			return nil, convErr
		}
		return nativeMap.Interface(), nil
	case reflect.Interface:
		if reflect.TypeFor[*protoMap]().Implements(typeDesc) {
			return m, nil
		}
		if reflect.TypeOf(m.m).Implements(typeDesc) {
			return m.m, nil
		}
	}
	return nil, fmt.Errorf("unsupported type conversion: %v to %v", m.Type(), typeDesc)
}

func (m *protoMap) ConvertToType(typeVal ref.Type) ref.Val {
	switch typeVal {
	case types.MapType:
		return m
	case types.TypeType:
		return types.MapType
	}
	return types.NewErr("type conversion error from '%s' to '%s'", types.MapType, typeVal)
}

func (m *protoMap) Equal(other ref.Val) ref.Val {
	otherMap, ok := other.(traits.Mapper)
	if !ok {
		return types.False
	}
	otherSize, ok := otherMap.Size().(types.Int)
	if !ok || m.m.Len() != int(otherSize) {
		return types.False
	}
	var result ref.Val = types.True
	m.m.Range(func(key protoreflect.MapKey, val protoreflect.Value) bool {
		celKey := m.adapter.NativeToValue(m.toCelKey(key))
		otherVal, found := otherMap.Find(celKey)
		if !found {
			result = types.False
			return false
		}
		celVal := m.nativeValue(val)
		eq := celVal.Equal(otherVal)
		if eq != types.True {
			result = eq
			return false
		}
		return true
	})
	return result
}

func (m *protoMap) Type() ref.Type {
	return types.MapType
}

func (m *protoMap) Value() any {
	return m.m
}

// traits.Container implementation

func (m *protoMap) Contains(key ref.Val) ref.Val {
	_, found := m.Find(key)
	return types.Bool(found)
}

// traits.Indexer implementation

func (m *protoMap) Get(key ref.Val) ref.Val {
	val, found := m.Find(key)
	if !found {
		return types.ValOrErr(val, "no such key: %v", key)
	}
	return val
}

// traits.Iterable implementation

func (m *protoMap) Iterator() traits.Iterator {
	keys := make([]protoreflect.MapKey, 0, m.m.Len())
	m.m.Range(func(key protoreflect.MapKey, _ protoreflect.Value) bool {
		keys = append(keys, key)
		return true
	})
	return &protoMapIterator{
		adapter: m.adapter,
		parent:  m,
		keys:    keys,
	}
}

// traits.Sizer implementation

func (m *protoMap) Size() ref.Val {
	return types.Int(m.m.Len())
}

// traits.Mapper implementation

func (m *protoMap) Find(key ref.Val) (ref.Val, bool) {
	if val, found := m.findInternal(key); found {
		return val, true
	}
	// Handle numeric type coercion - CEL uses 64-bit integers
	switch celKey := key.(type) {
	case types.Double:
		if intKey, ok := doubleToInt64Lossless(float64(celKey)); ok {
			if val, found := m.findInternal(types.Int(intKey)); found {
				return val, true
			}
		}
		if uintKey, ok := doubleToUint64Lossless(float64(celKey)); ok {
			return m.findInternal(types.Uint(uintKey))
		}
	case types.Int:
		if uintKey, ok := int64ToUint64Lossless(int64(celKey)); ok {
			return m.findInternal(types.Uint(uintKey))
		}
	case types.Uint:
		if intKey, ok := uint64ToInt64Lossless(uint64(celKey)); ok {
			return m.findInternal(types.Int(intKey))
		}
	}
	return nil, false
}

func (m *protoMap) findInternal(key ref.Val) (ref.Val, bool) {
	nativeKey, err := m.toNativeKey(key)
	if err != nil {
		return nil, false
	}
	protoKey := protoreflect.ValueOf(nativeKey).MapKey()
	val := m.m.Get(protoKey)
	if !val.IsValid() {
		return nil, false
	}
	return m.nativeValue(val), true
}

// toNativeKey converts a CEL key value to the appropriate native Go type for
// map lookup based on the map's key field descriptor.
func (m *protoMap) toNativeKey(key ref.Val) (any, error) {
	switch m.keyDesc.Kind() {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		if keyVal, ok := key.Value().(int64); ok {
			if keyVal < math.MinInt32 || keyVal > math.MaxInt32 {
				return nil, fmt.Errorf("int64 key %d out of int32 range", keyVal)
			}
			return int32(keyVal), nil
		}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		if keyVal, ok := key.Value().(int64); ok {
			return keyVal, nil
		}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		if keyVal, ok := key.Value().(uint64); ok {
			if keyVal > math.MaxUint32 {
				return nil, fmt.Errorf("uint64 key %d out of uint32 range", keyVal)
			}
			return uint32(keyVal), nil
		}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if keyVal, ok := key.Value().(uint64); ok {
			return keyVal, nil
		}
	case protoreflect.BoolKind:
		if keyVal, ok := key.Value().(bool); ok {
			return keyVal, nil
		}
	case protoreflect.StringKind:
		if keyVal, ok := key.Value().(string); ok {
			return keyVal, nil
		}
	}
	return nil, fmt.Errorf("unsupported map key type: %v", m.keyDesc.Kind())
}

// toCelKey converts a protoreflect.MapKey to the appropriate CEL type.
// CEL uses 64-bit integers, so 32-bit integer types are widened.
func (m *protoMap) toCelKey(key protoreflect.MapKey) any {
	switch m.keyDesc.Kind() {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return key.Int()
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return key.Uint()
	default:
		return key.Interface()
	}
}

// nativeValue converts a protoreflect.Value to a CEL ref.Val.
func (m *protoMap) nativeValue(val protoreflect.Value) ref.Val {
	v := val.Interface()
	// For message values, val.Interface() returns proto.Message (not
	// protoreflect.Message), so NativeToValue handles it directly.
	return m.adapter.NativeToValue(v)
}

var (
	_ ref.Val       = (*protoMap)(nil)
	_ traits.Mapper = (*protoMap)(nil)
)

// protoMapIterator iterates over the keys of a protoMap.
type protoMapIterator struct {
	adapter types.Adapter
	parent  *protoMap
	keys    []protoreflect.MapKey
	idx     int
}

func (it *protoMapIterator) ConvertToNative(_ reflect.Type) (any, error) {
	return nil, errors.New("type conversion on iterators not supported")
}

func (it *protoMapIterator) ConvertToType(_ ref.Type) ref.Val {
	return types.NoSuchOverloadErr()
}

func (it *protoMapIterator) Equal(_ ref.Val) ref.Val {
	return types.NoSuchOverloadErr()
}

func (it *protoMapIterator) Type() ref.Type {
	return types.IteratorType
}

func (it *protoMapIterator) Value() any {
	return nil
}

func (it *protoMapIterator) HasNext() ref.Val {
	return types.Bool(it.idx < len(it.keys))
}

func (it *protoMapIterator) Next() ref.Val {
	if it.idx < len(it.keys) {
		key := it.keys[it.idx]
		it.idx++
		return it.adapter.NativeToValue(it.parent.toCelKey(key))
	}
	return nil
}

var _ traits.Iterator = (*protoMapIterator)(nil)

// Numeric conversion helpers (borrowed from cel-go)

func doubleToInt64Lossless(v float64) (int64, bool) {
	i := int64(v)
	return i, float64(i) == v
}

func doubleToUint64Lossless(v float64) (uint64, bool) {
	if v < 0 {
		return 0, false
	}
	u := uint64(v)
	return u, float64(u) == v
}

func int64ToUint64Lossless(v int64) (uint64, bool) {
	if v < 0 {
		return 0, false
	}
	return uint64(v), true
}

func uint64ToInt64Lossless(v uint64) (int64, bool) {
	if v > math.MaxInt64 {
		return 0, false
	}
	return int64(v), true
}
