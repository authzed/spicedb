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
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/pb"
	"github.com/google/cel-go/common/types/ref"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// extendEnv extends the environment with proper registry isolation.
// If the environment uses a protovalidate registry, a child registry is created
// to ensure type registrations don't affect sibling extended environments.
func extendEnv(env *cel.Env, opts ...cel.EnvOption) (*cel.Env, error) {
	if reg, ok := env.CELTypeAdapter().(*registry); ok {
		child := reg.Copy()
		opts = append([]cel.EnvOption{
			cel.CustomTypeProvider(child),
			cel.CustomTypeAdapter(child),
		}, opts...)
	}
	return env.Extend(opts...)
}

// registry implements a tree of registries, where a registry can be extended cheaply
// be creating a new child, which delegates to its parent instead of copying the
// parent's content.
type registry struct {
	parent     *registry
	local      *types.Registry
	knownFiles map[protoreflect.FileDescriptor]struct{}
}

// newRegistry creates a new root registry seeded with the core CEL types
// (string, int, bool, etc.). Copy should be used to create child registries,
// which use an empty local store and delegate to the parent.
func newRegistry() (*registry, error) {
	local, err := types.NewRegistry()
	if err != nil {
		return nil, err
	}
	return &registry{
		parent: nil,
		local:  local,
	}, nil
}

// Copy creates a child registry that delegates to this registry.
// New registrations go to the child's local store, only if they aren't already
// in the parent. Lookups check local first, then delegate to parent.
func (r *registry) Copy() *registry {
	return &registry{
		parent: r,
		local:  types.NewEmptyRegistry(),
	}
}

// FindStructType returns the Type given a qualified type name.
func (r *registry) FindStructType(structType string) (*types.Type, bool) {
	if t, found := r.local.FindStructType(structType); found {
		return t, true
	}
	if r.parent != nil {
		return r.parent.FindStructType(structType)
	}
	return nil, false
}

// FindStructFieldType returns the field type for a checked type value.
func (r *registry) FindStructFieldType(structType, fieldName string) (*types.FieldType, bool) {
	if ft, found := r.local.FindStructFieldType(structType, fieldName); found {
		return ft, true
	}
	if r.parent != nil {
		return r.parent.FindStructFieldType(structType, fieldName)
	}
	return nil, false
}

// FindStructFieldNames returns the field names associated with the type.
func (r *registry) FindStructFieldNames(structType string) ([]string, bool) {
	if names, found := r.local.FindStructFieldNames(structType); found {
		return names, true
	}
	if r.parent != nil {
		return r.parent.FindStructFieldNames(structType)
	}
	return nil, false
}

// FindIdent takes a qualified identifier name and returns a ref.Val if one exists.
func (r *registry) FindIdent(identName string) (ref.Val, bool) {
	if v, found := r.local.FindIdent(identName); found {
		return v, true
	}
	if r.parent != nil {
		return r.parent.FindIdent(identName)
	}
	return nil, false
}

// EnumValue returns the numeric value of the given enum value name.
func (r *registry) EnumValue(enumName string) ref.Val {
	result := r.local.EnumValue(enumName)
	if result.Type() != types.ErrType {
		return result
	}
	if r.parent != nil {
		return r.parent.EnumValue(enumName)
	}
	return result
}

// NewValue creates a new type value from a qualified name and map of field values.
func (r *registry) NewValue(structType string, fields map[string]ref.Val) ref.Val {
	if _, found := r.local.FindStructType(structType); found {
		return r.local.NewValue(structType, fields)
	}
	if r.parent != nil {
		return r.parent.NewValue(structType, fields)
	}
	return r.local.NewValue(structType, fields)
}

// NativeToValue converts the input value to a CEL ref.Val.
func (r *registry) NativeToValue(value any) ref.Val {
	switch val := value.(type) {
	case protoreflect.List:
		// we need to use this registry as the adapter, not the local
		// types.Registry for cascading to work.
		return types.NewProtoList(r, val)
	case *pb.Map:
		// Same as protoreflect.List above: use the full registry chain
		// as adapter so map value lookups can resolve types registered
		// in ancestor registries.
		return types.NewProtoMap(r, val)
	default:
		result := r.local.NativeToValue(value)
		if r.parent != nil && types.IsError(result) {
			return r.parent.NativeToValue(value)
		}
		return result
	}
}

// RegisterMessage registers a protocol buffer message and its dependent messages.
// If the message type is already visible in the hierarchy, skip it.
func (r *registry) RegisterMessage(message proto.Message) error {
	typeName := string(message.ProtoReflect().Descriptor().FullName())
	if _, found := r.FindStructType(typeName); found {
		return nil
	}
	return r.local.RegisterMessage(message)
}

// RegisterType registers one or more ref.Type instances.
// If the type is already visible in the hierarchy, skip it.
func (r *registry) RegisterType(refTypes ...ref.Type) error {
	// Skip types already available through the parent chain.
	// Match standard types.Registry semantics: first registration wins.
	for _, t := range refTypes {
		if _, found := r.FindIdent(t.TypeName()); found {
			continue
		}
		if err := r.local.RegisterType(t); err != nil {
			return err
		}
	}
	return nil
}

// RegisterDescriptor registers all messages in a file descriptor.
// If the exact file descriptor has already been registered in an ancestor, skip it.
func (r *registry) RegisterDescriptor(fileDesc protoreflect.FileDescriptor) error {
	for reg := r; reg != nil; reg = reg.parent {
		if _, ok := reg.knownFiles[fileDesc]; ok {
			return nil // Already registered in ancestor, skip
		}
	}
	if r.knownFiles == nil {
		r.knownFiles = make(map[protoreflect.FileDescriptor]struct{})
	}
	r.knownFiles[fileDesc] = struct{}{}
	return r.local.RegisterDescriptor(fileDesc)
}

var (
	_ types.Provider = (*registry)(nil)
	_ types.Adapter  = (*registry)(nil)
)
