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
	"google.golang.org/protobuf/reflect/protoregistry"
)

//nolint:gochecknoglobals // static data, only want single instance
var resolver = newExtensionResolver()

// ResolveMessageRules returns the ResolveMessageRules option set for the MessageDescriptor.
func ResolveMessageRules(desc protoreflect.MessageDescriptor) (*validate.MessageRules, error) {
	return resolve[*validate.MessageRules](desc.Options(), validate.E_Message)
}

// ResolveOneofRules returns the ResolveOneofRules option set for the OneofDescriptor.
func ResolveOneofRules(desc protoreflect.OneofDescriptor) (*validate.OneofRules, error) {
	return resolve[*validate.OneofRules](desc.Options(), validate.E_Oneof)
}

// ResolveFieldRules returns the ResolveFieldRules option set for the FieldDescriptor.
func ResolveFieldRules(desc protoreflect.FieldDescriptor) (*validate.FieldRules, error) {
	return resolve[*validate.FieldRules](desc.Options(), validate.E_Field)
}

// ResolvePredefinedRules returns the ResolvePredefinedRules option set for the
// FieldDescriptor. Note that this value is only meaningful if it is set on a
// field or extension of a field rule message. This method is provided for
// convenience.
func ResolvePredefinedRules(desc protoreflect.FieldDescriptor) (*validate.PredefinedRules, error) {
	return resolve[*validate.PredefinedRules](desc.Options(), validate.E_Predefined)
}

// resolve resolves extensions without using [proto.GetExtension], in case the
// underlying type of the extension is not the concrete type expected by the
// library. In some cases, particularly when using a dynamic descriptor set, the
// underlying extension value's type will be a dynamicpb.Message. In some cases,
// the extension may not be resolved at all. This function handles reparsing the
// fields as needed to get it into the right concrete message. Resolve does not
// modify the input protobuf message, so it can be used concurrently.
func resolve[C proto.Message](
	options proto.Message,
	extensionType protoreflect.ExtensionType,
) (typedMessage C, err error) {
	var nilMessage C
	message, err := resolver.resolve(options, extensionType)
	if err != nil {
		return nilMessage, err
	}
	if message == nil {
		return nilMessage, nil
	} else if typedMessage, ok := message.(C); ok {
		return typedMessage, nil
	}
	typedMessage, _ = typedMessage.ProtoReflect().New().Interface().(C)
	b, err := proto.Marshal(message)
	if err != nil {
		return nilMessage, err
	}
	err = proto.Unmarshal(b, typedMessage)
	if err != nil {
		return nilMessage, err
	}
	return typedMessage, nil
}

// extensionResolver implements most of the logic of resolving protovalidate
// extensions.
type extensionResolver struct {
	// types is a types that just contains the protovalidate extensions.
	types *protoregistry.Types
}

// newExtensionResolver creates a new extension resolver. This is only called at
// init and will panic if it fails.
func newExtensionResolver() extensionResolver {
	resolver := extensionResolver{
		types: &protoregistry.Types{},
	}
	resolver.register(validate.E_Field)
	resolver.register(validate.E_Message)
	resolver.register(validate.E_Oneof)
	resolver.register(validate.E_Predefined)
	return resolver
}

// register registers an extension into the resolver's registry. This is only
// called at init and will panic if it fails.
func (resolver extensionResolver) register(extension protoreflect.ExtensionType) {
	if err := resolver.types.RegisterExtension(extension); err != nil {
		//nolint:forbidigo // this needs to be a fatal at init
		panic(err)
	}
}

// resolve handles the majority of extension resolution logic. This will return
// a proto.Message for the given extension if the message has the tag number of
// the provided extension. If there was no such tag number present in the known
// or unknown fields, this method will return nil. Note that the returned
// message may be dynamicpb.Message or another type, and thus may need to still
// be reparsed if needed.
func (resolver extensionResolver) resolve(
	options proto.Message,
	extensionType protoreflect.ExtensionType,
) (msg proto.Message, err error) {
	msg = resolver.getExtension(options, extensionType)
	if msg == nil {
		if unknown := options.ProtoReflect().GetUnknown(); len(unknown) > 0 {
			reparsedOptions := options.ProtoReflect().Type().New().Interface()
			if err = (proto.UnmarshalOptions{
				Resolver: resolver.types,
			}).Unmarshal(unknown, reparsedOptions); err == nil {
				msg = resolver.getExtension(reparsedOptions, extensionType)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// getExtension gets the extension extensionType on message, or if it is not
// found, nil. Unlike proto.GetExtension, this method will not panic if the
// runtime type of the extension is unexpected and returns nil if the extension
// is not present.
func (resolver extensionResolver) getExtension(
	message proto.Message,
	extensionType protoreflect.ExtensionType,
) proto.Message {
	reflect := message.ProtoReflect()
	if reflect.Has(extensionType.TypeDescriptor()) {
		extension, _ := reflect.Get(extensionType.TypeDescriptor()).Interface().(protoreflect.Message)
		return extension.Interface()
	}
	return nil
}
