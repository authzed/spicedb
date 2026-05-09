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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A ValidatorOption modifies the default configuration of a Validator. See the
// individual options for their defaults and affects on the fallibility of
// configuring a Validator.
type ValidatorOption interface {
	applyToValidator(cfg *config)
}

// WithMessages allows warming up the Validator with messages that are
// expected to be validated. Messages included transitively (i.e., fields with
// message values) are automatically handled.
func WithMessages(messages ...proto.Message) ValidatorOption {
	desc := make([]protoreflect.MessageDescriptor, len(messages))
	for i, msg := range messages {
		desc[i] = msg.ProtoReflect().Descriptor()
	}
	return WithMessageDescriptors(desc...)
}

// WithMessageDescriptors allows warming up the Validator with message
// descriptors that are expected to be validated. Messages included transitively
// (i.e., fields with message values) are automatically handled.
func WithMessageDescriptors(descriptors ...protoreflect.MessageDescriptor) ValidatorOption {
	return &messageDescriptorsOption{descriptors}
}

// WithDisableLazy prevents the Validator from lazily building validation logic
// for a message it has not encountered before. Disabling lazy logic
// additionally eliminates any internal locking as the validator becomes
// read-only.
//
// Note: All expected messages must be provided by WithMessages or
// WithMessageDescriptors during initialization.
func WithDisableLazy() ValidatorOption {
	return &disableLazyOption{}
}

// WithExtensionTypeResolver specifies a resolver to use when reparsing unknown
// extension types. When dealing with dynamic file descriptor sets, passing this
// option will allow extensions to be resolved using a custom resolver.
//
// To ignore unknown extension fields, use the [WithAllowUnknownFields] option.
// Note that this may result in messages being treated as valid even though not
// all rules are being applied.
func WithExtensionTypeResolver(extensionTypeResolver protoregistry.ExtensionTypeResolver) ValidatorOption {
	return &extensionTypeResolverOption{extensionTypeResolver}
}

// WithAllowUnknownFields specifies if the presence of unknown field rules
// should cause compilation to fail with an error. When set to false, an unknown
// field will simply be ignored, which will cause rules to silently not be
// applied. This condition may occur if a predefined rule definition isn't
// present in the extension type resolver, or when passing dynamic messages with
// standard rules defined in a newer version of protovalidate. The default
// value is false, to prevent silently-incorrect validation from occurring.
func WithAllowUnknownFields() ValidatorOption {
	return &allowUnknownFieldsOption{}
}

// A ValidationOption specifies per-validation configuration. See the individual
// options for their defaults and effects.
type ValidationOption interface {
	applyToValidation(cfg *validationConfig)
}

// WithFilter specifies a filter to use for this validation. A filter can
// control which fields are evaluated by the validator.
func WithFilter(filter Filter) ValidationOption {
	return &filterOption{filter}
}

// Option implements both [ValidatorOption] and [ValidationOption], so it can be
// applied both to validator instances as well as individual validations.
type Option interface {
	ValidatorOption
	ValidationOption
}

// WithFailFast specifies whether validation should fail on the first rule
// violation encountered or if all violations should be accumulated. By default,
// all violations are accumulated.
func WithFailFast() Option {
	return &failFastOption{}
}

// WithNowFunc specifies the function used to derive the `now` variable in CEL
// expressions. By default, [timestamppb.Now] is used.
func WithNowFunc(fn func() *timestamppb.Timestamp) Option {
	return nowFuncOption(fn)
}

type messageDescriptorsOption struct {
	descriptors []protoreflect.MessageDescriptor
}

func (o *messageDescriptorsOption) applyToValidator(cfg *config) {
	cfg.desc = append(cfg.desc, o.descriptors...)
}

type disableLazyOption struct{}

func (o *disableLazyOption) applyToValidator(cfg *config) {
	cfg.disableLazy = true
}

type extensionTypeResolverOption struct {
	extensionTypeResolver protoregistry.ExtensionTypeResolver
}

func (o *extensionTypeResolverOption) applyToValidator(cfg *config) {
	cfg.extensionTypeResolver = o.extensionTypeResolver
}

type allowUnknownFieldsOption struct{}

func (o *allowUnknownFieldsOption) applyToValidator(cfg *config) {
	cfg.allowUnknownFields = true
}

type filterOption struct{ filter Filter }

func (o *filterOption) applyToValidation(cfg *validationConfig) {
	if o.filter == nil {
		cfg.filter = nopFilter{}
	} else {
		cfg.filter = o.filter
	}
}

type failFastOption struct{}

func (o *failFastOption) applyToValidator(cfg *config) {
	cfg.failFast = true
}

func (o *failFastOption) applyToValidation(cfg *validationConfig) {
	cfg.failFast = true
}

type nowFuncOption func() *timestamppb.Timestamp

func (o nowFuncOption) applyToValidator(cfg *config) {
	cfg.nowFn = o
}

func (o nowFuncOption) applyToValidation(cfg *validationConfig) {
	cfg.nowFn = o
}
