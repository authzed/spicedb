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

import "google.golang.org/protobuf/reflect/protoreflect"

// The Filter interface determines which rules should be validated.
type Filter interface {
	// ShouldValidate returns whether rules for a given message, field, or
	// oneof should be evaluated. For a message or oneof, this only determines
	// whether message-level or oneof-level rules should be evaluated, and
	// ShouldValidate will still be called for each field in the message. If
	// ShouldValidate returns false for a specific field, all rules nested
	// in submessages of that field will be skipped as well.
	// For a message, the message argument provides the message itself. For a
	// field or oneof, the message argument provides the containing message.
	ShouldValidate(message protoreflect.Message, descriptor protoreflect.Descriptor) bool
}

// FilterFunc is a function type that implements the Filter interface, as a
// convenience for simple filters. A FilterFunc should follow the same semantics
// as the ShouldValidate method of Filter.
type FilterFunc func(protoreflect.Message, protoreflect.Descriptor) bool

func (f FilterFunc) ShouldValidate(
	message protoreflect.Message,
	descriptor protoreflect.Descriptor,
) bool {
	return f(message, descriptor)
}

type nopFilter struct{}

func (nopFilter) ShouldValidate(_ protoreflect.Message, _ protoreflect.Descriptor) bool {
	return true
}

var _ Filter = nopFilter{}
