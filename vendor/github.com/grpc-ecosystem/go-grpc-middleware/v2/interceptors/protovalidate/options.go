// Copyright (c) The go-grpc-middleware Authors.
// Licensed under the Apache License 2.0.

// Copyright 2017 David Ackroyd. All Rights Reserved.
// See LICENSE for licensing terms.

package protovalidate

import (
	"slices"

	"google.golang.org/protobuf/reflect/protoreflect"
)

type options struct {
	ignoreMessages []protoreflect.FullName
}

// An Option lets you add options to protovalidate interceptors using With* funcs.
type Option func(*options)

func evaluateOpts(opts []Option) *options {
	optCopy := &options{}
	for _, o := range opts {
		o(optCopy)
	}
	return optCopy
}

// WithIgnoreMessages sets the messages that should be ignored by the
// validator. Message types are matched using their fully-qualified Protobuf
// names.
//
// Use with caution and ensure validation is performed elsewhere.
func WithIgnoreMessages(msgs ...protoreflect.MessageType) Option {
	names := make([]protoreflect.FullName, 0, len(msgs))
	for _, msg := range msgs {
		names = append(names, msg.Descriptor().FullName())
	}
	slices.Sort(names)
	return func(o *options) {
		o.ignoreMessages = names
	}
}

func (o *options) shouldIgnoreMessage(fqn protoreflect.FullName) bool {
	// Names are sorted in WithIgnoreMessages, so we can use binary search.
	_, found := slices.BinarySearch(o.ignoreMessages, fqn)
	return found
}
