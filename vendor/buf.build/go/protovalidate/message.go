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

	"google.golang.org/protobuf/reflect/protoreflect"
)

// message performs validation on a protoreflect.Message.
type message struct {
	// Err stores if there was a compilation error constructing this evaluator.
	// It is cached here so that it can be stored in the registry's lookup table.
	Err error

	// evaluators are the individual evaluators that are applied to a message.
	evaluators messageEvaluators

	// nestedEvaluators are the evaluators that are applied to nested fields and
	// oneofs.
	nestedEvaluators messageEvaluators
}

func (m *message) Evaluate(_ protoreflect.Message, val protoreflect.Value, cfg *validationConfig) error {
	return m.EvaluateMessage(val.Message(), cfg)
}

func (m *message) EvaluateMessage(msg protoreflect.Message, cfg *validationConfig) error {
	var (
		err error
		ok  bool
	)
	if cfg.filter.ShouldValidate(msg, msg.Descriptor()) {
		if m.Err != nil {
			return m.Err
		}
		if ok, err = mergeViolations(err, m.evaluators.EvaluateMessage(msg, cfg), cfg); !ok {
			return err
		}
	}
	_, err = mergeViolations(err, m.nestedEvaluators.EvaluateMessage(msg, cfg), cfg)
	return err
}

func (m *message) Tautology() bool {
	// returning false for now to avoid recursive messages causing false positives
	// on tautology detection.
	//
	// TODO: use a more sophisticated method to detect recursions so we can
	//  continue to detect tautologies on message evaluators.
	return false
}

func (m *message) Append(eval messageEvaluator) {
	if eval != nil && !eval.Tautology() {
		m.evaluators = append(m.evaluators, eval)
	}
}

func (m *message) AppendNested(eval messageEvaluator) {
	if eval != nil && !eval.Tautology() {
		m.nestedEvaluators = append(m.nestedEvaluators, eval)
	}
}

// unknownMessage is a MessageEvaluator for an unknown descriptor. This is
// returned only if lazy-building of evaluators has been disabled and an unknown
// descriptor is encountered.
type unknownMessage struct {
	desc protoreflect.MessageDescriptor
}

func (u unknownMessage) Err() error {
	return &CompilationError{cause: fmt.Errorf(
		"no evaluator available for %s",
		u.desc.FullName())}
}

func (u unknownMessage) Tautology() bool { return false }

func (u unknownMessage) Evaluate(_ protoreflect.Message, _ protoreflect.Value, _ *validationConfig) error {
	return u.Err()
}

func (u unknownMessage) EvaluateMessage(_ protoreflect.Message, _ *validationConfig) error {
	return u.Err()
}

// embeddedMessage is a wrapper for fields containing messages. It contains data that
// may differ per embeddedMessage message so that it is not cached.
type embeddedMessage struct {
	base

	message *message
}

func (m *embeddedMessage) Evaluate(_ protoreflect.Message, val protoreflect.Value, cfg *validationConfig) error {
	err := m.message.EvaluateMessage(val.Message(), cfg)
	updateViolationPaths(err, m.FieldPathElement, nil)
	return err
}

func (m *embeddedMessage) Tautology() bool {
	return m.message.Tautology()
}

var (
	_ messageEvaluator = (*message)(nil)
	_ messageEvaluator = (*unknownMessage)(nil)
	_ evaluator        = (*embeddedMessage)(nil)
)
