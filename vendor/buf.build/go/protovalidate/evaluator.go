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
	"google.golang.org/protobuf/reflect/protoreflect"
)

// evaluator defines a validation evaluator. evaluator implementations may elide
// type checking of the passed in value, as the types have been guaranteed
// during the build phase.
type evaluator interface {
	// Tautology returns true if the evaluator always succeeds.
	Tautology() bool

	// Evaluate checks that the provided val is valid. Unless failFast is true,
	// evaluation attempts to find all violations present in val instead of
	// returning an error on the first violation. The returned error will be one
	// of the following expected types:
	//
	//   - *ValidationError: val is invalid.
	//   - *RuntimeError: error evaluating val determined at runtime.
	//   - *CompilationError: this evaluator (or child evaluator) failed to
	//       build. This error is not recoverable.
	//
	Evaluate(msg protoreflect.Message, val protoreflect.Value, cfg *validationConfig) error
}

// messageEvaluator is essentially the same as evaluator, but specialized for
// messages as an optimization. See evaluator for behavior.
type messageEvaluator interface {
	evaluator

	// EvaluateMessage checks that the provided msg is valid. See
	// evaluator.Evaluate for behavior
	EvaluateMessage(msg protoreflect.Message, cfg *validationConfig) error
}

// evaluators are a set of evaluator applied together to a value. Evaluation
// merges all errors.ValidationError violations or short-circuits if failFast is
// true or a different error is returned.
type evaluators []evaluator

func (e evaluators) Evaluate(msg protoreflect.Message, val protoreflect.Value, cfg *validationConfig) (err error) {
	var ok bool
	for _, eval := range e {
		evalErr := eval.Evaluate(msg, val, cfg)
		if ok, err = mergeViolations(err, evalErr, cfg); !ok {
			return err
		}
	}
	return err
}

func (e evaluators) Tautology() bool {
	for _, eval := range e {
		if !eval.Tautology() {
			return false
		}
	}
	return true
}

// messageEvaluators are a specialization of evaluators. See evaluators for
// behavior details.
type messageEvaluators []messageEvaluator

func (m messageEvaluators) Evaluate(val protoreflect.Value, cfg *validationConfig) error {
	return m.EvaluateMessage(val.Message(), cfg)
}

func (m messageEvaluators) EvaluateMessage(msg protoreflect.Message, cfg *validationConfig) (err error) {
	var ok bool
	for _, eval := range m {
		evalErr := eval.EvaluateMessage(msg, cfg)
		if ok, err = mergeViolations(err, evalErr, cfg); !ok {
			return err
		}
	}
	return err
}

func (m messageEvaluators) Tautology() bool {
	for _, eval := range m {
		if !eval.Tautology() {
			return false
		}
	}
	return true
}
