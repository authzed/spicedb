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
	"sync"

	pvcel "buf.build/go/protovalidate/cel"
	"github.com/google/cel-go/cel"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	getGlobalValidator = sync.OnceValues(func() (Validator, error) { return New() })

	// GlobalValidator provides access to the global Validator instance that is
	// used by the [Validate] function. This is intended to be used by libraries
	// that use protovalidate. This Validator can be used as a default when the
	// user does not specify a Validator instance to use.
	//
	// Using the global Validator instance (either through [Validator] or via
	// GlobalValidator) will result in lower memory usage than using multiple
	// Validator instances, because each Validator instance has its own caches.
	GlobalValidator Validator = globalValidator{}
)

// Validator performs validation on any proto.Message values. The Validator is
// safe for concurrent use.
type Validator interface {
	// Validate checks that message satisfies its rules. Rules are
	// defined within the Protobuf file as options from the buf.validate
	// package. An error is returned if the rules are violated
	// (ValidationError), the evaluation logic for the message cannot be built
	// (CompilationError), or there is a type error when attempting to evaluate
	// a CEL expression associated with the message (RuntimeError).
	Validate(msg proto.Message, options ...ValidationOption) error
}

// New creates a Validator with the given options. An error may occur in setting
// up the CEL execution environment if the configuration is invalid. See the
// individual ValidatorOption for how they impact the fallibility of New.
func New(options ...ValidatorOption) (Validator, error) {
	cfg := config{
		extensionTypeResolver: protoregistry.GlobalTypes,
		nowFn:                 timestamppb.Now,
	}
	for _, opt := range options {
		opt.applyToValidator(&cfg)
	}

	reg, err := newRegistry()
	if err != nil {
		return nil, fmt.Errorf(
			"failed to construct CEL type registry: %w", err)
	}
	env, err := cel.NewEnv(
		cel.CustomTypeProvider(reg),
		cel.CustomTypeAdapter(reg),
		cel.Lib(pvcel.NewLibrary()),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to construct CEL environment: %w", err)
	}

	bldr := newBuilder(
		env,
		cfg.disableLazy,
		cfg.extensionTypeResolver,
		cfg.allowUnknownFields,
		cfg.desc...,
	)

	return &validator{
		builder: bldr,
		cfg: &validationConfig{
			failFast: cfg.failFast,
			filter:   nopFilter{},
			nowFn:    cfg.nowFn,
		},
	}, nil
}

type validator struct {
	builder *builder
	cfg     *validationConfig
}

func (v *validator) Validate(
	msg proto.Message,
	options ...ValidationOption,
) error {
	if msg == nil {
		return nil
	}
	cfg := v.cfg
	if len(options) > 0 {
		cfg = cfg.clone()
		for _, opt := range options {
			opt.applyToValidation(cfg)
		}
	}
	refl := msg.ProtoReflect()
	eval := v.builder.Load(refl.Descriptor())
	err := eval.EvaluateMessage(refl, cfg)
	finalizeViolationPaths(err)
	return err
}

// Validate uses a global instance of Validator constructed with no ValidatorOptions and
// calls its Validate function. For the vast majority of validation cases, using this global
// function is safe and acceptable. If you need to provide i.e. a custom
// ExtensionTypeResolver, you'll need to construct a Validator.
func Validate(msg proto.Message, options ...ValidationOption) error {
	globalValidator, err := getGlobalValidator()
	if err != nil {
		return err
	}
	return globalValidator.Validate(msg, options...)
}

type config struct {
	failFast              bool
	disableLazy           bool
	desc                  []protoreflect.MessageDescriptor
	extensionTypeResolver protoregistry.ExtensionTypeResolver
	allowUnknownFields    bool
	nowFn                 func() *timestamppb.Timestamp
}

type validationConfig struct {
	failFast bool
	filter   Filter
	nowFn    func() *timestamppb.Timestamp
}

func (cfg *validationConfig) clone() *validationConfig {
	clonedCfg := *cfg
	return &clonedCfg
}

type globalValidator struct{}

func (globalValidator) Validate(msg proto.Message, options ...ValidationOption) error {
	return Validate(msg, options...)
}
